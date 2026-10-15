import { join } from 'node:path';
import { existsSync } from 'node:fs';
import type { CollectorConfig } from '../config.ts';
import { Checkpoint } from '../checkpoint.ts';

export async function status(cfg: CollectorConfig): Promise<void> {
  for (const lane of ['tail', 'backfill']) {
    const path = join(cfg.stateRoot, `${lane}.duckdb`);
    if (!existsSync(path)) { console.log(`${lane}: not started`); continue; }
    const c = await Checkpoint.open(path);
    try {
      const s = await c.getState(lane);
      const ranges = await c.listRanges(lane);
      const rows = new Map<string, number>();
      let nfiles = 0;
      for (const r of ranges) for (const f of r.files) { rows.set(f.table, (rows.get(f.table) ?? 0) + f.rows); nfiles++; }
      console.log(`${lane}: start ${s?.startSlot} next ${s?.nextSlot} target ${s?.targetSlot ?? '-'} ranges ${ranges.length} files ${nfiles}`);
      for (const [t, n] of [...rows.entries()].sort((a, b) => b[1] - a[1]).slice(0, 25)) console.log(`  ${t.padEnd(56)} ${n}`);
    } finally { await c.close(); }
  }
}
