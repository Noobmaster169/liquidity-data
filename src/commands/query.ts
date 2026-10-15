import { join } from 'node:path';
import { existsSync, readdirSync } from 'node:fs';
import type { CollectorConfig } from '../config.ts';
import { openDuck, q } from '../duck.ts';

// Registers one view per table directory (events/<p>/<E> -> events__p__E, swaps/<p> -> swaps__p, intents, blocks, decode_failures) then runs the SQL.
export async function query(cfg: CollectorConfig, sql: string): Promise<void> {
  const conn = await openDuck(':memory:');
  const root = cfg.dataRoot;
  const views: string[] = [];
  const addView = (name: string, dir: string) => { if (existsSync(join(root, dir))) views.push(`CREATE VIEW "${name}" AS SELECT * FROM read_parquet(${q(join(root, dir, '*', '*.parquet'))}, union_by_name=true)`); };
  for (const t of ['intents', 'blocks', 'decode_failures']) addView(t, t);
  for (const group of ['events', 'swaps', 'instructions']) {
    const gdir = join(root, group); if (!existsSync(gdir)) continue;
    for (const p of readdirSync(gdir)) {
      const pdir = join(gdir, p);
      const subs = readdirSync(pdir);
      if (subs.some((s) => s.startsWith('date='))) addView(`${group}__${p}`, join(group, p));
      else for (const e of subs) addView(`${group}__${p}__${e}`, join(group, p, e));
    }
  }
  for (const v of views) await conn.run(v);
  const r = await conn.runAndReadAll(sql);
  console.table(r.getRowObjectsJson());
}
