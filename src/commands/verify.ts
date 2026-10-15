import { createHash } from 'node:crypto';
import { createReadStream } from 'node:fs';
import { join } from 'node:path';
import type { CollectorConfig } from '../config.ts';
import { Checkpoint } from '../checkpoint.ts';
import { openDuck, rowsJson, q } from '../duck.ts';

async function sha256(path: string): Promise<string | null> {
  try { const h = createHash('sha256'); for await (const c of createReadStream(path)) h.update(c as Buffer); return h.digest('hex'); } catch { return null; }
}

export async function verifyLane(dataRoot: string, checkpoint: Checkpoint, lane: string): Promise<{ problems: string[]; ranges: number; files: number }> {
  const problems: string[] = [];
  const ranges = await checkpoint.listRanges(lane);
  const sorted = [...ranges].sort((a, b) => a.loSlot - b.loSlot);
  for (let i = 1; i < sorted.length; i++) {
    const prev = sorted[i - 1]!, cur = sorted[i]!;
    if (cur.loSlot !== prev.hiSlot + 1) problems.push(`gap or overlap between ranges ${prev.loSlot}-${prev.hiSlot} and ${cur.loSlot}-${cur.hiSlot}`);
  }
  let files = 0;
  const conn = await openDuck(':memory:');
  for (const r of sorted) {
    for (const f of r.files) {
      files++;
      const actual = await sha256(join(dataRoot, f.path));
      if (actual === null) problems.push(`missing file ${f.path}`);
      else if (actual !== f.sha256) problems.push(`sha256 mismatch ${f.path}`);
    }
    const blockFiles = r.files.filter((f) => f.table === 'blocks').map((f) => join(dataRoot, f.path));
    if (blockFiles.length === 0) { problems.push(`range ${r.loSlot}-${r.hiSlot} has no blocks file`); continue; }
    try {
      const rows = await rowsJson(conn, `SELECT count(*) AS n, min(slot) AS lo, max(slot) AS hi FROM read_parquet([${blockFiles.map(q).join(', ')}])`);
      const n = Number(rows[0]!.n), lo = Number(rows[0]!.lo), hi = Number(rows[0]!.hi);
      if (n !== r.hiSlot - r.loSlot + 1 || lo !== r.loSlot || hi !== r.hiSlot) problems.push(`range ${r.loSlot}-${r.hiSlot}: blocks rows ${n} (${lo}-${hi}) do not cover the range`);
    } catch (e) { problems.push(`range ${r.loSlot}-${r.hiSlot}: cannot read blocks (${e instanceof Error ? e.message : String(e)})`); }
  }
  return { problems, ranges: ranges.length, files };
}

export async function verify(cfg: CollectorConfig, lane: string): Promise<void> {
  const ckpt = await Checkpoint.open(join(cfg.stateRoot, `${lane}.duckdb`));
  try {
    const r = await verifyLane(cfg.dataRoot, ckpt, lane);
    console.log(`${lane}: ${r.ranges} ranges, ${r.files} files, ${r.problems.length} problems`);
    for (const p of r.problems) console.log(`  ${p}`);
    if (r.problems.length) process.exitCode = 1;
  } finally { await ckpt.close(); }
}
