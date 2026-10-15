import { createHash } from 'node:crypto';
import { mkdir, open, readdir, rename, rm, writeFile } from 'node:fs/promises';
import { createReadStream } from 'node:fs';
import { join, dirname } from 'node:path';
import type { Column } from './idl.ts';
import type { Row } from './rows.ts';
import { q, type DuckDBConnection } from './duck.ts';

export interface FileRecord { table: string; path: string; rows: number; sha256: string }

export interface RangeWriterOptions { dataRoot: string; conn: DuckDBConnection; decoderVersion: string; schemaFor: (table: string) => Column[]; idlSha256?: string }

async function sha256File(path: string): Promise<string> {
  const h = createHash('sha256');
  for await (const chunk of createReadStream(path)) h.update(chunk as Buffer);
  return h.digest('hex');
}
async function fsyncFile(path: string): Promise<void> { const fh = await open(path, 'r'); try { await fh.sync(); } finally { await fh.close(); } }

export class RangeWriter {
  readonly opts: RangeWriterOptions;
  constructor(opts: RangeWriterOptions) { this.opts = opts; }

  async writeRange(lane: string, lo: number, hi: number, tables: Map<string, Row[]>): Promise<FileRecord[]> {
    const { dataRoot, conn, decoderVersion, schemaFor, idlSha256 = '' } = this.opts;
    const staging = join(dataRoot, '.staging');
    await mkdir(staging, { recursive: true });
    let fallbackDate: string | null = null;
    for (const rows of tables.values()) for (const r of rows) { const bt = r.block_time; if (typeof bt === 'string') { fallbackDate = bt.slice(0, 10); break; } }
    const written: string[] = [];
    const records: FileRecord[] = [];
    try {
      for (const [table, rows] of [...tables.entries()].sort(([a], [b]) => a.localeCompare(b))) {
        if (rows.length === 0) continue;
        const byDate = new Map<string, Row[]>();
        for (const r of rows) {
          const bt = r.block_time;
          const date = typeof bt === 'string' ? bt.slice(0, 10) : fallbackDate ?? 'unknown';
          const arr = byDate.get(date); if (arr) arr.push(r); else byDate.set(date, [r]);
        }
        const cols = schemaFor(table);
        const colList = cols.map((c) => `"${c.name}"`).join(', ');
        const colTypes = `{${cols.map((c) => `${q(c.name)}: ${q(c.type)}`).join(', ')}}`;
        const order = ['slot', 'tx_index', 'ix_path'].filter((c) => cols.some((x) => x.name === c)).map((c) => `"${c}"`).join(', ');
        for (const [date, group] of byDate) {
          const stage = join(staging, `${lane}-${lo}-${hi}-${table.replace(/\//g, '__')}-${date}.jsonl`);
          await writeFile(stage, group.map((r) => JSON.stringify(r)).join('\n') + '\n');
          written.push(stage);
          const rel = `${table}/date=${date}/${lo}-${hi}.parquet`;
          const final = join(dataRoot, rel);
          const tmp = final + '.tmp';
          await mkdir(dirname(final), { recursive: true });
          written.push(tmp);
          const kv = `{decoder_version: ${q(decoderVersion)}, idl_sha256: ${q(idlSha256)}, lane: ${q(lane)}, lo_slot: ${q(String(lo))}, hi_slot: ${q(String(hi))}}`;
          await conn.run(
            `COPY (SELECT ${colList} FROM read_json(${q(stage)}, format='newline_delimited', columns=${colTypes}, maximum_object_size=134217728) ORDER BY ${order}) ` +
            `TO ${q(tmp)} (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 3, KV_METADATA ${kv})`,
          );
          await fsyncFile(tmp);
          await rename(tmp, final);
          written.splice(written.indexOf(tmp), 1);
          records.push({ table, path: rel, rows: group.length, sha256: await sha256File(final) });
          await rm(stage, { force: true });
          written.splice(written.indexOf(stage), 1);
        }
      }
      return records;
    } catch (e) {
      for (const p of written) await rm(p, { force: true });
      for (const r of records) await rm(join(dataRoot, r.path), { force: true });
      throw e;
    }
  }

  static async cleanupTemp(dataRoot: string): Promise<number> {
    let n = 0;
    const staging = join(dataRoot, '.staging');
    try { for (const f of await readdir(staging)) { await rm(join(staging, f), { force: true }); n++; } } catch { /* no staging dir */ }
    const walk = async (dir: string): Promise<void> => {
      let entries;
      try { entries = await readdir(dir, { withFileTypes: true }); } catch { return; }
      for (const e of entries) {
        const p = join(dir, e.name);
        if (e.isDirectory()) { if (e.name !== '.staging') await walk(p); }
        else if (e.name.endsWith('.parquet.tmp')) { await rm(p, { force: true }); n++; }
      }
    };
    await walk(dataRoot);
    return n;
  }
}
