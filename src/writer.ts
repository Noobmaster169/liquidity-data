import { createHash } from 'node:crypto';
import { appendFile, mkdir, open, readdir, rename, rm } from 'node:fs/promises';
import { createReadStream } from 'node:fs';
import { join, dirname } from 'node:path';
import type { Column } from './idl.ts';
import type { Row } from './rows.ts';
import { q, type DuckDBConnection } from './duck.ts';

export interface FileRecord { table: string; path: string; rows: number; sha256: string }

export interface RangeWriterOptions { dataRoot: string; conn: DuckDBConnection; decoderVersion: string; schemaFor: (table: string) => Column[]; idlSha256?: string; suffix?: string }

async function sha256File(path: string): Promise<string> {
  const h = createHash('sha256');
  for await (const chunk of createReadStream(path)) h.update(chunk as Buffer);
  return h.digest('hex');
}
async function fsyncFile(path: string): Promise<void> { const fh = await open(path, 'r'); try { await fh.sync(); } finally { await fh.close(); } }

export interface RangeSink { add(tables: Map<string, Row[]>): Promise<void>; finish(): Promise<FileRecord[]>; abort(): Promise<void> }

export class RangeWriter {
  readonly opts: RangeWriterOptions;
  constructor(opts: RangeWriterOptions) { this.opts = opts; }

  /** Convenience for small ranges: everything in memory, one call. */
  async writeRange(lane: string, lo: number, hi: number, tables: Map<string, Row[]>): Promise<FileRecord[]> {
    const sink = this.openRange(lane, lo, hi);
    try { await sink.add(tables); return await sink.finish(); } catch (e) { await sink.abort(); throw e; }
  }

  /**
   * Streams rows of one slot range into per-(table, date) JSONL stage files as blocks complete, so a range is never
   * held in memory. Rows without a block_time (skipped slots) are buffered until finish() and placed in the first date seen.
   */
  openRange(lane: string, lo: number, hi: number): RangeSink {
    const { dataRoot, conn, decoderVersion, schemaFor, idlSha256 = '', suffix = '' } = this.opts;
    const staging = join(dataRoot, '.staging', lane);
    const stages = new Map<string, { table: string; date: string; path: string; rows: number }>();
    const undated = new Map<string, Row[]>();
    let firstDate: string | null = null;
    let ready: Promise<void> | null = null;
    let chain: Promise<void> = Promise.resolve();
    const stageFor = async (table: string, date: string) => {
      const key = `${table}|${date}`;
      let st = stages.get(key);
      if (!st) { st = { table, date, path: join(staging, `${lo}-${hi}-${table.replace(/\//g, '__')}-${date}.jsonl`), rows: 0 }; stages.set(key, st); }
      return st;
    };
    const append = async (table: string, date: string, rows: Row[]) => {
      if (rows.length === 0) return;
      const st = await stageFor(table, date);
      await appendFile(st.path, rows.map((r) => JSON.stringify(r)).join('\n') + '\n');
      st.rows += rows.length;
    };
    const add = async (tables: Map<string, Row[]>): Promise<void> => {
      if (!ready) ready = mkdir(staging, { recursive: true }).then(() => undefined);
      await ready;
      for (const [table, rows] of tables) {
        const byDate = new Map<string, Row[]>();
        for (const r of rows) {
          const bt = r.block_time;
          if (typeof bt === 'string') { const d = bt.slice(0, 10); if (!firstDate) firstDate = d; const a = byDate.get(d); if (a) a.push(r); else byDate.set(d, [r]); }
          else { const a = undated.get(table); if (a) a.push(r); else undated.set(table, [r]); }
        }
        for (const [date, group] of byDate) await append(table, date, group);
      }
    };
    const cleanup = async () => { for (const st of stages.values()) await rm(st.path, { force: true }); await rm(staging, { recursive: true, force: true }).catch(() => undefined); };
    const finish = async (): Promise<FileRecord[]> => {
      const fallback = firstDate ?? 'unknown';
      for (const [table, rows] of undated) await append(table, fallback, rows);
      const records: FileRecord[] = [];
      const written: string[] = [];
      try {
        for (const st of [...stages.values()].sort((a, b) => a.table.localeCompare(b.table) || a.date.localeCompare(b.date))) {
          const cols = schemaFor(st.table);
          const colList = cols.map((c) => `"${c.name}"`).join(', ');
          const colTypes = `{${cols.map((c) => `${q(c.name)}: ${q(c.type)}`).join(', ')}}`;
          const order = ['slot', 'tx_index', 'ix_path'].filter((c) => cols.some((x) => x.name === c)).map((c) => `"${c}"`).join(', ');
          const rel = `${st.table}/date=${st.date}/${lo}-${hi}${suffix}.parquet`;
          const final = join(dataRoot, rel);
          const tmp = `${final}.${lane}.tmp`;
          await mkdir(dirname(final), { recursive: true });
          written.push(tmp);
          const kv = `{decoder_version: ${q(decoderVersion)}, idl_sha256: ${q(idlSha256)}, lane: ${q(lane)}, lo_slot: ${q(String(lo))}, hi_slot: ${q(String(hi))}}`;
          await conn.run(
            `COPY (SELECT ${colList} FROM read_json(${q(st.path)}, format='newline_delimited', columns=${colTypes}, maximum_object_size=134217728) ORDER BY ${order}) ` +
            `TO ${q(tmp)} (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 3, KV_METADATA ${kv})`,
          );
          await fsyncFile(tmp);
          await rename(tmp, final);
          written.pop();
          records.push({ table: st.table, path: rel, rows: st.rows, sha256: await sha256File(final) });
        }
        await cleanup();
        return records;
      } catch (e) {
        for (const p of written) await rm(p, { force: true });
        for (const r of records) await rm(join(dataRoot, r.path), { force: true });
        await cleanup();
        throw e;
      }
    };
    const abort = async () => { await chain.catch(() => undefined); await cleanup(); };
    // serialise adds so appends to one stage file never interleave
    return {
      add: (t) => { chain = chain.then(() => add(t)); return chain; },
      finish: async () => { await chain; return finish(); },
      abort,
    };
  }

  /** Removes this lane's leftover staging and .tmp files. Other lanes' files are untouched. */
  static async cleanupTemp(dataRoot: string, lane: string): Promise<number> {
    let n = 0;
    const staging = join(dataRoot, '.staging', lane);
    try { for (const f of await readdir(staging)) { await rm(join(staging, f), { force: true, recursive: true }); n++; } } catch { /* no staging dir */ }
    const marker = `.${lane}.tmp`;
    const walk = async (dir: string): Promise<void> => {
      let entries;
      try { entries = await readdir(dir, { withFileTypes: true }); } catch { return; }
      for (const e of entries) {
        const p = join(dir, e.name);
        if (e.isDirectory()) { if (e.name !== '.staging') await walk(p); }
        else if (e.name.endsWith(marker)) { await rm(p, { force: true }); n++; }
      }
    };
    await walk(dataRoot);
    return n;
  }
}
