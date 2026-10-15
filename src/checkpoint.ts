import { mkdir } from 'node:fs/promises';
import { dirname } from 'node:path';
import { openDuck, q, rowsJson, type DuckDBConnection } from './duck.ts';
import type { FileRecord } from './writer.ts';

export interface LaneState { lane: string; startSlot: number; targetSlot: number | null; nextSlot: number }
export interface RangeRecord { lane: string; loSlot: number; hiSlot: number; status: string; decoderVersion: string; files: FileRecord[]; committedAt: string }

export class Checkpoint {
  readonly conn: DuckDBConnection;
  private constructor(conn: DuckDBConnection) { this.conn = conn; }

  static async open(path: string): Promise<Checkpoint> {
    await mkdir(dirname(path), { recursive: true });
    const conn = await openDuck(path);
    await conn.run(`CREATE TABLE IF NOT EXISTS lane_state (lane VARCHAR PRIMARY KEY, start_slot UBIGINT, target_slot UBIGINT, next_slot UBIGINT)`);
    await conn.run(`CREATE TABLE IF NOT EXISTS ranges (lane VARCHAR, lo_slot UBIGINT, hi_slot UBIGINT, status VARCHAR, decoder_version VARCHAR, files JSON, committed_at TIMESTAMP, PRIMARY KEY (lane, lo_slot))`);
    return new Checkpoint(conn);
  }

  async getState(lane: string): Promise<LaneState | null> {
    const rows = await rowsJson(this.conn, `SELECT lane, start_slot, target_slot, next_slot FROM lane_state WHERE lane = ${q(lane)}`);
    const r = rows[0];
    if (!r) return null;
    return { lane, startSlot: Number(r.start_slot), targetSlot: r.target_slot === null ? null : Number(r.target_slot), nextSlot: Number(r.next_slot) };
  }

  async initState(s: LaneState): Promise<void> {
    await this.conn.run(`INSERT OR REPLACE INTO lane_state VALUES (${q(s.lane)}, ${s.startSlot}, ${s.targetSlot === null ? 'NULL' : s.targetSlot}, ${s.nextSlot})`);
  }

  async commitRange(lane: string, lo: number, hi: number, files: FileRecord[], decoderVersion: string, nextSlot: number): Promise<void> {
    await this.conn.run('BEGIN');
    try {
      await this.conn.run(`INSERT OR REPLACE INTO ranges VALUES (${q(lane)}, ${lo}, ${hi}, 'committed', ${q(decoderVersion)}, ${q(JSON.stringify(files))}, now())`);
      await this.conn.run(`UPDATE lane_state SET next_slot = ${nextSlot} WHERE lane = ${q(lane)}`);
      await this.conn.run('COMMIT');
    } catch (e) { await this.conn.run('ROLLBACK'); throw e; }
  }

  async listRanges(lane: string): Promise<RangeRecord[]> {
    const rows = await rowsJson(this.conn, `SELECT lane, lo_slot, hi_slot, status, decoder_version, files::VARCHAR AS files, committed_at::VARCHAR AS committed_at FROM ranges WHERE lane = ${q(lane)} ORDER BY lo_slot`);
    return rows.map((r) => ({ lane, loSlot: Number(r.lo_slot), hiSlot: Number(r.hi_slot), status: String(r.status), decoderVersion: String(r.decoder_version), files: JSON.parse(String(r.files)) as FileRecord[], committedAt: String(r.committed_at) }));
  }

  async close(): Promise<void> { this.conn.closeSync(); }
}
