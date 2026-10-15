import { join } from 'node:path';
import type { CollectorConfig } from '../config.ts';
import type { DecodeContext } from '../decoders/context.ts';
import { openDuck, rowsJson, q } from '../duck.ts';
import { decodeStruct } from '../borsh.ts';
import { jsonSafe, type Row } from '../rows.ts';
import { RangeWriter } from '../writer.ts';
import { schemaFor, eventTable } from '../tables.ts';
import { DECODER_VERSION } from '../version.ts';

// Re-runs tier 1 event decoding over decode_failures rows whose reason starts with "decode:" or is "unknown_discriminator",
// using the current IDLs. Recovered rows are written as <lo>-<hi>-redecode-<version>.parquet files; trader and
// outer_program are null because the transaction is not available offline (spec §5, redecode).
export async function redecode(cfg: CollectorConfig, ctx: DecodeContext, flags: Record<string, string>): Promise<void> {
  const conn = await openDuck(':memory:');
  const glob = join(cfg.dataRoot, 'decode_failures', '*', '*.parquet');
  const where = flags.from && flags.to ? `AND slot BETWEEN ${Number(flags.from)} AND ${Number(flags.to)}` : '';
  const rows = await rowsJson(conn, `SELECT signature, slot, block_time::VARCHAR AS block_time, tx_index, ix_path, program, reason, payload FROM read_parquet(${q(glob)}) WHERE (reason LIKE 'decode:%' OR reason = 'unknown_discriminator') AND payload IS NOT NULL ${where}`);
  const tables = new Map<string, Row[]>();
  let recovered = 0, still = 0;
  for (const r of rows) {
    const idl = ctx.idls.get(String(r.program));
    if (!idl) { still++; continue; }
    const payload = Buffer.from(String(r.payload), 'base64');
    // a failed decode stores the bytes after the discriminator; unknown_discriminator stores disc + payload
    const known = String(r.reason) === 'unknown_discriminator' ? idl.events.get(payload.subarray(0, 8).toString('hex')) : undefined;
    const body = known ? payload.subarray(8) : payload;
    const ev = known ?? [...idl.events.values()].find((e) => { try { decodeStruct(e.fields, body, idl.types); return true; } catch { return false; } });
    if (!ev) { still++; continue; }
    try {
      const fields = jsonSafe(decodeStruct(ev.fields, body, idl.types)) as Row;
      const row: Row = { signature: r.signature, slot: Number(r.slot), block_time: r.block_time, tx_index: Number(r.tx_index), ix_path: r.ix_path, event_ordinal: 0, outer_program: null, trader: null, ...fields };
      const t = eventTable(idl.label, ev.name);
      const arr = tables.get(t); if (arr) arr.push(row); else tables.set(t, [row]);
      recovered++;
    } catch { still++; }
  }
  if (recovered) {
    const lo = Math.min(...rows.map((r) => Number(r.slot))), hi = Math.max(...rows.map((r) => Number(r.slot)));
    const writer = new RangeWriter({ dataRoot: cfg.dataRoot, conn, decoderVersion: DECODER_VERSION, schemaFor: (t) => schemaFor(t, ctx), suffix: `-redecode-${DECODER_VERSION}` });
    const files = await writer.writeRange(`redecode-${DECODER_VERSION}`, lo, hi, tables);
    console.log(`recovered ${recovered} rows into ${files.length} files; ${still} still failing`);
  } else console.log(`nothing recovered; ${still} still failing`);
}
