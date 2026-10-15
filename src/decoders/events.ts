import { decodeStruct, BorshError } from '../borsh.ts';
import { jsonSafe, type Row } from '../rows.ts';
import type { LogEvent } from '../logs.ts';
import { instructionAt, type Transfer, type TxModel } from '../tx.ts';
import { resolveTrader } from '../trader.ts';
import { eventTable, tsString } from '../tables.ts';
import type { DecodeContext } from './context.ts';
import { RAYDIUM_V4, decodeRayLog, ammAccountIndex } from './raydiumV4.ts';

export const EVENT_CPI_PREFIX = 'e445a52e51cb9a1d';

export interface DecodeOutput { rows: Map<string, Row[]>; failures: Row[] }

export function pushRow(out: DecodeOutput, table: string, row: Row): void {
  let arr = out.rows.get(table);
  if (!arr) { arr = []; out.rows.set(table, arr); }
  arr.push(row);
}

export function outerProgramFor(tx: TxModel, path: string, venue: string): string | null {
  const outer = tx.outerPrograms[Number(path.split('.')[0])]!;
  return outer === venue ? null : outer;
}

export function envelope(tx: TxModel, path: string, ordinal: number, outerProgram: string | null, trader: string | null): Row {
  return { signature: tx.signature, slot: tx.slot, block_time: tsString(tx.blockTime), tx_index: tx.txIndex, ix_path: path, event_ordinal: ordinal, outer_program: outerProgram, trader };
}

export function failureRow(tx: TxModel, path: string, program: string, reason: string, payload: Uint8Array | null, logLines: string[], transfers: Transfer[] | null): Row {
  return {
    signature: tx.signature, slot: tx.slot, block_time: tsString(tx.blockTime), tx_index: tx.txIndex, ix_path: path, program, reason,
    payload: payload ? Buffer.from(payload).toString('base64') : null, log_lines: logLines,
    transfers: transfers ? transfers.map((t) => ({ source: t.source, destination: t.destination, owner_source: t.sourceOwner, owner_destination: t.destOwner, mint: t.mint, amount: t.amount.toString() })) : null,
  };
}

interface Candidate { programId: string; path: string; disc: string; payload: Uint8Array; source: 'cpi' | 'log' }

function collect(tx: TxModel, ctx: DecodeContext, parsed: { events: LogEvent[] }): Candidate[] {
  const seen = new Set<string>();
  const out: Candidate[] = [];
  const add = (c: Candidate) => { const k = `${c.path}|${c.disc}|${Buffer.from(c.payload).toString('hex')}`; if (seen.has(k)) return; seen.add(k); out.push(c); };
  for (const ix of tx.instructions) {
    if (!ctx.idls.has(ix.programId) || ix.data.length < 16) continue;
    if (Buffer.from(ix.data.subarray(0, 8)).toString('hex') !== EVENT_CPI_PREFIX) continue;
    const parent = ix.path.includes('.') ? ix.path.slice(0, ix.path.lastIndexOf('.')) : ix.path;
    add({ programId: ix.programId, path: parent, disc: Buffer.from(ix.data.subarray(8, 16)).toString('hex'), payload: ix.data.subarray(16), source: 'cpi' });
  }
  for (const e of parsed.events) {
    if (e.kind !== 'data' || !ctx.idls.has(e.programId) || e.payload.length < 8) continue;
    add({ programId: e.programId, path: e.path, disc: Buffer.from(e.payload.subarray(0, 8)).toString('hex'), payload: e.payload.subarray(8), source: 'log' });
  }
  return out;
}

export function decodeTier1(tx: TxModel, ctx: DecodeContext, parsed: { events: LogEvent[]; truncated: boolean }): DecodeOutput {
  const out: DecodeOutput = { rows: new Map(), failures: [] };
  const ordinals = new Map<string, number>();
  const traders = new Map<string, string | null>();
  const traderFor = (path: string) => { if (!traders.has(path)) traders.set(path, resolveTrader(tx, path).trader); return traders.get(path)!; };
  const pathsWithEvents = new Set<string>();

  for (const c of collect(tx, ctx, parsed)) {
    const idl = ctx.idls.get(c.programId)!;
    const ev = idl.events.get(c.disc);
    if (!ev) { out.failures.push(failureRow(tx, c.path, c.programId, 'unknown_discriminator', c.payload, [], null)); continue; }
    try {
      const fields = jsonSafe(decodeStruct(ev.fields, c.payload, idl.types)) as Row;
      const key = `${c.path}|${ev.name}`;
      const ordinal = ordinals.get(key) ?? 0;
      ordinals.set(key, ordinal + 1);
      const row: Row = { ...envelope(tx, c.path, ordinal, outerProgramFor(tx, c.path, c.programId), traderFor(c.path)), ...fields };
      if (idl.label === 'raydium_clmm' && (ev.name === 'IncreaseLiquidityEvent' || ev.name === 'DecreaseLiquidityEvent')) {
        const ix = instructionAt(tx, c.path);
        row.pool_state = ix?.accounts[ev.name === 'IncreaseLiquidityEvent' ? 2 : 3] ?? null;
      }
      pushRow(out, eventTable(idl.label, ev.name), row);
      pathsWithEvents.add(c.path);
    } catch (e) {
      const msg = e instanceof BorshError ? e.message : String(e);
      out.failures.push(failureRow(tx, c.path, c.programId, `decode:${msg}`.slice(0, 120), c.payload, [], null));
    }
  }

  for (const e of parsed.events) {
    if (e.kind !== 'raylog' || e.programId !== RAYDIUM_V4) continue;
    try {
      const { variant, fields } = decodeRayLog(e.payload);
      const ix = instructionAt(tx, e.path);
      const row: Row = { ...envelope(tx, e.path, 0, outerProgramFor(tx, e.path, RAYDIUM_V4), traderFor(e.path)), amm: ix ? ix.accounts[ammAccountIndex(ix.data)] ?? null : null, ...(jsonSafe(fields) as Row) };
      pushRow(out, eventTable('raydium_amm_v4', variant), row);
      pathsWithEvents.add(e.path);
    } catch (err) {
      out.failures.push(failureRow(tx, e.path, RAYDIUM_V4, `decode:${err instanceof Error ? err.message : String(err)}`.slice(0, 120), e.payload, [], null));
    }
  }

  if (parsed.truncated) {
    for (const ix of tx.instructions) {
      if (!ctx.registry.tier1.has(ix.programId) || pathsWithEvents.has(ix.path)) continue;
      if (ix.data.length >= 8 && Buffer.from(ix.data.subarray(0, 8)).toString('hex') === EVENT_CPI_PREFIX) continue;
      out.failures.push(failureRow(tx, ix.path, ix.programId, 'log_truncated', ix.data, [], null));
    }
  }
  return out;
}
