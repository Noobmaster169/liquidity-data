import { buildTx, type RpcBlock } from '../tx.ts';
import { parseLogs } from '../logs.ts';
import type { Row } from '../rows.ts';
import { tsString } from '../tables.ts';
import type { DecodeContext } from './context.ts';
import { decodeTier1, type DecodeOutput } from './events.ts';
import { decodeTier2 } from './tier2.ts';
import { decodeIntents } from './intents.ts';
import { decodeDlmmPositions } from './dlmmPosition.ts';

export interface DecodedBlock { slot: number; tables: Map<string, Row[]>; stats: Record<string, number> }

export function mergeTables(into: Map<string, Row[]>, from: Map<string, Row[]>): void {
  for (const [t, rows] of from) { const arr = into.get(t); if (arr) arr.push(...rows); else into.set(t, [...rows]); }
}
function mergeStats(into: Record<string, number>, from: Record<string, number>): void {
  for (const [k, v] of Object.entries(from)) into[k] = (into[k] ?? 0) + v;
}
function absorb(d: DecodedBlock, o: DecodeOutput): void {
  mergeTables(d.tables, o.rows);
  if (o.failures.length) { const f = d.tables.get('decode_failures'); if (f) f.push(...o.failures); else d.tables.set('decode_failures', [...o.failures]); }
  mergeStats(d.stats, o.stats);
}

export function decodeBlock(block: RpcBlock | null, slot: number, ctx: DecodeContext): DecodedBlock {
  const d: DecodedBlock = { slot, tables: new Map(), stats: {} };
  if (!block) { d.tables.set('blocks', [{ slot, block_time: null, status: 'skipped' }]); return d; }
  d.tables.set('blocks', [{ slot, block_time: tsString(block.blockTime), status: 'ok' }]);
  const bump = (k: string) => { d.stats[k] = (d.stats[k] ?? 0) + 1; };
  block.transactions.forEach((raw, txIndex) => {
    bump('tx_total');
    const tx = buildTx(raw, slot, block.blockTime, txIndex);
    if (!tx) { bump('tx_null_meta'); return; }
    if (tx.isVote) { bump('tx_vote'); return; }
    if (!tx.success) { bump('tx_failed'); return; }
    let touches = false;
    for (const p of tx.programs) if (ctx.registry.isVenue(p)) { touches = true; break; }
    if (!touches) return;
    bump('tx_venue');
    const parsed = parseLogs(tx.logs);
    absorb(d, decodeTier1(tx, ctx, parsed));
    absorb(d, decodeTier2(tx, ctx));
    absorb(d, decodeIntents(tx, ctx));
    absorb(d, decodeDlmmPositions(tx, ctx));
  });
  return d;
}
