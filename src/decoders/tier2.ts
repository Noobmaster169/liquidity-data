import type { TxModel } from '../tx.ts';
import { resolveTrader } from '../trader.ts';
import type { DecodeContext } from './context.ts';
import { type DecodeOutput, newOutput, bump, pushRow, envelope, failureRow, outerProgramFor } from './events.ts';

export function decodeTier2(tx: TxModel, ctx: DecodeContext): DecodeOutput {
  const out = newOutput();
  for (const ix of tx.instructions) {
    const venue = ctx.registry.tier2.get(ix.programId);
    if (!venue) continue;
    const r = resolveTrader(tx, ix.path);
    if (r.transfers.length === 0) { bump(out, 'tier2_no_transfers'); continue; }
    if (r.parties.length === 0) { bump(out, 'tier2_non_swap'); continue; }
    if (r.userSide === null) { out.failures.push(failureRow(tx, ix.path, ix.programId, 'not_a_swap', null, [], r.transfers)); continue; }
    const m = r.net.net.get(r.userSide)!;
    let mintIn = '', mintOut = '', amountIn = 0n, amountOut = 0n;
    for (const [mint, v] of m) { if (v < 0n) { mintIn = mint; amountIn = -v; } else if (v > 0n) { mintOut = mint; amountOut = v; } }
    pushRow(out, `swaps/${venue.label}`, {
      ...envelope(tx, ix.path, 0, outerProgramFor(tx, ix.path, ix.programId), r.trader),
      program: ix.programId, mint_in: mintIn, amount_in: amountIn.toString(), mint_out: mintOut, amount_out: amountOut.toString(),
    });
  }
  return out;
}
