import { isUnder, transfersUnder, type TxModel } from '../tx.ts';
import { netTransfers, resolveTrader } from '../trader.ts';
import type { DecodeContext } from './context.ts';
import { type DecodeOutput, newOutput, bump, pushRow, envelope, failureRow } from './events.ts';

export function decodeIntents(tx: TxModel, ctx: DecodeContext): DecodeOutput {
  const out = newOutput();
  const outers = [...new Set(tx.instructions.filter((i) => ctx.registry.isVenue(i.programId)).map((i) => i.outerIndex))].sort((a, b) => a - b);
  for (const i of outers) {
    const prefix = String(i);
    const r = resolveTrader(tx, prefix);
    if (r.transfers.length === 0) { bump(out, 'intent_no_transfers'); continue; }
    if (r.parties.length === 0) { bump(out, 'intent_no_swap_party'); continue; }
    const hops = tx.instructions.filter((ix) => ctx.registry.isVenue(ix.programId) && isUnder(ix.path, prefix));
    const hopParties = new Set<string>();
    const hopMints = new Set<string>();
    for (const h of hops) for (const t of transfersUnder(tx, h.path)) { if (t.sourceOwner) hopParties.add(t.sourceOwner); if (t.destOwner) hopParties.add(t.destOwner); if (t.mint) hopMints.add(t.mint); }
    const senders = new Set(r.transfers.map((t) => t.sourceOwner));
    // fee legs: to an owner that only receives in this instruction and is not a party to any hop
    const core = r.transfers.filter((t) => !(t.destOwner !== null && !senders.has(t.destOwner) && !hopParties.has(t.destOwner)));
    const n = netTransfers(core);
    const who = r.trader ?? r.userSide;
    const m = who ? n.net.get(who) : undefined;
    if (!who || !m) { out.failures.push(failureRow(tx, prefix, tx.outerPrograms[i]!, 'not_an_intent', null, [], r.transfers)); continue; }
    const neg: string[] = [], pos: string[] = [];
    for (const [mint, v] of m) { if (!hopMints.has(mint) || v === 0n) continue; (v < 0n ? neg : pos).push(mint); }
    let kind: string, mintIn: string, mintOut: string;
    if (neg.length === 1 && pos.length === 1) { kind = 'swap'; mintIn = neg[0]!; mintOut = pos[0]!; }
    else if (neg.length + pos.length <= 1) {
      const sent = n.grossSent.get(who);
      if (!sent || sent.size === 0) { out.failures.push(failureRow(tx, prefix, tx.outerPrograms[i]!, 'not_an_intent', null, [], r.transfers)); continue; }
      kind = 'cycle'; mintIn = [...sent.entries()].sort((a, b) => (a[1] < b[1] ? 1 : -1))[0]![0]; mintOut = mintIn;
    } else { out.failures.push(failureRow(tx, prefix, tx.outerPrograms[i]!, 'not_an_intent', null, [], r.transfers)); continue; }
    const amountIn = n.grossSent.get(who)?.get(mintIn) ?? 0n;
    const amountOut = n.grossRecv.get(who)?.get(mintOut) ?? 0n;
    pushRow(out, 'intents', {
      ...envelope(tx, prefix, 0, null, r.trader),
      program: tx.outerPrograms[i]!, mint_in: mintIn, amount_in: amountIn.toString(), mint_out: mintOut, amount_out: amountOut.toString(), hops: hops.length, kind,
    });
  }
  return out;
}
