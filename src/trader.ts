import type { Transfer, TxModel } from './tx.ts';
import { isUnder, transfersUnder } from './tx.ts';

export type OwnerNet = Map<string, Map<string, bigint>>;
export interface NetResult { net: OwnerNet; grossSent: OwnerNet; grossRecv: OwnerNet }

function add(m: OwnerNet, owner: string, mint: string, v: bigint): void {
  let o = m.get(owner);
  if (!o) { o = new Map(); m.set(owner, o); }
  o.set(mint, (o.get(mint) ?? 0n) + v);
}

export function netTransfers(trs: Transfer[]): NetResult {
  const r: NetResult = { net: new Map(), grossSent: new Map(), grossRecv: new Map() };
  for (const t of trs) {
    if (t.mint === null) continue;
    if (t.sourceOwner) { add(r.net, t.sourceOwner, t.mint, -t.amount); add(r.grossSent, t.sourceOwner, t.mint, t.amount); }
    if (t.destOwner) { add(r.net, t.destOwner, t.mint, t.amount); add(r.grossRecv, t.destOwner, t.mint, t.amount); }
  }
  return r;
}

export function swapParties(n: NetResult): string[] {
  const out: string[] = [];
  for (const [owner, mints] of n.net) {
    let neg = 0, pos = 0;
    for (const v of mints.values()) { if (v < 0n) neg++; else if (v > 0n) pos++; }
    if (neg === 1 && pos === 1) out.push(owner);
  }
  return out;
}

export interface TraderResult { transfers: Transfer[]; net: NetResult; parties: string[]; userSide: string | null; trader: string | null }

export function resolveTrader(tx: TxModel, path: string): TraderResult {
  const transfers = transfersUnder(tx, path);
  const net = netTransfers(transfers);
  const parties = swapParties(net);
  let userSide: string | null = null;
  const signing = parties.filter((p) => tx.signers.has(p));
  if (signing.length === 1) {
    userSide = signing[0]!;
  } else if (signing.length === 0 && parties.length >= 1) {
    const outside = new Set<string>();
    for (const t of tx.transfers) if (!isUnder(t.path, path)) { outside.add(t.source); outside.add(t.destination); }
    const cands = parties.filter((p) => transfers.some((t) => (t.sourceOwner === p && outside.has(t.source)) || (t.destOwner === p && outside.has(t.destination))));
    if (cands.length === 1) userSide = cands[0]!;
  }
  let trader: string | null = userSide !== null && tx.signers.has(userSide) ? userSide : null;
  if (trader === null) {
    const withDeltas = [...tx.signers].filter((s) => { for (const [k, v] of tx.txDeltas) if (v !== 0n && k.startsWith(s + '|')) return true; return false; });
    if (withDeltas.length === 1) trader = withDeltas[0]!;
  }
  return { transfers, net, parties, userSide, trader };
}
