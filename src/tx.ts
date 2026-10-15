import { decode as b58decode, encode as b58encode } from './base58.ts';

export interface RpcInstruction { programIdIndex: number; accounts: number[]; data: string; stackHeight?: number | null }
export interface RpcTokenBalance { accountIndex: number; mint: string; owner?: string; programId?: string; uiTokenAmount: { amount: string; decimals: number } }
export interface RpcMeta {
  err: unknown; fee?: number; logMessages?: string[] | null;
  innerInstructions?: { index: number; instructions: RpcInstruction[] }[] | null;
  loadedAddresses?: { writable: string[]; readonly: string[] } | null;
  preTokenBalances?: RpcTokenBalance[] | null; postTokenBalances?: RpcTokenBalance[] | null;
}
export interface RpcTransaction {
  transaction: { signatures: string[]; message: { accountKeys: string[]; header: { numRequiredSignatures: number }; instructions: RpcInstruction[] } };
  meta: RpcMeta | null; version?: 'legacy' | number;
}
export interface RpcBlock { blockTime: number | null; blockHeight: number | null; parentSlot: number; transactions: RpcTransaction[] }

export const TOKEN_PROGRAMS = new Set(['TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA', 'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb']);
export const SYSTEM_PROGRAM = '11111111111111111111111111111111';
export const ATA_PROGRAM = 'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL';
export const VOTE_PROGRAM = 'Vote111111111111111111111111111111111111111';
export const WSOL_MINT = 'So11111111111111111111111111111111111111112';

export interface Instruction { path: string; outerIndex: number; programId: string; accounts: string[]; data: Uint8Array }
export interface TokenAccountInfo { mint: string | null; owner: string | null }
export interface Transfer { path: string; source: string; destination: string; mint: string | null; amount: bigint; sourceOwner: string | null; destOwner: string | null }
export interface TxModel {
  signature: string; slot: number; blockTime: number | null; txIndex: number; success: boolean; isVote: boolean;
  keys: string[]; signers: Set<string>; instructions: Instruction[]; outerPrograms: string[]; programs: Set<string>;
  logs: string[]; tokenAccounts: Map<string, TokenAccountInfo>; txDeltas: Map<string, bigint>; transfers: Transfer[];
}

function view(d: Uint8Array): Buffer { return Buffer.from(d.buffer, d.byteOffset, d.byteLength); }

export function buildTx(raw: RpcTransaction, slot: number, blockTime: number | null, txIndex: number): TxModel | null {
  const meta = raw.meta;
  if (!meta) return null;
  const msg = raw.transaction.message;
  const keys = [...msg.accountKeys, ...(meta.loadedAddresses?.writable ?? []), ...(meta.loadedAddresses?.readonly ?? [])];
  const inner = new Map<number, RpcInstruction[]>();
  for (const g of meta.innerInstructions ?? []) inner.set(g.index, g.instructions);
  const instructions: Instruction[] = [];
  const outerPrograms: string[] = [];
  msg.instructions.forEach((ix, i) => {
    const programId = keys[ix.programIdIndex]!;
    outerPrograms.push(programId);
    instructions.push({ path: String(i), outerIndex: i, programId, accounts: ix.accounts.map((a) => keys[a]!), data: b58decode(ix.data) });
    let counters: number[] = [];
    for (const iix of inner.get(i) ?? []) {
      const depth = Math.max(0, (iix.stackHeight ?? 2) - 2);
      counters = counters.slice(0, depth + 1);
      while (counters.length < depth + 1) counters.push(0);
      counters[depth]!++;
      instructions.push({ path: `${i}.${counters.join('.')}`, outerIndex: i, programId: keys[iix.programIdIndex]!, accounts: iix.accounts.map((a) => keys[a]!), data: b58decode(iix.data) });
    }
  });
  const programs = new Set(instructions.map((x) => x.programId));
  const tx: TxModel = {
    signature: raw.transaction.signatures[0]!, slot, blockTime, txIndex, success: meta.err == null, isVote: programs.has(VOTE_PROGRAM),
    keys, signers: new Set(keys.slice(0, msg.header.numRequiredSignatures)), instructions, outerPrograms, programs,
    logs: meta.logMessages ?? [], tokenAccounts: resolveTokenAccounts(meta, keys, instructions), txDeltas: tokenDeltas(meta), transfers: [],
  };
  tx.transfers = extractTransfers(tx);
  return tx;
}

function resolveTokenAccounts(meta: RpcMeta, keys: string[], instructions: Instruction[]): Map<string, TokenAccountInfo> {
  const m = new Map<string, TokenAccountInfo>();
  for (const tb of [...(meta.preTokenBalances ?? []), ...(meta.postTokenBalances ?? [])]) m.set(keys[tb.accountIndex]!, { mint: tb.mint, owner: tb.owner ?? null });
  for (const ix of instructions) {
    if (TOKEN_PROGRAMS.has(ix.programId) && ix.data.length > 0) {
      const tag = ix.data[0];
      const acct = ix.accounts[0];
      if (acct === undefined || m.has(acct)) continue;
      if (tag === 1 && ix.accounts.length >= 3) m.set(acct, { mint: ix.accounts[1]!, owner: ix.accounts[2]! });
      else if ((tag === 16 || tag === 18) && ix.accounts.length >= 2 && ix.data.length >= 33) m.set(acct, { mint: ix.accounts[1]!, owner: b58encode(ix.data.subarray(1, 33)) });
    } else if (ix.programId === ATA_PROGRAM && ix.accounts.length >= 4 && (ix.data.length === 0 || ix.data[0] === 0 || ix.data[0] === 1)) {
      const ata = ix.accounts[1]!;
      if (!m.has(ata)) m.set(ata, { mint: ix.accounts[3]!, owner: ix.accounts[2]! });
    }
  }
  return m;
}

function tokenDeltas(meta: RpcMeta): Map<string, bigint> {
  const d = new Map<string, bigint>();
  for (const tb of meta.preTokenBalances ?? []) { const k = `${tb.owner ?? ''}|${tb.mint}`; d.set(k, (d.get(k) ?? 0n) - BigInt(tb.uiTokenAmount.amount)); }
  for (const tb of meta.postTokenBalances ?? []) { const k = `${tb.owner ?? ''}|${tb.mint}`; d.set(k, (d.get(k) ?? 0n) + BigInt(tb.uiTokenAmount.amount)); }
  return d;
}

export function extractTransfers(tx: TxModel): Transfer[] {
  const out: Transfer[] = [];
  for (const ix of tx.instructions) {
    const d = ix.data;
    if (TOKEN_PROGRAMS.has(ix.programId) && d.length >= 9 && (d[0] === 3 || d[0] === 12)) {
      const checked = d[0] === 12;
      if (ix.accounts.length < (checked ? 4 : 3)) continue;
      const source = ix.accounts[0]!;
      const destination = checked ? ix.accounts[2]! : ix.accounts[1]!;
      const authority = checked ? ix.accounts[3]! : ix.accounts[2]!;
      const s = tx.tokenAccounts.get(source);
      const t = tx.tokenAccounts.get(destination);
      out.push({ path: ix.path, source, destination, mint: s?.mint ?? t?.mint ?? (checked ? ix.accounts[1]! : null), amount: view(d).readBigUInt64LE(1), sourceOwner: s?.owner ?? authority, destOwner: t?.owner ?? null });
    } else if (ix.programId === SYSTEM_PROGRAM && d.length >= 12 && ix.accounts.length >= 2 && view(d).readUInt32LE(0) === 2) {
      out.push({ path: ix.path, source: ix.accounts[0]!, destination: ix.accounts[1]!, mint: WSOL_MINT, amount: view(d).readBigUInt64LE(4), sourceOwner: ix.accounts[0]!, destOwner: ix.accounts[1]! });
    }
  }
  return out;
}

export function isUnder(path: string, prefix: string): boolean { return path === prefix || path.startsWith(prefix + '.'); }
export function transfersUnder(tx: TxModel, prefix: string): Transfer[] { return tx.transfers.filter((t) => t.path !== prefix && isUnder(t.path, prefix)); }
export function instructionAt(tx: TxModel, path: string): Instruction | undefined { return tx.instructions.find((i) => i.path === path); }
