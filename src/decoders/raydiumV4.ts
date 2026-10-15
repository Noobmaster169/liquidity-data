import { ByteReader, BorshError } from '../borsh.ts';
import type { Row } from '../rows.ts';

export const RAYDIUM_V4 = '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8';

// Layouts transcribed from raydium-amm program/src/log.rs (see idl/SOURCES.md). Byte 0 is the log type.
const LAYOUTS: Record<number, { variant: string; read: (r: ByteReader) => Row }> = {
  0: { variant: 'Init', read: (r) => ({ time: r.u64(), pc_decimals: r.u8(), coin_decimals: r.u8(), pc_lot_size: r.u64(), coin_lot_size: r.u64(), pc_amount: r.u64(), coin_amount: r.u64(), market: r.pubkey() }) },
  1: { variant: 'Deposit', read: (r) => ({ max_coin: r.u64(), max_pc: r.u64(), base: r.u64(), pool_coin: r.u64(), pool_pc: r.u64(), pool_lp: r.u64(), calc_pnl_x: r.u128(), calc_pnl_y: r.u128(), deduct_coin: r.u64(), deduct_pc: r.u64(), mint_lp: r.u64() }) },
  2: { variant: 'Withdraw', read: (r) => ({ withdraw_lp: r.u64(), user_lp: r.u64(), pool_coin: r.u64(), pool_pc: r.u64(), pool_lp: r.u64(), calc_pnl_x: r.u128(), calc_pnl_y: r.u128(), out_coin: r.u64(), out_pc: r.u64() }) },
  3: { variant: 'SwapBaseIn', read: (r) => ({ amount_in: r.u64(), minimum_out: r.u64(), direction: r.u64(), user_source: r.u64(), pool_coin: r.u64(), pool_pc: r.u64(), out_amount: r.u64() }) },
  4: { variant: 'SwapBaseOut', read: (r) => ({ max_in: r.u64(), amount_out: r.u64(), direction: r.u64(), user_source: r.u64(), pool_coin: r.u64(), pool_pc: r.u64(), deduct_in: r.u64() }) },
};

export function decodeRayLog(payload: Uint8Array): { variant: string; fields: Row } {
  const r = new ByteReader(payload);
  const type = r.u8();
  const layout = LAYOUTS[type];
  if (!layout) throw new BorshError(`unknown ray_log type ${type}`);
  const fields = layout.read(r);
  if (r.remaining() !== 0) throw new BorshError(`ray_log ${layout.variant}: ${r.remaining()} bytes left`);
  return { variant: layout.variant, fields };
}

// Raydium v4 instruction tag is data[0]: 0 initialize, 1 initialize2 (amm at account 4); 3 deposit, 4 withdraw, 9 swapBaseIn, 11 swapBaseOut (amm at account 1).
export function ammAccountIndex(ixData: Uint8Array): number { return ixData[0] === 0 || ixData[0] === 1 ? 4 : 1; }
