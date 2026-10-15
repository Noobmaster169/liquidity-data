import type { Column, LoadedIdl } from './idl.ts';
import { fieldColumns } from './idl.ts';
import type { DecodeContext } from './decoders/context.ts';

export const ENVELOPE: Column[] = [
  { name: 'signature', type: 'VARCHAR' }, { name: 'slot', type: 'UBIGINT' }, { name: 'block_time', type: 'TIMESTAMP' },
  { name: 'tx_index', type: 'UINTEGER' }, { name: 'ix_path', type: 'VARCHAR' }, { name: 'event_ordinal', type: 'USMALLINT' },
  { name: 'outer_program', type: 'VARCHAR' }, { name: 'trader', type: 'VARCHAR' },
];
export const SWAP_COLUMNS: Column[] = [...ENVELOPE, { name: 'program', type: 'VARCHAR' }, { name: 'mint_in', type: 'VARCHAR' }, { name: 'amount_in', type: 'UBIGINT' }, { name: 'mint_out', type: 'VARCHAR' }, { name: 'amount_out', type: 'UBIGINT' }];
export const INTENT_COLUMNS: Column[] = [...SWAP_COLUMNS, { name: 'hops', type: 'USMALLINT' }, { name: 'kind', type: 'VARCHAR' }];
export const DLMM_POSITION_COLUMNS: Column[] = [...ENVELOPE, { name: 'position', type: 'VARCHAR' }, { name: 'lb_pair', type: 'VARCHAR' }, { name: 'lower_bin_id', type: 'INTEGER' }, { name: 'width', type: 'INTEGER' }, { name: 'fee_owner', type: 'VARCHAR' }, { name: 'lock_release_point', type: 'UBIGINT' }];
export const BLOCKS_COLUMNS: Column[] = [{ name: 'slot', type: 'UBIGINT' }, { name: 'block_time', type: 'TIMESTAMP' }, { name: 'status', type: 'VARCHAR' }];
export const FAILURE_COLUMNS: Column[] = [
  { name: 'signature', type: 'VARCHAR' }, { name: 'slot', type: 'UBIGINT' }, { name: 'block_time', type: 'TIMESTAMP' }, { name: 'tx_index', type: 'UINTEGER' },
  { name: 'ix_path', type: 'VARCHAR' }, { name: 'program', type: 'VARCHAR' }, { name: 'reason', type: 'VARCHAR' }, { name: 'payload', type: 'VARCHAR' },
  { name: 'log_lines', type: 'VARCHAR[]' },
  { name: 'transfers', type: 'STRUCT("source" VARCHAR, "destination" VARCHAR, "owner_source" VARCHAR, "owner_destination" VARCHAR, "mint" VARCHAR, "amount" UBIGINT)[]' },
];
const U = (n: string): Column => ({ name: n, type: 'UBIGINT' });
export const RAYDIUM_V4_COLUMNS: Record<string, Column[]> = {
  SwapBaseIn: [...ENVELOPE, { name: 'amm', type: 'VARCHAR' }, ...['amount_in', 'minimum_out', 'direction', 'user_source', 'pool_coin', 'pool_pc', 'out_amount'].map(U)],
  SwapBaseOut: [...ENVELOPE, { name: 'amm', type: 'VARCHAR' }, ...['max_in', 'amount_out', 'direction', 'user_source', 'pool_coin', 'pool_pc', 'deduct_in'].map(U)],
  Deposit: [...ENVELOPE, { name: 'amm', type: 'VARCHAR' }, ...['max_coin', 'max_pc', 'base', 'pool_coin', 'pool_pc', 'pool_lp'].map(U), { name: 'calc_pnl_x', type: 'VARCHAR' }, { name: 'calc_pnl_y', type: 'VARCHAR' }, ...['deduct_coin', 'deduct_pc', 'mint_lp'].map(U)],
  Withdraw: [...ENVELOPE, { name: 'amm', type: 'VARCHAR' }, ...['withdraw_lp', 'user_lp', 'pool_coin', 'pool_pc', 'pool_lp'].map(U), { name: 'calc_pnl_x', type: 'VARCHAR' }, { name: 'calc_pnl_y', type: 'VARCHAR' }, ...['out_coin', 'out_pc'].map(U)],
  Init: [...ENVELOPE, { name: 'amm', type: 'VARCHAR' }, U('time'), { name: 'pc_decimals', type: 'UINTEGER' }, { name: 'coin_decimals', type: 'UINTEGER' }, ...['pc_lot_size', 'coin_lot_size', 'pc_amount', 'coin_amount'].map(U), { name: 'market', type: 'VARCHAR' }],
};

export function eventTable(label: string, event: string): string { return `events/${label}/${event}`; }

export function tsString(blockTime: number | null): string | null {
  if (blockTime === null) return null;
  return new Date(blockTime * 1000).toISOString().replace('T', ' ').replace('.000Z', '');
}

export function schemaFor(table: string, ctx: DecodeContext): Column[] {
  if (table === 'blocks') return BLOCKS_COLUMNS;
  if (table === 'decode_failures') return FAILURE_COLUMNS;
  if (table === 'intents') return INTENT_COLUMNS;
  if (table.startsWith('swaps/')) return SWAP_COLUMNS;
  if (table === 'instructions/meteora_dlmm/initialize_position') return DLMM_POSITION_COLUMNS;
  const m = /^events\/([^/]+)\/([^/]+)$/.exec(table);
  if (!m) throw new Error(`unknown table ${table}`);
  const label = m[1]!, event = m[2]!;
  if (label === 'raydium_amm_v4') { const c = RAYDIUM_V4_COLUMNS[event]; if (!c) throw new Error(`unknown ray_log variant ${event}`); return c; }
  const idl: LoadedIdl | undefined = ctx.idlsByLabel.get(label);
  if (!idl) throw new Error(`no IDL for ${label}`);
  const ev = [...idl.events.values()].find((e) => e.name === event);
  if (!ev) throw new Error(`no event ${event} in ${label}`);
  const extra: Column[] = label === 'raydium_clmm' && (event === 'IncreaseLiquidityEvent' || event === 'DecreaseLiquidityEvent') ? [{ name: 'pool_state', type: 'VARCHAR' }] : [];
  return [...ENVELOPE, ...fieldColumns(ev.fields, idl.types), ...extra];
}
