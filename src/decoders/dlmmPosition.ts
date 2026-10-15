import { ByteReader, readType } from '../borsh.ts';
import { jsonSafe, type Row } from '../rows.ts';
import type { TxModel } from '../tx.ts';
import { resolveTrader } from '../trader.ts';
import type { DecodeContext } from './context.ts';
import { type DecodeOutput, newOutput, pushRow, envelope, failureRow, outerProgramFor } from './events.ts';

export const DLMM = 'LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo';
export const DLMM_POSITION_TABLE = 'instructions/meteora_dlmm/initialize_position';

export function decodeDlmmPositions(tx: TxModel, ctx: DecodeContext): DecodeOutput {
  const out = newOutput();
  const idl = ctx.idls.get(DLMM);
  if (!idl) return out;
  for (const ix of tx.instructions) {
    if (ix.programId !== DLMM || ix.data.length < 8) continue;
    const def = idl.instructions.get(Buffer.from(ix.data.subarray(0, 8)).toString('hex'));
    if (!def || !def.name.startsWith('initialize_position')) continue;
    try {
      const r = new ByteReader(ix.data.subarray(8));
      const args: Row = {};
      for (const a of def.args) args[a.name] = readType(a.type, r, idl.types);
      const acc = (name: string) => { const i = def.accounts.indexOf(name); return i >= 0 ? ix.accounts[i] ?? null : null; };
      const a = jsonSafe(args) as Row;
      pushRow(out, DLMM_POSITION_TABLE, {
        ...envelope(tx, ix.path, 0, outerProgramFor(tx, ix.path, DLMM), resolveTrader(tx, ix.path).trader),
        position: acc('position'), lb_pair: acc('lb_pair'), lower_bin_id: a.lower_bin_id ?? null, width: a.width ?? null,
        fee_owner: a.fee_owner ?? null, lock_release_point: a.lock_release_point ?? null,
      });
    } catch (e) {
      out.failures.push(failureRow(tx, ix.path, DLMM, `decode:${e instanceof Error ? e.message : String(e)}`.slice(0, 120), ix.data, [], null));
    }
  }
  return out;
}
