import { readFileSync } from 'node:fs';
import { loadRegistry } from '../src/registry.ts';
import { buildTx, type RpcTransaction, type TxModel } from '../src/tx.ts';
import { buildDecodeContext, type DecodeContext } from '../src/decoders/context.ts';

export interface Fixture { name: string; slot: number; blockTime: number; txIndex: number; tx: RpcTransaction }
export function loadFixture(name: string): Fixture {
  return JSON.parse(readFileSync(`tests/fixtures/${name}.json`, 'utf8')) as Fixture;
}
export function fixtureTx(name: string): TxModel {
  const f = loadFixture(name);
  const tx = buildTx(f.tx, f.slot, f.blockTime, f.txIndex);
  if (!tx) throw new Error(`fixture ${name} has null meta`);
  return tx;
}
let cached: DecodeContext | null = null;
export function buildContext(): DecodeContext {
  if (!cached) cached = buildDecodeContext(loadRegistry('config/venues.json', 'config/routers.json'));
  return cached;
}
