import { readFileSync } from 'node:fs';
import { loadRegistry, type Registry } from '../src/registry.ts';
import { loadIdl, type LoadedIdl } from '../src/idl.ts';
import { buildTx, type RpcTransaction, type TxModel } from '../src/tx.ts';

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
let cached: { registry: Registry; idls: Map<string, LoadedIdl> } | null = null;
export function buildContext(): { registry: Registry; idls: Map<string, LoadedIdl> } {
  if (cached) return cached;
  const registry = loadRegistry('config/venues.json', 'config/routers.json');
  const idls = new Map<string, LoadedIdl>();
  for (const v of registry.tier1.values()) if (v.idl) idls.set(v.program_id, loadIdl(v.label, v.idl));
  cached = { registry, idls };
  return cached;
}
