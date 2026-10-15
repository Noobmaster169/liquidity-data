import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { loadIdl, discriminatorHex, duckdbType, fieldColumns } from '../src/idl.ts';
import { decodeStruct } from '../src/borsh.ts';
import { jsonSafe } from '../src/rows.ts';
import { decode } from '../src/base58.ts';

const dlmm = loadIdl('meteora_dlmm', 'idl/meteora_dlmm.json');

test('loads events keyed by discriminator and instructions with flattened accounts', () => {
  assert.equal(dlmm.address, 'LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo');
  assert.equal(dlmm.events.size, 30);
  const swap2 = [...dlmm.events.values()].find((e) => e.name === 'Swap2Evt');
  assert.ok(swap2);
  assert.deepEqual(swap2.fields.map((f) => f.name).slice(0, 3), ['lb_pair', 'from', 'start_bin_id']);
  const init = [...dlmm.instructions.values()].find((i) => i.name === 'initialize_position');
  assert.ok(init);
  assert.deepEqual(init.args.map((a) => a.name), ['lower_bin_id', 'width']);
  assert.deepEqual(init.accounts.slice(0, 4), ['payer', 'position', 'lb_pair', 'owner']);
  assert.equal(dlmm.sha256.length, 64);
});

test('discriminator is sha256("event:Name")[0..8] when not given', () => {
  assert.equal(discriminatorHex('event', 'Swap', undefined), discriminatorHex('event', 'Swap', [81, 108, 227, 190, 205, 208, 10, 196]));
});

test('duckdb types follow the spec mapping', () => {
  assert.equal(duckdbType('u64', dlmm.types), 'UBIGINT');
  assert.equal(duckdbType('u128', dlmm.types), 'VARCHAR');
  assert.equal(duckdbType('i32', dlmm.types), 'INTEGER');
  assert.equal(duckdbType('pubkey', dlmm.types), 'VARCHAR');
  assert.equal(duckdbType({ array: ['u64', 2] }, dlmm.types), 'UBIGINT[]');
  const damm = loadIdl('meteora_damm_v2', 'idl/meteora_damm_v2.json');
  const swap2 = [...damm.events.values()].find((e) => e.name === 'EvtSwap2')!;
  const cols = fieldColumns(swap2.fields, damm.types);
  assert.ok(cols.find((c) => c.name === 'params')!.type.startsWith('STRUCT('));
});

test('decodes a real Swap2Evt payload from the fixture and jsonSafe stringifies bigints', () => {
  const fx = JSON.parse(readFileSync('tests/fixtures/jupiter_multihop_dlmm_swap2.json', 'utf8'));
  const ix = fx.tx.meta.innerInstructions.find((g: { index: number }) => g.index === 3).instructions as { data: string }[];
  const cpi = ix.map((i) => decode(i.data)).find((d) => Buffer.from(d.subarray(0, 8)).toString('hex') === 'e445a52e51cb9a1d' && dlmm.events.get(Buffer.from(d.subarray(8, 16)).toString('hex'))?.name === 'Swap2Evt')!;
  const ev = dlmm.events.get(Buffer.from(cpi.subarray(8, 16)).toString('hex'))!;
  const out = jsonSafe(decodeStruct(ev.fields, cpi.subarray(16), dlmm.types)) as Record<string, unknown>;
  assert.equal(out.lb_pair, 'zxTpi4BtaWX3mgdAPoezkMD1hxx8CdeCfrqXMWvSCLX');
  assert.equal(out.amount_in, '104023597531');
  assert.equal(out.amount_out, '110065105');
  assert.equal(out.fee_bps, '2145563');
  assert.equal(out.start_bin_id, -3428);
  assert.equal(out.swap_for_y, true);
});
