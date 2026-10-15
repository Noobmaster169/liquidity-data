import { test } from 'node:test';
import assert from 'node:assert/strict';
import { decodeRayLog, ammAccountIndex } from '../src/decoders/raydiumV4.ts';
import { parseLogs } from '../src/logs.ts';
import { fixtureTx } from './helpers.ts';
import { instructionAt } from '../src/tx.ts';

test('SwapBaseIn ray_log decodes to seven u64 fields and the amm is instruction account 1', () => {
  const tx = fixtureTx('raydium_v4_raylog');
  const ray = parseLogs(tx.logs).events.find((e) => e.kind === 'raylog')!;
  const { variant, fields } = decodeRayLog(ray.payload);
  assert.equal(variant, 'SwapBaseIn');
  assert.deepEqual(fields, { amount_in: 6707650079n, minimum_out: 3369944938n, direction: 1n, user_source: 667329481906n, pool_coin: 19024065453291n, pool_pc: 37764144298030n, out_amount: 3370001128n });
  const ix = instructionAt(tx, '1')!;
  assert.equal(ammAccountIndex(ix.data), 1);
  assert.equal(ix.accounts[1], '3sjNoCnkkhWPVXYGDtem8rCciHSGc9jSFZuUAzKbvRVp');
});

test('unknown log type or wrong length throws', () => {
  assert.throws(() => decodeRayLog(new Uint8Array([9, 0, 0])));
  assert.throws(() => decodeRayLog(new Uint8Array([3, 0, 0])));
});
