import { test } from 'node:test';
import assert from 'node:assert/strict';
import { encode, decode } from '../src/base58.ts';

test('decodes a known pubkey to 32 bytes and round-trips', () => {
  const s = 'LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo';
  const b = decode(s);
  assert.equal(b.length, 32);
  assert.equal(encode(b), s);
});

test('preserves leading zero bytes', () => {
  assert.equal(encode(new Uint8Array([0, 0, 1])), '112');
  assert.deepEqual([...decode('112')], [0, 0, 1]);
  assert.equal(encode(new Uint8Array(32)), '1'.repeat(32));
});

test('decodes the system program id to 32 zero bytes', () => {
  assert.deepEqual([...decode('11111111111111111111111111111111')], new Array(32).fill(0));
});

test('rejects invalid characters', () => {
  assert.throws(() => decode('0OIl'), /invalid base58/);
});
