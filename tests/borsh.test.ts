import { test } from 'node:test';
import assert from 'node:assert/strict';
import { ByteReader, BorshError, decodeStruct, type Types, type IdlField } from '../src/borsh.ts';

const types: Types = new Map([
  ['Pair', { name: 'Pair', type: { kind: 'struct', fields: [{ name: 'a', type: 'u64' }, { name: 'b', type: 'i32' }] } }],
  ['Side', { name: 'Side', type: { kind: 'enum', variants: [{ name: 'Buy' }, { name: 'Sell' }] } }],
]);

test('reads primitives little-endian', () => {
  const r = new ByteReader(new Uint8Array([1, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe, 0xff, 0xff, 0xff]));
  assert.equal(r.u8(), 1);
  assert.equal(r.u64(), 18446744073709551615n);
  assert.equal(r.i32(), -2);
  assert.equal(r.remaining(), 0);
});

test('u128 and i128', () => {
  const b = new Uint8Array(32).fill(0xff);
  const r = new ByteReader(b);
  assert.equal(r.u128(), (1n << 128n) - 1n);
  assert.equal(r.i128(), -1n);
});

test('decodeStruct handles nested struct, array, vec, option, enum, pubkey', () => {
  const fields: IdlField[] = [
    { name: 'p', type: { defined: { name: 'Pair' } } },
    { name: 'arr', type: { array: ['u8', 2] } },
    { name: 'v', type: { vec: 'u16' } },
    { name: 'o', type: { option: 'u8' } },
    { name: 's', type: { defined: { name: 'Side' } } },
    { name: 'k', type: 'pubkey' },
    { name: 'flag', type: 'bool' },
  ];
  const data = Buffer.concat([
    Buffer.from([5, 0, 0, 0, 0, 0, 0, 0, 0xfe, 0xff, 0xff, 0xff]),
    Buffer.from([9, 8]),
    Buffer.from([2, 0, 0, 0, 1, 0, 2, 0]),
    Buffer.from([1, 7]),
    Buffer.from([1]),
    Buffer.alloc(32),
    Buffer.from([1]),
  ]);
  const out = decodeStruct(fields, new Uint8Array(data), types);
  assert.deepEqual(out, { p: { a: 5n, b: -2 }, arr: [9, 8], v: [1, 2], o: 7, s: 'Sell', k: '11111111111111111111111111111111', flag: true });
});

test('length mismatch and short data throw BorshError', () => {
  assert.throws(() => decodeStruct([{ name: 'a', type: 'u8' }], new Uint8Array([1, 2]), types), BorshError);
  assert.throws(() => decodeStruct([{ name: 'a', type: 'u64' }], new Uint8Array([1, 2]), types), BorshError);
});
