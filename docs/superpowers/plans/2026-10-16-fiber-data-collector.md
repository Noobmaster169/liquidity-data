# fiber-data Collector Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the `fiber-data` collector: a TypeScript program that fetches every finalized Solana block over RPC, decodes DEX events and swaps from the ten tier 1 programs and all tier 2 venues, and writes them as typed Parquet tables with resumable checkpoints.

**Architecture:** One Node 24 binary with two lanes (`tail`, `backfill`) that process slot ranges. Per block: build a transaction model, extract events by self-CPI and by log, decode with the vendored IDLs, net token transfers for tier 2 swaps and intents, apply the trader rule, then stage rows as JSONL and let DuckDB write ZSTD Parquet with typed columns. A DuckDB checkpoint file per lane records committed ranges.

**Tech Stack:** Node 24 (native TypeScript type stripping, no build step), `@duckdb/node-api 1.5.6-r.1`, `node:test`, no other runtime dependencies. Base58 and Borsh are implemented in-repo.

**Spec:** `docs/superpowers/specs/2026-10-15-fiber-data-collector-design.md` — read it first. Vendored IDLs are in `idl/` (see `idl/SOURCES.md`), real transaction fixtures are in `tests/fixtures/` (see `tests/fixtures/README.md`).

## Global Constraints

- Node `>=24`; code runs directly with `node src/cli.ts`. TypeScript must be erasable: no `enum`, no `namespace`, no constructor parameter properties, `import type` for types, relative imports end in `.ts`. `tsconfig` sets `erasableSyntaxOnly: true`.
- Only runtime dependency: `@duckdb/node-api` pinned to `1.5.6-r.1`.
- RPC: `getBlock` with `encoding: "json"`, `transactionDetails: "full"`, `maxSupportedTransactionVersion: 1`, `rewards: false`, commitment `finalized`. The RPC URL comes from `SOLANA_RPC_URL` in `.env` and is never logged or included in error messages.
- Amounts are integers in base units. u64 → `bigint` in memory, `UBIGINT` in Parquet. u128/i128 → decimal string, `VARCHAR` in Parquet. No floats.
- Instruction paths: outer index is 0-based (`4`), inner siblings count from 1 (`4.1`, `4.1.2`).
- Envelope on every table, in this order: `signature, slot, block_time, tx_index, ix_path, event_ordinal, outer_program, trader`.
- Parquet: ZSTD level 3, sorted by `(slot, tx_index, ix_path)`, KV metadata `decoder_version, idl_sha256, lane, lo_slot, hi_slot`, path `data/<table>/date=YYYY-MM-DD/<lo>-<hi>.parquet`, written as `.tmp` then fsync then rename.
- Default range 5,000 slots. A range commits only after all of its slots are `ok` or `skipped` (RPC codes -32007, -32009). Any other error retries.
- Failed and vote transactions are dropped. Venue instructions with no transfers produce nothing.
- `DECODER_VERSION` is a constant in `src/version.ts`, bumped whenever an IDL or decoder changes.
- Git: commit after every task with the message shown; end commit messages with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`.

## Review Focus

1. A block whose `blockTime` is null (happens on some historical slots): rows must still be written, `block_time` null, partitioned into the date of the nearest block in the range with a time. Test in Task 9.
2. A transfer whose mint cannot be resolved (account absent from balance metadata, not initialised in the transaction): it must be excluded from netting without crashing, and a venue call that then has no swap-shaped party is skipped silently. Test in Task 5.
3. A transaction whose `meta` is null (RPC returns this for some failed historical transactions): skipped and counted, no crash. Test in Task 3.
4. Inner instructions with `stackHeight: null` (older RPC versions): treated as depth 2. Test in Task 3.
5. A 429 in the middle of a range: the slot is retried and never recorded as skipped; the range does not commit until the retry succeeds. Test in Task 10.

## File Structure

```
package.json, tsconfig.json, .gitignore, .env.example, README.md
config/venues.json          generated registry (tier 1, tier 2, excluded)
config/routers.json         router labels
config/collector.json       rates, range size, paths, workers
config/sources/…            Jupiter label list (already present)
idl/*.json                  vendored IDLs (already present)
scripts/build-venues.ts     generates config/venues.json from config/sources
src/version.ts              DECODER_VERSION
src/base58.ts               encode/decode
src/borsh.ts                ByteReader, IDL type reader, decodeStruct
src/idl.ts                  loadIdl, discriminators, DuckDB column types
src/rows.ts                 Row type, jsonSafe
src/registry.ts             venue/router registry
src/tx.ts                   RPC types, TxModel builder, token-account resolution, transfers
src/logs.ts                 log parsing: Program data, ray_log, truncation, paths
src/trader.ts               netting, swap parties, trader rule
src/tables.ts               column schemas for every table
src/decoders/raydiumV4.ts   ray_log layouts
src/decoders/events.ts      tier 1 event rows (CPI + log, dedupe, added pool columns)
src/decoders/tier2.ts       swaps rows
src/decoders/intents.ts     intents rows
src/decoders/dlmmPosition.ts initialize_position rows
src/decoders/block.ts       decodeBlock orchestrator
src/rpc.ts                  RateLimiter, RpcClient
src/writer.ts               RangeWriter (JSONL staging → Parquet via DuckDB)
src/checkpoint.ts           per-lane DuckDB state
src/lane.ts                 processRange, runLane (tail, backfill)
src/decodePool.ts, src/worker.ts   worker_threads decode pool
src/config.ts               config + env loading
src/commands/{status,verify,redecode,registryCheck,query}.ts
src/cli.ts                  command dispatch
tests/helpers.ts            fixture loader, context builder
tests/*.test.ts             one file per module
Dockerfile, compose.yaml
```

---

### Task 1: Project scaffold and base58

**Files:**
- Create: `package.json`, `tsconfig.json`, `.gitignore`, `.env.example`, `src/version.ts`, `src/base58.ts`
- Test: `tests/base58.test.ts`

**Interfaces:**
- Produces: `encode(bytes: Uint8Array): string`, `decode(s: string): Uint8Array` from `src/base58.ts`; `DECODER_VERSION: string` from `src/version.ts`.

- [ ] **Step 1: Write the scaffold files**

`package.json`:
```json
{
  "name": "fiber-data",
  "version": "0.1.0",
  "private": true,
  "type": "module",
  "engines": { "node": ">=24.0.0" },
  "scripts": {
    "check": "tsc --noEmit",
    "test": "node --test 'tests/**/*.test.ts'",
    "fiber-data": "node src/cli.ts"
  },
  "dependencies": { "@duckdb/node-api": "1.5.6-r.1" },
  "devDependencies": { "typescript": "^5.9.0", "@types/node": "^24.0.0" }
}
```

`tsconfig.json`:
```json
{
  "compilerOptions": {
    "target": "es2022", "lib": ["es2022"], "module": "nodenext", "moduleResolution": "nodenext",
    "strict": true, "noEmit": true, "allowImportingTsExtensions": true, "verbatimModuleSyntax": true,
    "erasableSyntaxOnly": true, "types": ["node"], "skipLibCheck": true, "noUncheckedIndexedAccess": true
  },
  "include": ["src", "tests", "scripts"]
}
```

`.gitignore`:
```
node_modules/
data/
state/
.env
*.tmp
```

`.env.example`:
```
SOLANA_RPC_URL=https://your-provider.example/your-key
```

`src/version.ts`:
```ts
export const DECODER_VERSION = '1.0.0';
```

- [ ] **Step 2: Install and check the toolchain**

Run: `npm install && npx tsc --version && node --version`
Expected: typescript 5.9.x, node v24.x, `@duckdb/node-api` installed without build errors.

- [ ] **Step 3: Write the failing base58 test**

`tests/base58.test.ts`:
```ts
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
```

- [ ] **Step 4: Run it to verify it fails**

Run: `npm test`
Expected: FAIL, cannot find module `../src/base58.ts`.

- [ ] **Step 5: Implement base58**

`src/base58.ts`:
```ts
const ALPHABET = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz';
const INDEX = new Map<string, bigint>([...ALPHABET].map((c, i) => [c, BigInt(i)]));

export function decode(s: string): Uint8Array {
  let n = 0n;
  for (const c of s) {
    const v = INDEX.get(c);
    if (v === undefined) throw new Error(`invalid base58 character ${JSON.stringify(c)}`);
    n = n * 58n + v;
  }
  let zeros = 0;
  while (zeros < s.length && s[zeros] === '1') zeros++;
  let hex = n === 0n ? '' : n.toString(16);
  if (hex.length % 2 === 1) hex = '0' + hex;
  const body = Buffer.from(hex, 'hex');
  const out = new Uint8Array(zeros + body.length);
  out.set(body, zeros);
  return out;
}

export function encode(bytes: Uint8Array): string {
  let zeros = 0;
  while (zeros < bytes.length && bytes[zeros] === 0) zeros++;
  const hex = Buffer.from(bytes).toString('hex');
  let n = hex.length === 0 ? 0n : BigInt('0x' + hex);
  let out = '';
  while (n > 0n) {
    out = ALPHABET[Number(n % 58n)] + out;
    n /= 58n;
  }
  return '1'.repeat(zeros) + out;
}
```

- [ ] **Step 6: Run tests and the type check**

Run: `npm test && npm run check`
Expected: 4 tests pass, tsc reports no errors.

- [ ] **Step 7: Commit**

```bash
git add package.json package-lock.json tsconfig.json .gitignore .env.example src/version.ts src/base58.ts tests/base58.test.ts
git commit -m "Scaffold: Node 24 TypeScript project, base58 codec

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 2: Borsh reader and IDL loader

**Files:**
- Create: `src/borsh.ts`, `src/idl.ts`, `src/rows.ts`
- Test: `tests/borsh.test.ts`, `tests/idl.test.ts`

**Interfaces:**
- Consumes: `encode` from `src/base58.ts`.
- Produces:
  - `src/borsh.ts`: `class ByteReader`, `class BorshError`, types `IdlType`, `IdlField`, `IdlTypeDef`, `Types = Map<string, IdlTypeDef>`, `readType(ty, reader, types): unknown`, `decodeStruct(fields, data, types): Record<string, unknown>` (throws `BorshError` on short data or leftover bytes).
  - `src/idl.ts`: `interface LoadedIdl { label; address; version; types; events: Map<discHex, EventDef>; instructions: Map<discHex, InstructionDef>; sha256 }`, `EventDef { name; fields }`, `InstructionDef { name; args; accounts: string[] }`, `Column { name; type }`, `loadIdl(label, path): LoadedIdl`, `discriminatorHex(prefix, name, given?)`, `duckdbType(ty, types): string`, `fieldColumns(fields, types): Column[]`.
  - `src/rows.ts`: `type Row = Record<string, unknown>`, `jsonSafe(v): unknown` (bigint → decimal string, Uint8Array → hex, recursive).

- [ ] **Step 1: Write the failing tests**

`tests/borsh.test.ts`:
```ts
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
    Buffer.from([5, 0, 0, 0, 0, 0, 0, 0, 0xfe, 0xff, 0xff, 0xff]), // Pair{a:5,b:-2}
    Buffer.from([9, 8]),                                            // arr
    Buffer.from([2, 0, 0, 0, 1, 0, 2, 0]),                          // vec [1,2]
    Buffer.from([1, 7]),                                            // Some(7)
    Buffer.from([1]),                                               // Sell
    Buffer.alloc(32),                                               // system program
    Buffer.from([1]),
  ]);
  const out = decodeStruct(fields, new Uint8Array(data), types);
  assert.deepEqual(out, { p: { a: 5n, b: -2 }, arr: [9, 8], v: [1, 2], o: 7, s: 'Sell', k: '11111111111111111111111111111111', flag: true });
});

test('length mismatch and short data throw BorshError', () => {
  assert.throws(() => decodeStruct([{ name: 'a', type: 'u8' }], new Uint8Array([1, 2]), types), BorshError);
  assert.throws(() => decodeStruct([{ name: 'a', type: 'u64' }], new Uint8Array([1, 2]), types), BorshError);
});
```

`tests/idl.test.ts`:
```ts
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { loadIdl, discriminatorHex, duckdbType, fieldColumns } from '../src/idl.ts';
import { decodeStruct } from '../src/borsh.ts';
import { jsonSafe } from '../src/rows.ts';

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
  const fx = JSON.parse(require('node:fs').readFileSync('tests/fixtures/jupiter_multihop_dlmm_swap2.json', 'utf8'));
  // inner instruction at path 3.1.3 is the self-CPI event: data = e445a52e51cb9a1d + disc + payload
  const { decode } = require('../src/base58.ts');
  const ix = fx.tx.meta.innerInstructions.find((g: any) => g.index === 3).instructions;
  const cpi = ix.map((i: any) => decode(i.data)).find((d: Uint8Array) => Buffer.from(d.subarray(0, 8)).toString('hex') === 'e445a52e51cb9a1d' && dlmm.events.get(Buffer.from(d.subarray(8, 16)).toString('hex'))?.name === 'Swap2Evt');
  const ev = dlmm.events.get(Buffer.from(cpi.subarray(8, 16)).toString('hex'))!;
  const out = jsonSafe(decodeStruct(ev.fields, cpi.subarray(16), dlmm.types)) as Record<string, unknown>;
  assert.equal(out.lb_pair, 'zxTpi4BtaWX3mgdAPoezkMD1hxx8CdeCfrqXMWvSCLX');
  assert.equal(out.amount_in, '104023597531');
  assert.equal(out.amount_out, '110065105');
  assert.equal(out.fee_bps, '2145563');
  assert.equal(out.start_bin_id, -3428);
  assert.equal(out.swap_for_y, true);
});
```
Note for the last test: ESM test files cannot use `require`; write it with `import { readFileSync } from 'node:fs'` and `import { decode } from '../src/base58.ts'` at the top instead. The intent is unchanged.

- [ ] **Step 2: Run to verify failure**

Run: `npm test`
Expected: FAIL, modules `../src/borsh.ts`, `../src/idl.ts` not found.

- [ ] **Step 3: Implement `src/borsh.ts`**

```ts
import { encode as b58encode } from './base58.ts';

export class BorshError extends Error {}

export type IdlType =
  | string
  | { array: [IdlType, number] }
  | { vec: IdlType }
  | { option: IdlType }
  | { coption: IdlType }
  | { defined: { name: string } | string };

export interface IdlField { name: string; type: IdlType }
export type IdlTypeDef =
  | { name: string; type: { kind: 'struct'; fields?: IdlField[] } }
  | { name: string; type: { kind: 'enum'; variants: { name: string; fields?: IdlField[] | IdlType[] }[] } }
  | { name: string; type: { kind: 'type'; alias: IdlType } };
export type Types = Map<string, IdlTypeDef>;

export class ByteReader {
  readonly buf: Buffer;
  pos = 0;
  constructor(bytes: Uint8Array) { this.buf = Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength); }
  remaining(): number { return this.buf.length - this.pos; }
  private need(n: number): void {
    if (this.pos + n > this.buf.length) throw new BorshError(`need ${n} bytes at offset ${this.pos}, ${this.remaining()} left`);
  }
  u8(): number { this.need(1); return this.buf[this.pos++]!; }
  i8(): number { this.need(1); return this.buf.readInt8(this.pos++); }
  u16(): number { this.need(2); const v = this.buf.readUInt16LE(this.pos); this.pos += 2; return v; }
  i16(): number { this.need(2); const v = this.buf.readInt16LE(this.pos); this.pos += 2; return v; }
  u32(): number { this.need(4); const v = this.buf.readUInt32LE(this.pos); this.pos += 4; return v; }
  i32(): number { this.need(4); const v = this.buf.readInt32LE(this.pos); this.pos += 4; return v; }
  u64(): bigint { this.need(8); const v = this.buf.readBigUInt64LE(this.pos); this.pos += 8; return v; }
  i64(): bigint { this.need(8); const v = this.buf.readBigInt64LE(this.pos); this.pos += 8; return v; }
  u128(): bigint { const lo = this.u64(); const hi = this.u64(); return (hi << 64n) | lo; }
  i128(): bigint { return BigInt.asIntN(128, this.u128()); }
  f32(): number { this.need(4); const v = this.buf.readFloatLE(this.pos); this.pos += 4; return v; }
  f64(): number { this.need(8); const v = this.buf.readDoubleLE(this.pos); this.pos += 8; return v; }
  bool(): boolean { return this.u8() !== 0; }
  bytes(n: number): Uint8Array { this.need(n); const out = new Uint8Array(this.buf.subarray(this.pos, this.pos + n)); this.pos += n; return out; }
  pubkey(): string { return b58encode(this.bytes(32)); }
  string(): string { const n = this.u32(); return Buffer.from(this.bytes(n)).toString('utf8'); }
}

function definedName(d: { name: string } | string): string { return typeof d === 'string' ? d : d.name; }

export function readType(ty: IdlType, r: ByteReader, types: Types): unknown {
  if (typeof ty === 'string') {
    switch (ty) {
      case 'u8': return r.u8(); case 'i8': return r.i8();
      case 'u16': return r.u16(); case 'i16': return r.i16();
      case 'u32': return r.u32(); case 'i32': return r.i32();
      case 'u64': return r.u64(); case 'i64': return r.i64();
      case 'u128': return r.u128(); case 'i128': return r.i128();
      case 'f32': return r.f32(); case 'f64': return r.f64();
      case 'bool': return r.bool(); case 'string': return r.string();
      case 'pubkey': case 'publicKey': return r.pubkey();
      case 'bytes': { const n = r.u32(); return Buffer.from(r.bytes(n)).toString('hex'); }
      default: throw new BorshError(`unknown primitive type ${ty}`);
    }
  }
  if ('array' in ty) { const [inner, n] = ty.array; const out: unknown[] = []; for (let i = 0; i < n; i++) out.push(readType(inner, r, types)); return out; }
  if ('vec' in ty) { const n = r.u32(); const out: unknown[] = []; for (let i = 0; i < n; i++) out.push(readType(ty.vec, r, types)); return out; }
  if ('option' in ty) return r.u8() === 0 ? null : readType(ty.option, r, types);
  if ('coption' in ty) return r.u32() === 0 ? null : readType(ty.coption, r, types);
  if ('defined' in ty) {
    const name = definedName(ty.defined);
    const def = types.get(name);
    if (!def) throw new BorshError(`unknown defined type ${name}`);
    const t = def.type;
    if (t.kind === 'struct') { const out: Record<string, unknown> = {}; for (const f of t.fields ?? []) out[f.name] = readType(f.type, r, types); return out; }
    if (t.kind === 'enum') {
      const idx = r.u8();
      const v = t.variants[idx];
      if (!v) throw new BorshError(`enum ${name} has no variant ${idx}`);
      if (!v.fields || v.fields.length === 0) return v.name;
      const first = v.fields[0] as unknown;
      if (typeof first === 'object' && first !== null && 'name' in first && 'type' in first) {
        const out: Record<string, unknown> = {};
        for (const f of v.fields as IdlField[]) out[f.name] = readType(f.type, r, types);
        return { [v.name]: out };
      }
      return { [v.name]: (v.fields as IdlType[]).map((ft) => readType(ft, r, types)) };
    }
    return readType(t.alias, r, types);
  }
  throw new BorshError(`unsupported type ${JSON.stringify(ty)}`);
}

export function decodeStruct(fields: IdlField[], data: Uint8Array, types: Types): Record<string, unknown> {
  const r = new ByteReader(data);
  const out: Record<string, unknown> = {};
  for (const f of fields) out[f.name] = readType(f.type, r, types);
  if (r.remaining() !== 0) throw new BorshError(`length mismatch: ${r.remaining()} bytes left after ${fields.length} fields`);
  return out;
}
```

- [ ] **Step 4: Implement `src/idl.ts` and `src/rows.ts`**

`src/idl.ts`:
```ts
import { readFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import type { IdlField, IdlType, IdlTypeDef, Types } from './borsh.ts';

export interface EventDef { name: string; fields: IdlField[] }
export interface InstructionDef { name: string; args: IdlField[]; accounts: string[] }
export interface LoadedIdl {
  label: string; address: string; version: string; types: Types;
  events: Map<string, EventDef>; instructions: Map<string, InstructionDef>; sha256: string;
}
export interface Column { name: string; type: string }

interface RawAccount { name: string; accounts?: RawAccount[] }
interface RawIdl {
  address: string; metadata?: { version?: string }; types?: IdlTypeDef[];
  events?: { name: string; discriminator?: number[]; fields?: IdlField[] }[];
  instructions?: { name: string; discriminator?: number[]; args?: IdlField[]; accounts?: RawAccount[] }[];
}

export function discriminatorHex(prefix: 'event' | 'global', name: string, given?: number[]): string {
  if (given) return Buffer.from(given).toString('hex');
  return createHash('sha256').update(`${prefix}:${name}`).digest().subarray(0, 8).toString('hex');
}

function flattenAccounts(accs: RawAccount[] | undefined): string[] {
  const out: string[] = [];
  for (const a of accs ?? []) { if (a.accounts) out.push(...flattenAccounts(a.accounts)); else out.push(a.name); }
  return out;
}

export function loadIdl(label: string, path: string): LoadedIdl {
  const text = readFileSync(path, 'utf8');
  const raw = JSON.parse(text) as RawIdl;
  const types: Types = new Map((raw.types ?? []).map((t) => [t.name, t]));
  const events = new Map<string, EventDef>();
  for (const e of raw.events ?? []) {
    const def = types.get(e.name);
    const fields = e.fields ?? (def && def.type.kind === 'struct' ? def.type.fields ?? [] : []);
    events.set(discriminatorHex('event', e.name, e.discriminator), { name: e.name, fields });
  }
  const instructions = new Map<string, InstructionDef>();
  for (const i of raw.instructions ?? []) {
    instructions.set(discriminatorHex('global', i.name, i.discriminator), { name: i.name, args: i.args ?? [], accounts: flattenAccounts(i.accounts) });
  }
  return { label, address: raw.address, version: raw.metadata?.version ?? 'unknown', types, events, instructions, sha256: createHash('sha256').update(text).digest('hex') };
}

export function duckdbType(ty: IdlType, types: Types): string {
  if (typeof ty === 'string') {
    switch (ty) {
      case 'u8': case 'u16': case 'u32': return 'UINTEGER';
      case 'u64': return 'UBIGINT';
      case 'i8': case 'i16': case 'i32': return 'INTEGER';
      case 'i64': return 'BIGINT';
      case 'u128': case 'i128': return 'VARCHAR';
      case 'f32': case 'f64': return 'DOUBLE';
      case 'bool': return 'BOOLEAN';
      case 'string': case 'pubkey': case 'publicKey': case 'bytes': return 'VARCHAR';
      default: throw new Error(`no DuckDB type for ${ty}`);
    }
  }
  if ('array' in ty) return `${duckdbType(ty.array[0], types)}[]`;
  if ('vec' in ty) return `${duckdbType(ty.vec, types)}[]`;
  if ('option' in ty) return duckdbType(ty.option, types);
  if ('coption' in ty) return duckdbType(ty.coption, types);
  const name = typeof ty.defined === 'string' ? ty.defined : ty.defined.name;
  const def = types.get(name);
  if (!def) throw new Error(`unknown defined type ${name}`);
  const t = def.type;
  if (t.kind === 'struct') {
    const fields = t.fields ?? [];
    if (fields.length === 0) return 'JSON';
    return `STRUCT(${fields.map((f) => `"${f.name}" ${duckdbType(f.type, types)}`).join(', ')})`;
  }
  if (t.kind === 'enum') return t.variants.every((v) => !v.fields || v.fields.length === 0) ? 'VARCHAR' : 'JSON';
  return duckdbType(t.alias, types);
}

export function fieldColumns(fields: IdlField[], types: Types): Column[] {
  return fields.map((f) => ({ name: f.name, type: duckdbType(f.type, types) }));
}
```

`src/rows.ts`:
```ts
export type Row = Record<string, unknown>;

export function jsonSafe(v: unknown): unknown {
  if (typeof v === 'bigint') return v.toString();
  if (v instanceof Uint8Array) return Buffer.from(v).toString('hex');
  if (Array.isArray(v)) return v.map(jsonSafe);
  if (v !== null && typeof v === 'object') {
    const o: Row = {};
    for (const [k, x] of Object.entries(v as Row)) o[k] = jsonSafe(x);
    return o;
  }
  return v;
}
```

- [ ] **Step 5: Run tests and type check**

Run: `npm test && npm run check`
Expected: all borsh and idl tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/borsh.ts src/idl.ts src/rows.ts tests/borsh.test.ts tests/idl.test.ts
git commit -m "Borsh reader and IDL loader with DuckDB type mapping

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 3: Registry and transaction model

**Files:**
- Create: `src/registry.ts`, `scripts/build-venues.ts`, `config/venues.json` (generated), `config/routers.json`, `src/tx.ts`, `tests/helpers.ts`
- Test: `tests/registry.test.ts`, `tests/tx.test.ts`

**Interfaces:**
- Consumes: `decode`/`encode` from `src/base58.ts`.
- Produces:
  - `src/registry.ts`: `interface VenueEntry { program_id; label; kind; tier: 0|1|2; idl?: string; reason?: string }`, `interface Registry { tier1: Map<string, VenueEntry>; tier2: Map<string, VenueEntry>; routers: Map<string, string>; isVenue(p): boolean; labelOf(p): string }`, `loadRegistry(venuesPath, routersPath): Registry`.
  - `src/tx.ts`: RPC JSON types `RpcInstruction`, `RpcTokenBalance`, `RpcMeta`, `RpcTransaction`, `RpcBlock`; constants `TOKEN_PROGRAMS`, `SYSTEM_PROGRAM`, `ATA_PROGRAM`, `VOTE_PROGRAM`, `WSOL_MINT`; `interface Instruction { path; outerIndex; programId; accounts: string[]; data: Uint8Array }`; `interface Transfer { path; source; destination; mint: string|null; amount: bigint; sourceOwner: string|null; destOwner: string|null }`; `interface TxModel { signature; slot; blockTime: number|null; txIndex; success; isVote; keys; signers: Set<string>; instructions: Instruction[]; outerPrograms: string[]; programs: Set<string>; logs: string[]; tokenAccounts: Map<string,{mint,owner}>; txDeltas: Map<string,bigint>; transfers: Transfer[] }`; `buildTx(raw, slot, blockTime, txIndex): TxModel | null` (null when `meta` is null); `isUnder(path, prefix)`; `transfersUnder(tx, prefix): Transfer[]`; `instructionAt(tx, path): Instruction | undefined`.
  - `tests/helpers.ts`: `loadFixture(name): { slot; blockTime; txIndex; tx: RpcTransaction }`, `fixtureTx(name): TxModel`, `buildContext(): DecodeContext` (defined in Task 6; until then it returns `{ registry, idls }` where `idls` is `Map<programId, LoadedIdl>` for the nine IDL files).

- [ ] **Step 1: Write `config/routers.json` and the venue generator**

`config/routers.json`:
```json
{
  "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4": "jupiter_v6",
  "61DFfeTKM7trxYcPQCM78bJ794ddZprZpAwAnLiwTpYH": "jupiter_order_engine",
  "fd3nMFYTQjX1yr5ER8u7tPdHJB7qt8RpDpNtLQX2Br5": "jupiter_rfq_v2",
  "routeUGWgWzqBWFcrCfv8tritsqukccJPu3q5GPP3xS": "raydium_route",
  "T1TANpTeScyeqVzzgNViGDNrkQ6qHz9KrSBS4aNXvGT": "titan",
  "DF1ow4tspfHX9JwWJsAb9epbkA8hmpSEAtxXy1V27QBH": "dflow"
}
```

`scripts/build-venues.ts`:
```ts
// Generates config/venues.json from the Jupiter program-id-to-label snapshot plus curated tier 1 and exclusion lists.
import { readFileSync, writeFileSync } from 'node:fs';

const source = JSON.parse(readFileSync('config/sources/jupiter_program_id_to_label_2026-10-15.json', 'utf8')) as Record<string, string>;
const routers = JSON.parse(readFileSync('config/routers.json', 'utf8')) as Record<string, string>;

const TIER1: Record<string, { label: string; kind: string; idl: string | null }> = {
  LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo: { label: 'meteora_dlmm', kind: 'dlmm', idl: 'idl/meteora_dlmm.json' },
  cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG: { label: 'meteora_damm_v2', kind: 'clmm', idl: 'idl/meteora_damm_v2.json' },
  dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN: { label: 'meteora_dbc', kind: 'bonding_curve', idl: 'idl/meteora_dbc.json' },
  whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc: { label: 'orca_whirlpool', kind: 'clmm', idl: 'idl/orca_whirlpool.json' },
  CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK: { label: 'raydium_clmm', kind: 'clmm', idl: 'idl/raydium_clmm.json' },
  CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C: { label: 'raydium_cpmm', kind: 'amm', idl: 'idl/raydium_cpmm.json' },
  '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8': { label: 'raydium_amm_v4', kind: 'amm', idl: null },
  LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj: { label: 'raydium_launchlab', kind: 'bonding_curve', idl: 'idl/raydium_launchlab.json' },
  pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA: { label: 'pumpswap', kind: 'amm', idl: 'idl/pumpswap.json' },
  '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P': { label: 'pumpfun', kind: 'bonding_curve', idl: 'idl/pumpfun.json' },
};
// Programs in the Jupiter list that are not spot swaps. Kept with tier 0 and a reason so the exclusion is visible.
const EXCLUDE: Record<string, string> = {
  'Sanctum': 'LST stake/unstake router', 'Sanctum Infinity': 'LST pool', 'Sanctum Prop S': 'LST', 'Sanctum SOLS': 'LST',
  'Jupiter Lend Earn': 'lending', 'JupiterRfqV2': 'router, see routers.json', 'Perps': 'perpetuals', 'Helium Network': 'not a DEX',
  'Solayer': 'restaking', 'XOrca': 'staking', 'VaultLiquidUnstake': 'unstake', 'Carrot': 'yield vault', 'Voltr': 'vault',
  'Huma': 'credit', 'Perena': 'stable LP mint', 'Perena Star V2': 'stable LP mint',
};
const EXTRA: { program_id: string; label: string; kind: string }[] = [
  { program_id: 'goonERTdGsjnkZqWuVjs73BZ3Pb9qoCUdBUL17BnS5j', label: 'goonfi', kind: 'prop' },
  { program_id: '2wT8Yq49kHgDzXuPxZSaeLaH1qbmGXtEyPy64bL7aD3c', label: 'lifinity_v2', kind: 'prop' },
  { program_id: 'EewxydAPCCVuNEyrVN68PuSYdQ7wKn27V9Gjeoi8dy3S', label: 'lifinity_v1', kind: 'prop' },
  { program_id: '9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP', label: 'orca_v2', kind: 'amm' },
];
const PROP = new Set(['BisonFi', 'TesseraV', 'HumidiFi', 'GoonFi V2', 'SolFi', 'SolFi V2', 'ZeroFi', 'Obric V2', 'Quantum', 'AlphaQ', 'Scorch', 'Hadron', 'Obsidian', 'Archer']);
const CLOB = new Set(['Manifest', 'Phoenix', 'OpenBook V2']);

function slug(s: string): string { return s.toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_|_$/g, ''); }

const out: Record<string, unknown>[] = [];
for (const [pid, t] of Object.entries(TIER1)) out.push({ program_id: pid, label: t.label, kind: t.kind, tier: 1, idl: t.idl });
for (const [pid, name] of Object.entries(source)) {
  if (TIER1[pid] || routers[pid]) continue;
  if (EXCLUDE[name]) { out.push({ program_id: pid, label: slug(name), kind: 'excluded', tier: 0, reason: EXCLUDE[name] }); continue; }
  out.push({ program_id: pid, label: slug(name), kind: PROP.has(name) ? 'prop' : CLOB.has(name) ? 'clob' : 'amm', tier: 2 });
}
for (const e of EXTRA) if (!out.some((o) => o.program_id === e.program_id)) out.push({ ...e, tier: 2 });
writeFileSync('config/venues.json', JSON.stringify(out, null, 2) + '\n');
console.log(`wrote ${out.length} entries: tier1=${out.filter((o) => o.tier === 1).length} tier2=${out.filter((o) => o.tier === 2).length} excluded=${out.filter((o) => o.tier === 0).length}`);
```

Run: `node scripts/build-venues.ts`
Expected: `wrote 111 entries: tier1=10 tier2=85 excluded=16` (counts may differ by one or two; tier1 must be exactly 10).

- [ ] **Step 2: Write the failing registry and tx tests**

`tests/helpers.ts`:
```ts
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
```

`tests/registry.test.ts`:
```ts
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { loadRegistry } from '../src/registry.ts';

test('registry has ten tier 1 programs, prop AMMs in tier 2, routers separate, exclusions visible', () => {
  const r = loadRegistry('config/venues.json', 'config/routers.json');
  assert.equal(r.tier1.size, 10);
  assert.equal(r.tier1.get('LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo')?.label, 'meteora_dlmm');
  assert.equal(r.tier2.get('TessVdML9pBGgG9yGks7o4HewRaXVAMuoVj4x83GLQH')?.kind, 'prop');
  assert.equal(r.tier2.get('BiSoNHVpsVZW2F7rx2eQ59yQwKxzU5NvBcmKshCSUypi')?.label, 'bisonfi');
  assert.ok(r.isVenue('9H6tua7jkLhdm3w8BvgpTn5LZNU7g4ZynDmCiNN3q6Rp'));
  assert.ok(!r.isVenue('JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4'));
  assert.equal(r.routers.get('JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4'), 'jupiter_v6');
  assert.equal(r.labelOf('JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4'), 'jupiter_v6');
  assert.ok(!r.isVenue('stkitrT1Uoy18Dk1fTrgPw8W6MVzoCfYoAFT4MLsmhq'), 'Sanctum router is excluded');
});
```

`tests/tx.test.ts`:
```ts
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { buildTx, transfersUnder, instructionAt, WSOL_MINT } from '../src/tx.ts';
import { loadFixture, fixtureTx } from './helpers.ts';

const DLMM = 'LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo';

test('instruction paths: outer 0-based, inner siblings from 1, nested depth from stackHeight', () => {
  const tx = fixtureTx('jupiter_multihop_dlmm_swap2');
  assert.equal(tx.instructions.length, 15);
  const dlmm = tx.instructions.filter((i) => i.programId === DLMM).map((i) => i.path);
  assert.deepEqual(dlmm, ['3.1', '3.1.3', '3.1.4']);
  assert.equal(tx.outerPrograms[3], 'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4');
  assert.equal(instructionAt(tx, '3.1')?.accounts.length, 19);
});

test('signers, success, vote, version 1 and legacy', () => {
  const tx = fixtureTx('jupiter_multihop_dlmm_swap2');
  assert.deepEqual([...tx.signers], ['5op7ba3vhSGZgewfa4TGSMZU7wnHkVGmLFYunzzuQwxp', 'sighWH8KaiT7QhtV4w29ReVF8kG6D5yG3EQP1KYyGVF']);
  assert.equal(tx.success, true);
  assert.equal(fixtureTx('vote').isVote, true);
  assert.equal(fixtureTx('failed_venue_tx').success, false);
  assert.equal(fixtureTx('version1_tx').instructions.length, 5);
});

test('null meta yields null; null stackHeight is depth 2', () => {
  const f = loadFixture('whirlpool_traded');
  assert.equal(buildTx({ ...f.tx, meta: null }, f.slot, f.blockTime, f.txIndex), null);
  const copy = JSON.parse(JSON.stringify(f.tx));
  for (const g of copy.meta.innerInstructions ?? []) for (const ix of g.instructions) ix.stackHeight = null;
  const tx = buildTx(copy, f.slot, f.blockTime, f.txIndex)!;
  assert.ok(tx.instructions.every((i) => i.path.split('.').length <= 2));
});

test('token account owners come from balance metadata, InitializeAccount3, then transfer authority', () => {
  const tx = fixtureTx('ephemeral_wsol_tier2_swap');
  const init = instructionAt(tx, '0.1.4')!;              // InitializeAccount3 for the ephemeral wSOL account
  assert.equal(init.data[0], 18);
  const info = tx.tokenAccounts.get(init.accounts[0]!)!;
  assert.equal(info.mint, WSOL_MINT);
  assert.equal(info.owner, '97itvmxTihn33ZJg3adr5hpgCmjdEU92oARk59yNYzv');
  const trs = transfersUnder(tx, '0.4');                 // Tessera V hop
  assert.equal(trs.length, 2);
  assert.ok(trs.every((t) => t.mint !== null && t.sourceOwner !== null && t.destOwner !== null));
});

test('Token-2022 TransferChecked carries its mint from the instruction accounts', () => {
  const tx = fixtureTx('token2022_transfer_checked');
  const t = tx.transfers.find((x) => x.path === '4.2')!;
  assert.equal(t.mint, '3CtbiqdLW3iHFqBd1vYZyENpVxjMMb5H8oxCRAvXnHsp');
  assert.equal(t.amount, 106740368042097n);
});

test('transaction-level token deltas are keyed owner|mint', () => {
  const tx = fixtureTx('jupiter_shared_accounts_hop');
  const signer = 'CpTescFMHdEzHPgjt7pTKxapBfEkd2qtuyY2rc6Nrioe';
  assert.ok([...tx.txDeltas.entries()].some(([k, v]) => k.startsWith(signer + '|') && v !== 0n));
});
```

- [ ] **Step 3: Run to verify failure**

Run: `npm test`
Expected: FAIL, `../src/registry.ts` and `../src/tx.ts` not found.

- [ ] **Step 4: Implement `src/registry.ts`**

```ts
import { readFileSync } from 'node:fs';

export interface VenueEntry { program_id: string; label: string; kind: string; tier: 0 | 1 | 2; idl?: string | null; reason?: string }

export interface Registry {
  tier1: Map<string, VenueEntry>;
  tier2: Map<string, VenueEntry>;
  routers: Map<string, string>;
  isVenue(programId: string): boolean;
  labelOf(programId: string): string;
}

export function loadRegistry(venuesPath: string, routersPath: string): Registry {
  const entries = JSON.parse(readFileSync(venuesPath, 'utf8')) as VenueEntry[];
  const routersRaw = JSON.parse(readFileSync(routersPath, 'utf8')) as Record<string, string>;
  const tier1 = new Map<string, VenueEntry>();
  const tier2 = new Map<string, VenueEntry>();
  for (const e of entries) {
    if (e.tier === 1) tier1.set(e.program_id, e);
    else if (e.tier === 2) tier2.set(e.program_id, e);
  }
  const routers = new Map(Object.entries(routersRaw));
  return {
    tier1, tier2, routers,
    isVenue: (p) => tier1.has(p) || tier2.has(p),
    labelOf: (p) => tier1.get(p)?.label ?? tier2.get(p)?.label ?? routers.get(p) ?? p,
  };
}
```

- [ ] **Step 5: Implement `src/tx.ts`**

```ts
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
```

- [ ] **Step 6: Run tests and type check**

Run: `npm test && npm run check`
Expected: registry and tx tests pass (plus Tasks 1–2).

- [ ] **Step 7: Commit**

```bash
git add src/registry.ts src/tx.ts scripts/build-venues.ts config/venues.json config/routers.json tests/helpers.ts tests/registry.test.ts tests/tx.test.ts
git commit -m "Venue registry and transaction model with instruction paths, token-account resolution and transfers

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 4: Log parsing

**Files:**
- Create: `src/logs.ts`
- Test: `tests/logs.test.ts`

**Interfaces:**
- Produces: `interface LogEvent { programId; path; kind: 'data' | 'raylog'; payload: Uint8Array }`, `parseLogs(logs: string[]): { events: LogEvent[]; truncated: boolean }`.

- [ ] **Step 1: Write the failing test**

`tests/logs.test.ts`:
```ts
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { parseLogs } from '../src/logs.ts';
import { fixtureTx } from './helpers.ts';

test('Program data lines are attributed to the invoking program and instruction path', () => {
  const tx = fixtureTx('whirlpool_traded');
  const { events, truncated } = parseLogs(tx.logs);
  assert.equal(truncated, false);
  const traded = events.filter((e) => e.kind === 'data' && e.programId === 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc');
  assert.equal(traded.length, 1);
  assert.equal(traded[0]!.path, '2');
});

test('nested CPI paths match the innerInstructions numbering', () => {
  const tx = fixtureTx('raydium_cpmm_swap');
  const { events } = parseLogs(tx.logs);
  const cpmm = events.find((e) => e.programId === 'CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C')!;
  assert.equal(cpmm.path, '2.8');
  assert.equal(tx.instructions.find((i) => i.path === '2.8')?.programId, cpmm.programId);
});

test('ray_log lines are captured with their raw bytes', () => {
  const tx = fixtureTx('raydium_v4_raylog');
  const { events } = parseLogs(tx.logs);
  const ray = events.find((e) => e.kind === 'raylog')!;
  assert.equal(ray.path, '1');
  assert.equal(ray.payload.length, 57);
  assert.equal(ray.payload[0], 3);
});

test('Log truncated is detected', () => {
  assert.equal(parseLogs(fixtureTx('log_truncated').logs).truncated, true);
});

test('synthetic: sibling numbering and depth return', () => {
  const logs = [
    'Program A invoke [1]', 'Program B invoke [2]', 'Program data: AQ==', 'Program B success',
    'Program C invoke [2]', 'Program D invoke [3]', 'Program data: Ag==', 'Program D success', 'Program C success',
    'Program A success', 'Program E invoke [1]', 'Program data: Aw==', 'Program E success',
  ];
  const { events } = parseLogs(logs);
  assert.deepEqual(events.map((e) => [e.programId, e.path, e.payload[0]]), [['B', '0.1', 1], ['D', '0.2.1', 2], ['E', '1', 3]]);
});
```

- [ ] **Step 2: Run to verify failure**

Run: `npm test -- --test-name-pattern='Program data'`
Expected: FAIL, module not found.

- [ ] **Step 3: Implement `src/logs.ts`**

```ts
export interface LogEvent { programId: string; path: string; kind: 'data' | 'raylog'; payload: Uint8Array }

export function parseLogs(logs: string[]): { events: LogEvent[]; truncated: boolean } {
  const events: LogEvent[] = [];
  const stack: { programId: string; path: string }[] = [];
  const counters: number[] = [];
  let truncated = false;
  for (const line of logs) {
    if (line.startsWith('Program ') && line.includes(' invoke [')) {
      const programId = line.split(' ')[1]!;
      const depth = Number(line.slice(line.lastIndexOf('[') + 1, line.lastIndexOf(']')));
      counters.length = Math.min(counters.length, depth);
      while (counters.length < depth) counters.push(0);
      counters[depth - 1]!++;
      const path = depth === 1 ? String(counters[0]! - 1) : `${counters[0]! - 1}.${counters.slice(1).join('.')}`;
      stack.push({ programId, path });
    } else if (line.startsWith('Program ') && (line.endsWith(' success') || line.includes(' failed: '))) {
      stack.pop();
    } else if (line.startsWith('Program data: ')) {
      const top = stack[stack.length - 1];
      if (top) events.push({ programId: top.programId, path: top.path, kind: 'data', payload: new Uint8Array(Buffer.from(line.slice(14), 'base64')) });
    } else if (line.startsWith('Program log: ray_log: ')) {
      const top = stack[stack.length - 1];
      if (top) events.push({ programId: top.programId, path: top.path, kind: 'raylog', payload: new Uint8Array(Buffer.from(line.slice(22), 'base64')) });
    } else if (line === 'Log truncated') {
      truncated = true;
    }
  }
  return { events, truncated };
}
```

- [ ] **Step 4: Run tests**

Run: `npm test && npm run check`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/logs.ts tests/logs.test.ts
git commit -m "Log parser: Program data and ray_log attribution by invoke stack, truncation flag

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 5: Netting and the trader rule

**Files:**
- Create: `src/trader.ts`
- Test: `tests/trader.test.ts`

**Interfaces:**
- Consumes: `TxModel`, `Transfer`, `transfersUnder`, `isUnder` from `src/tx.ts`.
- Produces: `type OwnerNet = Map<string, Map<string, bigint>>`, `interface NetResult { net; grossSent; grossRecv }`, `netTransfers(trs): NetResult`, `swapParties(n): string[]`, `interface TraderResult { transfers; net; parties; userSide: string|null; trader: string|null }`, `resolveTrader(tx, path): TraderResult`.

- [ ] **Step 1: Write the failing test**

`tests/trader.test.ts`:
```ts
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { resolveTrader, netTransfers, swapParties } from '../src/trader.ts';
import { fixtureTx } from './helpers.ts';
import type { Transfer } from '../src/tx.ts';

test('direct signer is the trader; both parties net symmetrically', () => {
  const tx = fixtureTx('humidifi_swap');
  const r = resolveTrader(tx, '4.5');
  assert.equal(r.transfers.length, 2);
  assert.deepEqual([...r.parties].sort(), ['4B8srvotmWddTZuMt4RrBQrnx1sQj2Z7FaXgLbWvpVdd', 'hKgG7iEDRFNsJSwLYqz8ETHuZwzh6qMMLow8VXa8pLm']);
  assert.equal(r.userSide, '4B8srvotmWddTZuMt4RrBQrnx1sQj2Z7FaXgLbWvpVdd');
  assert.equal(r.trader, '4B8srvotmWddTZuMt4RrBQrnx1sQj2Z7FaXgLbWvpVdd');
  const m = r.net.net.get(r.userSide!)!;
  assert.equal(m.get('JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN'), -337601850n);
  assert.equal(m.get('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'), 105030197n);
});

test('shared-accounts hop: user side by outside-use rule, trader by transaction-level deltas', () => {
  const tx = fixtureTx('jupiter_shared_accounts_hop');
  const r = resolveTrader(tx, '1.4');
  assert.equal(r.userSide, 'ARu4n5mFdZogZAravu7CcizaojWnS6oqka37gdLT5SZn');
  assert.ok(!tx.signers.has(r.userSide!));
  assert.equal(r.trader, 'CpTescFMHdEzHPgjt7pTKxapBfEkd2qtuyY2rc6Nrioe');
});

test('ephemeral wSOL hop resolves through InitializeAccount3 ownership', () => {
  const r = resolveTrader(fixtureTx('ephemeral_wsol_tier2_swap'), '0.4');
  assert.equal(r.trader, '97itvmxTihn33ZJg3adr5hpgCmjdEU92oARk59yNYzv');
  assert.equal(r.net.net.get(r.userSide!)!.get('So11111111111111111111111111111111111111112'), -923667285n);
});

test('instruction without transfers: no parties, no trader', () => {
  const r = resolveTrader(fixtureTx('propamm_oracle_update_no_transfers'), '0');
  assert.equal(r.transfers.length, 0);
  assert.deepEqual(r.parties, []);
  assert.equal(r.userSide, null);
});

test('transfers with unknown mint are excluded from netting without throwing', () => {
  const trs: Transfer[] = [
    { path: '1.1', source: 'a', destination: 'b', mint: null, amount: 5n, sourceOwner: 'X', destOwner: 'Y' },
    { path: '1.2', source: 'c', destination: 'd', mint: 'M', amount: 7n, sourceOwner: 'Y', destOwner: 'X' },
  ];
  const n = netTransfers(trs);
  assert.deepEqual(swapParties(n), []);
  assert.equal(n.net.get('X')!.get('M'), 7n);
});
```

- [ ] **Step 2: Run to verify failure**

Run: `npm test`
Expected: FAIL, `../src/trader.ts` not found.

- [ ] **Step 3: Implement `src/trader.ts`**

```ts
import type { Transfer, TxModel } from './tx.ts';
import { isUnder, transfersUnder } from './tx.ts';

export type OwnerNet = Map<string, Map<string, bigint>>;
export interface NetResult { net: OwnerNet; grossSent: OwnerNet; grossRecv: OwnerNet }

function add(m: OwnerNet, owner: string, mint: string, v: bigint): void {
  let o = m.get(owner);
  if (!o) { o = new Map(); m.set(owner, o); }
  o.set(mint, (o.get(mint) ?? 0n) + v);
}

export function netTransfers(trs: Transfer[]): NetResult {
  const r: NetResult = { net: new Map(), grossSent: new Map(), grossRecv: new Map() };
  for (const t of trs) {
    if (t.mint === null) continue;
    if (t.sourceOwner) { add(r.net, t.sourceOwner, t.mint, -t.amount); add(r.grossSent, t.sourceOwner, t.mint, t.amount); }
    if (t.destOwner) { add(r.net, t.destOwner, t.mint, t.amount); add(r.grossRecv, t.destOwner, t.mint, t.amount); }
  }
  return r;
}

export function swapParties(n: NetResult): string[] {
  const out: string[] = [];
  for (const [owner, mints] of n.net) {
    let neg = 0, pos = 0;
    for (const v of mints.values()) { if (v < 0n) neg++; else if (v > 0n) pos++; }
    if (neg === 1 && pos === 1) out.push(owner);
  }
  return out;
}

export interface TraderResult { transfers: Transfer[]; net: NetResult; parties: string[]; userSide: string | null; trader: string | null }

export function resolveTrader(tx: TxModel, path: string): TraderResult {
  const transfers = transfersUnder(tx, path);
  const net = netTransfers(transfers);
  const parties = swapParties(net);
  let userSide: string | null = null;
  const signing = parties.filter((p) => tx.signers.has(p));
  if (signing.length === 1) {
    userSide = signing[0]!;
  } else if (signing.length === 0 && parties.length >= 1) {
    const outside = new Set<string>();
    for (const t of tx.transfers) if (!isUnder(t.path, path)) { outside.add(t.source); outside.add(t.destination); }
    const cands = parties.filter((p) => transfers.some((t) => (t.sourceOwner === p && outside.has(t.source)) || (t.destOwner === p && outside.has(t.destination))));
    if (cands.length === 1) userSide = cands[0]!;
  }
  let trader: string | null = userSide !== null && tx.signers.has(userSide) ? userSide : null;
  if (trader === null) {
    const withDeltas = [...tx.signers].filter((s) => { for (const [k, v] of tx.txDeltas) if (v !== 0n && k.startsWith(s + '|')) return true; return false; });
    if (withDeltas.length === 1) trader = withDeltas[0]!;
  }
  return { transfers, net, parties, userSide, trader };
}
```

- [ ] **Step 4: Run tests**

Run: `npm test && npm run check`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/trader.ts tests/trader.test.ts
git commit -m "Transfer netting, swap parties and the trader rule, verified on real hops

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 6: Table schemas, Raydium v4 ray_log, tier 1 event decoder

**Files:**
- Create: `src/tables.ts`, `src/decoders/raydiumV4.ts`, `src/decoders/context.ts`, `src/decoders/events.ts`
- Modify: `tests/helpers.ts` (return a `DecodeContext`)
- Test: `tests/raydiumV4.test.ts`, `tests/events.test.ts`

**Interfaces:**
- Consumes: Tasks 2–5.
- Produces:
  - `src/decoders/context.ts`: `interface DecodeContext { registry: Registry; idls: Map<string, LoadedIdl> /* by program id */; idlsByLabel: Map<string, LoadedIdl> }`, `buildDecodeContext(registry): DecodeContext` (loads every tier 1 IDL path from the registry).
  - `src/tables.ts`: `ENVELOPE: Column[]`, `SWAP_COLUMNS`, `INTENT_COLUMNS`, `DLMM_POSITION_COLUMNS`, `BLOCKS_COLUMNS`, `FAILURE_COLUMNS`, `RAYDIUM_V4_COLUMNS: Record<string, Column[]>`, `eventTable(label, event): string`, `schemaFor(table, ctx): Column[]`, `tsString(blockTime: number|null): string|null` (`YYYY-MM-DD HH:MM:SS` UTC).
  - `src/decoders/raydiumV4.ts`: `RAYDIUM_V4`, `decodeRayLog(payload): { variant: string; fields: Row }`, `ammAccountIndex(ixData): number`.
  - `src/decoders/events.ts`: `EVENT_CPI_PREFIX = 'e445a52e51cb9a1d'`, `envelope(tx, path, ordinal, trader): Row`, `failureRow(tx, path, program, reason, payload: Uint8Array|null, logLines: string[], transfers: Transfer[]|null): Row`, `interface DecodeOutput { rows: Map<string, Row[]>; failures: Row[] }`, `pushRow(out, table, row)`, `decodeTier1(tx, ctx, parsed: { events: LogEvent[]; truncated: boolean }): DecodeOutput`.

- [ ] **Step 1: Write `src/decoders/context.ts` and extend `tests/helpers.ts`**

`src/decoders/context.ts`:
```ts
import type { Registry } from '../registry.ts';
import { loadIdl, type LoadedIdl } from '../idl.ts';

export interface DecodeContext { registry: Registry; idls: Map<string, LoadedIdl>; idlsByLabel: Map<string, LoadedIdl> }

export function buildDecodeContext(registry: Registry): DecodeContext {
  const idls = new Map<string, LoadedIdl>();
  const idlsByLabel = new Map<string, LoadedIdl>();
  for (const v of registry.tier1.values()) {
    if (!v.idl) continue;
    const idl = loadIdl(v.label, v.idl);
    idls.set(v.program_id, idl);
    idlsByLabel.set(v.label, idl);
  }
  return { registry, idls, idlsByLabel };
}
```

Replace `buildContext` in `tests/helpers.ts` with:
```ts
import { buildDecodeContext, type DecodeContext } from '../src/decoders/context.ts';
let cached: DecodeContext | null = null;
export function buildContext(): DecodeContext {
  if (!cached) cached = buildDecodeContext(loadRegistry('config/venues.json', 'config/routers.json'));
  return cached;
}
```
(remove the old `loadIdl`/`LoadedIdl` imports if unused).

- [ ] **Step 2: Write the failing tests**

`tests/raydiumV4.test.ts`:
```ts
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
```

`tests/events.test.ts`:
```ts
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { decodeTier1 } from '../src/decoders/events.ts';
import { parseLogs } from '../src/logs.ts';
import { instructionAt } from '../src/tx.ts';
import { buildContext, fixtureTx } from './helpers.ts';

function run(name: string) { const tx = fixtureTx(name); return { tx, out: decodeTier1(tx, buildContext(), parseLogs(tx.logs)) }; }

test('DLMM Swap2Evt via self-CPI: envelope + verbatim fields', () => {
  const { out } = run('jupiter_multihop_dlmm_swap2');
  const rows = out.rows.get('events/meteora_dlmm/Swap2Evt')!;
  assert.equal(rows.length, 1);
  const r = rows[0]!;
  assert.equal(r.signature, '288dkjsD9qvbWaCArFypoRfjfB81a2kbd8qRwx1HJZWhwUVbt7w5LFLqnZvrtNSMs1kMHKp28dgGAs59Sun9dU3F');
  assert.equal(r.slot, 457320276);
  assert.equal(r.block_time, '2026-10-15 23:26:23');
  assert.equal(r.tx_index, 126);
  assert.equal(r.ix_path, '3.1');
  assert.equal(r.event_ordinal, 0);
  assert.equal(r.outer_program, 'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4');
  assert.equal(r.trader, '5op7ba3vhSGZgewfa4TGSMZU7wnHkVGmLFYunzzuQwxp');
  assert.equal(r.lb_pair, 'zxTpi4BtaWX3mgdAPoezkMD1hxx8CdeCfrqXMWvSCLX');
  assert.equal(r.amount_in, '104023597531');
  assert.equal(r.amount_out, '110065105');
  assert.equal(r.fee_bps, '2145563');
  assert.equal(r.end_bin_id, -3428);
  assert.equal(out.rows.get('events/meteora_dlmm/Swap')!.length, 1, 'the legacy Swap event is a second table');
  assert.equal(out.failures.length, 0);
});

test('events emitted on both channels produce one row', () => {
  const { out } = run('pumpswap_buy');
  assert.equal(out.rows.get('events/pumpswap/BuyEvent')!.length, 1);
  assert.equal(out.rows.get('events/pumpswap/BuyEvent')![0]!.ix_path, '6');
});

test('log-only events (Whirlpool Traded) decode with outer_program null on a direct call', () => {
  const { out } = run('whirlpool_traded');
  const r = out.rows.get('events/orca_whirlpool/Traded')![0]!;
  assert.equal(r.ix_path, '2');
  assert.equal(r.outer_program, null);
  assert.equal(typeof r.input_amount, 'string');
});

test('two different events from one instruction get ordinal 0 each; same event twice would get 0 and 1', () => {
  const { out } = run('dlmm_claim_fee');
  assert.equal(out.rows.get('events/meteora_dlmm/ClaimFee')![0]!.ix_path, '7');
  assert.equal(out.rows.get('events/meteora_dlmm/ClaimFee2')![0]!.ix_path, '7');
  assert.equal(out.rows.get('events/meteora_dlmm/ClaimFee2')![0]!.event_ordinal, 0);
  assert.ok(out.rows.get('events/meteora_dlmm/PositionClose'));
});

test('Raydium CLMM DecreaseLiquidityEvent gets pool_state from instruction account 3', () => {
  const { tx, out } = run('raydium_clmm_liquidity');
  const r = out.rows.get('events/raydium_clmm/DecreaseLiquidityEvent')![0]!;
  assert.equal(r.ix_path, '4');
  assert.equal(r.pool_state, instructionAt(tx, '4')!.accounts[3]);
});

test('Raydium CLMM SwapEvent with the mismatched IDL lands in failures, not in a table', () => {
  const { out } = run('raydium_clmm_swap');
  assert.equal(out.rows.get('events/raydium_clmm/SwapEvent'), undefined);
  const f = out.failures.find((x) => x.program === 'CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK')!;
  assert.match(String(f.reason), /^decode:/);
  assert.equal(typeof f.payload, 'string');
});

test('Raydium v4 ray_log becomes a row with amm', () => {
  const { out } = run('raydium_v4_raylog');
  const r = out.rows.get('events/raydium_amm_v4/SwapBaseIn')![0]!;
  assert.equal(r.amm, '3sjNoCnkkhWPVXYGDtem8rCciHSGc9jSFZuUAzKbvRVp');
  assert.equal(r.amount_in, '6707650079');
  assert.equal(r.trader, 'MfDuWeqSHEqTFVYZ7LoexgAK9dxk7cy4DFJWjWMGVWa');
});

test('log truncation: tier 1 instructions with no event get a log_truncated failure', () => {
  const { out } = run('log_truncated');
  assert.ok(out.rows.get('events/pumpswap/BuyEvent'));
  const reasons = out.failures.map((f) => f.reason);
  assert.ok(reasons.every((r) => r === 'log_truncated' || String(r).startsWith('decode:') || r === 'unknown_discriminator'));
});
```

- [ ] **Step 3: Run to verify failure**

Run: `npm test`
Expected: FAIL on missing modules.

- [ ] **Step 4: Implement `src/tables.ts`**

```ts
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
  const [, label, event] = m as unknown as [string, string, string];
  if (label === 'raydium_amm_v4') { const c = RAYDIUM_V4_COLUMNS[event]; if (!c) throw new Error(`unknown ray_log variant ${event}`); return c; }
  const idl: LoadedIdl | undefined = ctx.idlsByLabel.get(label);
  if (!idl) throw new Error(`no IDL for ${label}`);
  const ev = [...idl.events.values()].find((e) => e.name === event);
  if (!ev) throw new Error(`no event ${event} in ${label}`);
  const extra: Column[] = label === 'raydium_clmm' && (event === 'IncreaseLiquidityEvent' || event === 'DecreaseLiquidityEvent') ? [{ name: 'pool_state', type: 'VARCHAR' }] : [];
  return [...ENVELOPE, ...fieldColumns(ev.fields, idl.types), ...extra];
}
```

- [ ] **Step 5: Implement `src/decoders/raydiumV4.ts`**

```ts
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
```

- [ ] **Step 6: Implement `src/decoders/events.ts`**

```ts
import { decodeStruct, BorshError } from '../borsh.ts';
import { jsonSafe, type Row } from '../rows.ts';
import type { LogEvent } from '../logs.ts';
import { instructionAt, type Transfer, type TxModel } from '../tx.ts';
import { resolveTrader } from '../trader.ts';
import { eventTable, tsString } from '../tables.ts';
import type { DecodeContext } from './context.ts';
import { RAYDIUM_V4, decodeRayLog, ammAccountIndex } from './raydiumV4.ts';

export const EVENT_CPI_PREFIX = 'e445a52e51cb9a1d';

export interface DecodeOutput { rows: Map<string, Row[]>; failures: Row[] }

export function pushRow(out: DecodeOutput, table: string, row: Row): void {
  let arr = out.rows.get(table);
  if (!arr) { arr = []; out.rows.set(table, arr); }
  arr.push(row);
}

export function outerProgramFor(tx: TxModel, path: string, venue: string): string | null {
  const outer = tx.outerPrograms[Number(path.split('.')[0])]!;
  return outer === venue ? null : outer;
}

export function envelope(tx: TxModel, path: string, ordinal: number, outerProgram: string | null, trader: string | null): Row {
  return { signature: tx.signature, slot: tx.slot, block_time: tsString(tx.blockTime), tx_index: tx.txIndex, ix_path: path, event_ordinal: ordinal, outer_program: outerProgram, trader };
}

export function failureRow(tx: TxModel, path: string, program: string, reason: string, payload: Uint8Array | null, logLines: string[], transfers: Transfer[] | null): Row {
  return {
    signature: tx.signature, slot: tx.slot, block_time: tsString(tx.blockTime), tx_index: tx.txIndex, ix_path: path, program, reason,
    payload: payload ? Buffer.from(payload).toString('base64') : null, log_lines: logLines,
    transfers: transfers ? transfers.map((t) => ({ source: t.source, destination: t.destination, owner_source: t.sourceOwner, owner_destination: t.destOwner, mint: t.mint, amount: t.amount.toString() })) : null,
  };
}

interface Candidate { programId: string; path: string; disc: string; payload: Uint8Array; source: 'cpi' | 'log' }

function collect(tx: TxModel, ctx: DecodeContext, parsed: { events: LogEvent[] }): Candidate[] {
  const seen = new Set<string>();
  const out: Candidate[] = [];
  const add = (c: Candidate) => { const k = `${c.path}|${c.disc}|${Buffer.from(c.payload).toString('hex')}`; if (seen.has(k)) return; seen.add(k); out.push(c); };
  for (const ix of tx.instructions) {
    if (!ctx.idls.has(ix.programId) || ix.data.length < 16) continue;
    if (Buffer.from(ix.data.subarray(0, 8)).toString('hex') !== EVENT_CPI_PREFIX) continue;
    const parent = ix.path.includes('.') ? ix.path.slice(0, ix.path.lastIndexOf('.')) : ix.path;
    add({ programId: ix.programId, path: parent, disc: Buffer.from(ix.data.subarray(8, 16)).toString('hex'), payload: ix.data.subarray(16), source: 'cpi' });
  }
  for (const e of parsed.events) {
    if (e.kind !== 'data' || !ctx.idls.has(e.programId) || e.payload.length < 8) continue;
    add({ programId: e.programId, path: e.path, disc: Buffer.from(e.payload.subarray(0, 8)).toString('hex'), payload: e.payload.subarray(8), source: 'log' });
  }
  return out;
}

export function decodeTier1(tx: TxModel, ctx: DecodeContext, parsed: { events: LogEvent[]; truncated: boolean }): DecodeOutput {
  const out: DecodeOutput = { rows: new Map(), failures: [] };
  const ordinals = new Map<string, number>();
  const traders = new Map<string, string | null>();
  const traderFor = (path: string) => { if (!traders.has(path)) traders.set(path, resolveTrader(tx, path).trader); return traders.get(path)!; };
  const pathsWithEvents = new Set<string>();

  for (const c of collect(tx, ctx, parsed)) {
    const idl = ctx.idls.get(c.programId)!;
    const ev = idl.events.get(c.disc);
    if (!ev) { out.failures.push(failureRow(tx, c.path, c.programId, 'unknown_discriminator', c.payload, [], null)); continue; }
    try {
      const fields = jsonSafe(decodeStruct(ev.fields, c.payload, idl.types)) as Row;
      const key = `${c.path}|${ev.name}`;
      const ordinal = ordinals.get(key) ?? 0;
      ordinals.set(key, ordinal + 1);
      const row: Row = { ...envelope(tx, c.path, ordinal, outerProgramFor(tx, c.path, c.programId), traderFor(c.path)), ...fields };
      if (idl.label === 'raydium_clmm' && (ev.name === 'IncreaseLiquidityEvent' || ev.name === 'DecreaseLiquidityEvent')) {
        const ix = instructionAt(tx, c.path);
        row.pool_state = ix?.accounts[ev.name === 'IncreaseLiquidityEvent' ? 2 : 3] ?? null;
      }
      pushRow(out, eventTable(idl.label, ev.name), row);
      pathsWithEvents.add(c.path);
    } catch (e) {
      const msg = e instanceof BorshError ? e.message : String(e);
      out.failures.push(failureRow(tx, c.path, c.programId, `decode:${msg}`.slice(0, 120), c.payload, [], null));
    }
  }

  for (const e of parsed.events) {
    if (e.kind !== 'raylog' || e.programId !== RAYDIUM_V4) continue;
    try {
      const { variant, fields } = decodeRayLog(e.payload);
      const ix = instructionAt(tx, e.path);
      const row: Row = { ...envelope(tx, e.path, 0, outerProgramFor(tx, e.path, RAYDIUM_V4), traderFor(e.path)), amm: ix ? ix.accounts[ammAccountIndex(ix.data)] ?? null : null, ...(jsonSafe(fields) as Row) };
      pushRow(out, eventTable('raydium_amm_v4', variant), row);
      pathsWithEvents.add(e.path);
    } catch (err) {
      out.failures.push(failureRow(tx, e.path, RAYDIUM_V4, `decode:${err instanceof Error ? err.message : String(err)}`.slice(0, 120), e.payload, [], null));
    }
  }

  if (parsed.truncated) {
    for (const ix of tx.instructions) {
      if (!ctx.registry.tier1.has(ix.programId) || pathsWithEvents.has(ix.path)) continue;
      if (ix.data.length >= 8 && Buffer.from(ix.data.subarray(0, 8)).toString('hex') === EVENT_CPI_PREFIX) continue;
      out.failures.push(failureRow(tx, ix.path, ix.programId, 'log_truncated', ix.data, [], null));
    }
  }
  return out;
}
```

- [ ] **Step 7: Run tests and type check**

Run: `npm test && npm run check`
Expected: all pass. If `events/meteora_dlmm/Swap` is absent, check that the legacy `Swap` discriminator in the IDL matches; the fixture emits both.

- [ ] **Step 8: Commit**

```bash
git add src/tables.ts src/decoders tests/helpers.ts tests/raydiumV4.test.ts tests/events.test.ts
git commit -m "Tier 1 event decoder: CPI and log channels, dedupe, ray_log, pool columns, table schemas

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 7: Tier 2 swaps, intents, DLMM positions

**Files:**
- Create: `src/decoders/tier2.ts`, `src/decoders/intents.ts`, `src/decoders/dlmmPosition.ts`
- Test: `tests/tier2.test.ts`, `tests/intents.test.ts`, `tests/dlmmPosition.test.ts`

**Interfaces:**
- Consumes: Task 6 (`DecodeOutput`, `pushRow`, `envelope`, `failureRow`, `outerProgramFor`), Task 5 (`resolveTrader`, `netTransfers`, `swapParties`), Task 3.
- Produces: `decodeTier2(tx, ctx): DecodeOutput`, `decodeIntents(tx, ctx): DecodeOutput`, `decodeDlmmPositions(tx, ctx): DecodeOutput`, and `interface DecodeStats { [k: string]: number }` returned alongside via `out.stats` (add `stats: Record<string, number>` to `DecodeOutput` in `src/decoders/events.ts`, initialised to `{}` everywhere it is constructed).

- [ ] **Step 1: Write the failing tests**

`tests/tier2.test.ts`:
```ts
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { decodeTier2 } from '../src/decoders/tier2.ts';
import { buildContext, fixtureTx } from './helpers.ts';

test('HumidiFi hop under Jupiter becomes one swap row from the user side', () => {
  const out = decodeTier2(fixtureTx('humidifi_swap'), buildContext());
  const rows = out.rows.get('swaps/humidifi')!;
  assert.equal(rows.length, 1);
  assert.deepEqual(rows[0], {
    signature: '15PAuXvXQeAYS3ZSmGiUwb6ubE6HkjUJRQWKk7TBBKvA3Nok19oThKdxP4rTuRwTczF6GUBr97TuG6BUcVCy5B7',
    slot: 457320309, block_time: '2026-10-15 23:26:30', tx_index: 349, ix_path: '4.5', event_ordinal: 0,
    outer_program: 'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4', trader: '4B8srvotmWddTZuMt4RrBQrnx1sQj2Z7FaXgLbWvpVdd',
    program: '9H6tua7jkLhdm3w8BvgpTn5LZNU7g4ZynDmCiNN3q6Rp',
    mint_in: 'JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN', amount_in: '337601850',
    mint_out: 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', amount_out: '105030197',
  });
  assert.equal(out.failures.length, 0);
});

test('Tessera V hop with ephemeral wSOL account', () => {
  const out = decodeTier2(fixtureTx('ephemeral_wsol_tier2_swap'), buildContext());
  const r = out.rows.get('swaps/tesserav')![0]!;
  assert.equal(r.mint_in, 'So11111111111111111111111111111111111111112');
  assert.equal(r.amount_in, '923667285');
  assert.equal(r.amount_out, '97899986');
  assert.equal(r.trader, '97itvmxTihn33ZJg3adr5hpgCmjdEU92oARk59yNYzv');
});

test('shared-accounts ZeroFi hop: user side is not the signer, trader is', () => {
  const out = decodeTier2(fixtureTx('jupiter_shared_accounts_hop'), buildContext());
  const r = out.rows.get('swaps/zerofi')![0]!;
  assert.equal(r.ix_path, '1.4');
  assert.equal(r.trader, 'CpTescFMHdEzHPgjt7pTKxapBfEkd2qtuyY2rc6Nrioe');
});

test('prop AMM instruction with no transfers produces nothing, counted in stats', () => {
  const out = decodeTier2(fixtureTx('propamm_oracle_update_no_transfers'), buildContext());
  assert.equal(out.rows.size, 0);
  assert.equal(out.failures.length, 0);
  assert.equal(out.stats.tier2_no_transfers, 1);
});

test('Manifest instruction without a swap-shaped party is skipped silently', () => {
  const out = decodeTier2(fixtureTx('manifest_non_swap'), buildContext());
  assert.equal(out.rows.size, 0);
  assert.equal(out.failures.length, 0);
});
```

`tests/intents.test.ts`:
```ts
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { decodeIntents } from '../src/decoders/intents.ts';
import { buildContext, fixtureTx } from './helpers.ts';

test('DFlow route: fee leg removed, intent amounts are the trader gross, hops counted', () => {
  const out = decodeIntents(fixtureTx('dflow_route_with_fee_leg'), buildContext());
  const rows = out.rows.get('intents')!;
  assert.equal(rows.length, 1);
  const r = rows[0]!;
  assert.equal(r.ix_path, '0');
  assert.equal(r.outer_program, null);
  assert.equal(r.program, 'DF1ow4tspfHX9JwWJsAb9epbkA8hmpSEAtxXy1V27QBH');
  assert.equal(r.trader, '97itvmxTihn33ZJg3adr5hpgCmjdEU92oARk59yNYzv');
  assert.equal(r.kind, 'swap');
  assert.equal(r.mint_in, 'H8WTvo3ZXSYHJAcYqzCforbG8coR1FDAX2kHFfqHBAGS');
  assert.equal(r.amount_in, '84566873727156');
  assert.equal(r.mint_out, 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v');
  assert.equal(r.amount_out, '97899986');
  assert.equal(r.hops, 3);
});

test('direct DLMM swap under Jupiter: one intent for the route', () => {
  const out = decodeIntents(fixtureTx('jupiter_multihop_dlmm_swap2'), buildContext());
  const r = out.rows.get('intents')![0]!;
  assert.equal(r.ix_path, '3');
  assert.equal(r.program, 'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4');
  assert.equal(r.trader, '5op7ba3vhSGZgewfa4TGSMZU7wnHkVGmLFYunzzuQwxp');
  assert.ok(r.kind === 'swap' || r.kind === 'cycle');
});

test('outer instructions with no transfers or no swap-shaped party are skipped silently', () => {
  const a = decodeIntents(fixtureTx('propamm_oracle_update_no_transfers'), buildContext());
  assert.equal(a.rows.size, 0); assert.equal(a.failures.length, 0); assert.equal(a.stats.intent_no_transfers, 1);
  const b = decodeIntents(fixtureTx('manifest_non_swap'), buildContext());
  assert.equal(b.rows.size, 0); assert.equal(b.failures.length, 0);
});
```

`tests/dlmmPosition.test.ts`:
```ts
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { decodeDlmmPositions } from '../src/decoders/dlmmPosition.ts';
import { buildContext, fixtureTx } from './helpers.ts';

test('initialize_position args and named accounts become a row', () => {
  const out = decodeDlmmPositions(fixtureTx('dlmm_position_create'), buildContext());
  const r = out.rows.get('instructions/meteora_dlmm/initialize_position')![0]!;
  assert.equal(r.ix_path, '1');
  assert.equal(r.position, 'BPe1vLZSXvGbM4nn4SPE7PrkaPzb2yW2QJEeoMkVquXg');
  assert.equal(r.lb_pair, 'EQcKUXfKugCghU5kLe8vTsF4EnZZZJ2rPXou2RqzTN11');
  assert.equal(r.lower_bin_id, -469);
  assert.equal(r.width, 70);
  assert.equal(r.fee_owner, null);
  assert.equal(r.lock_release_point, null);
  assert.equal(r.trader, null, 'no transfers in a position creation');
});

test('a transaction without initialize_position yields nothing', () => {
  assert.equal(decodeDlmmPositions(fixtureTx('humidifi_swap'), buildContext()).rows.size, 0);
});
```

- [ ] **Step 2: Run to verify failure**

Run: `npm test`
Expected: FAIL on missing modules.

- [ ] **Step 3: Add `stats` to `DecodeOutput`**

In `src/decoders/events.ts` change the interface and the constructor site:
```ts
export interface DecodeOutput { rows: Map<string, Row[]>; failures: Row[]; stats: Record<string, number> }
export function newOutput(): DecodeOutput { return { rows: new Map(), failures: [], stats: {} }; }
export function bump(out: DecodeOutput, key: string): void { out.stats[key] = (out.stats[key] ?? 0) + 1; }
```
and in `decodeTier1` replace `const out: DecodeOutput = { rows: new Map(), failures: [] };` with `const out = newOutput();`.

- [ ] **Step 4: Implement `src/decoders/tier2.ts`**

```ts
import type { TxModel } from '../tx.ts';
import { resolveTrader } from '../trader.ts';
import type { DecodeContext } from './context.ts';
import { type DecodeOutput, newOutput, bump, pushRow, envelope, failureRow, outerProgramFor } from './events.ts';

export function decodeTier2(tx: TxModel, ctx: DecodeContext): DecodeOutput {
  const out = newOutput();
  for (const ix of tx.instructions) {
    const venue = ctx.registry.tier2.get(ix.programId);
    if (!venue) continue;
    const r = resolveTrader(tx, ix.path);
    if (r.transfers.length === 0) { bump(out, 'tier2_no_transfers'); continue; }
    if (r.parties.length === 0) { bump(out, 'tier2_non_swap'); continue; }
    if (r.userSide === null) { out.failures.push(failureRow(tx, ix.path, ix.programId, 'not_a_swap', null, [], r.transfers)); continue; }
    const m = r.net.net.get(r.userSide)!;
    let mintIn = '', mintOut = '', amountIn = 0n, amountOut = 0n;
    for (const [mint, v] of m) { if (v < 0n) { mintIn = mint; amountIn = -v; } else if (v > 0n) { mintOut = mint; amountOut = v; } }
    pushRow(out, `swaps/${venue.label}`, {
      ...envelope(tx, ix.path, 0, outerProgramFor(tx, ix.path, ix.programId), r.trader),
      program: ix.programId, mint_in: mintIn, amount_in: amountIn.toString(), mint_out: mintOut, amount_out: amountOut.toString(),
    });
  }
  return out;
}
```

- [ ] **Step 5: Implement `src/decoders/intents.ts`**

```ts
import { isUnder, transfersUnder, type TxModel } from '../tx.ts';
import { netTransfers, resolveTrader } from '../trader.ts';
import type { DecodeContext } from './context.ts';
import { type DecodeOutput, newOutput, bump, pushRow, envelope, failureRow } from './events.ts';

export function decodeIntents(tx: TxModel, ctx: DecodeContext): DecodeOutput {
  const out = newOutput();
  const outers = [...new Set(tx.instructions.filter((i) => ctx.registry.isVenue(i.programId)).map((i) => i.outerIndex))].sort((a, b) => a - b);
  for (const i of outers) {
    const prefix = String(i);
    const r = resolveTrader(tx, prefix);
    if (r.transfers.length === 0) { bump(out, 'intent_no_transfers'); continue; }
    if (r.parties.length === 0) { bump(out, 'intent_no_swap_party'); continue; }
    const hops = tx.instructions.filter((ix) => ctx.registry.isVenue(ix.programId) && isUnder(ix.path, prefix));
    const hopParties = new Set<string>();
    const hopMints = new Set<string>();
    for (const h of hops) for (const t of transfersUnder(tx, h.path)) { if (t.sourceOwner) hopParties.add(t.sourceOwner); if (t.destOwner) hopParties.add(t.destOwner); if (t.mint) hopMints.add(t.mint); }
    const senders = new Set(r.transfers.map((t) => t.sourceOwner));
    // fee legs: to an owner that only receives in this instruction and is not a party to any hop
    const core = r.transfers.filter((t) => !(t.destOwner !== null && !senders.has(t.destOwner) && !hopParties.has(t.destOwner)));
    const n = netTransfers(core);
    const who = r.trader ?? r.userSide;
    const m = who ? n.net.get(who) : undefined;
    if (!who || !m) { out.failures.push(failureRow(tx, prefix, tx.outerPrograms[i]!, 'not_an_intent', null, [], r.transfers)); continue; }
    const neg: string[] = [], pos: string[] = [];
    for (const [mint, v] of m) { if (!hopMints.has(mint) || v === 0n) continue; (v < 0n ? neg : pos).push(mint); }
    let kind: string, mintIn: string, mintOut: string;
    if (neg.length === 1 && pos.length === 1) { kind = 'swap'; mintIn = neg[0]!; mintOut = pos[0]!; }
    else if (neg.length + pos.length <= 1) {
      const sent = n.grossSent.get(who);
      if (!sent || sent.size === 0) { out.failures.push(failureRow(tx, prefix, tx.outerPrograms[i]!, 'not_an_intent', null, [], r.transfers)); continue; }
      kind = 'cycle'; mintIn = [...sent.entries()].sort((a, b) => (a[1] < b[1] ? 1 : -1))[0]![0]; mintOut = mintIn;
    } else { out.failures.push(failureRow(tx, prefix, tx.outerPrograms[i]!, 'not_an_intent', null, [], r.transfers)); continue; }
    const amountIn = n.grossSent.get(who)?.get(mintIn) ?? 0n;
    const amountOut = n.grossRecv.get(who)?.get(mintOut) ?? 0n;
    pushRow(out, 'intents', {
      ...envelope(tx, prefix, 0, null, r.trader),
      program: tx.outerPrograms[i]!, mint_in: mintIn, amount_in: amountIn.toString(), mint_out: mintOut, amount_out: amountOut.toString(), hops: hops.length, kind,
    });
  }
  return out;
}
```

- [ ] **Step 6: Implement `src/decoders/dlmmPosition.ts`**

```ts
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
```

- [ ] **Step 7: Run tests and type check**

Run: `npm test && npm run check`
Expected: all pass. The `swaps/<label>` table name uses the registry label (`tesserav`, `zerofi`, `humidifi` as generated by `build-venues.ts`); adjust the test's expected labels to the generated ones if the slug differs, never the other way round.

- [ ] **Step 8: Commit**

```bash
git add src/decoders tests/tier2.test.ts tests/intents.test.ts tests/dlmmPosition.test.ts
git commit -m "Tier 2 swaps, intents with fee-leg removal, DLMM initialize_position rows

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 8: Block decoder

**Files:**
- Create: `src/decoders/block.ts`
- Test: `tests/block.test.ts`

**Interfaces:**
- Consumes: Tasks 3–7.
- Produces: `interface DecodedBlock { slot: number; tables: Map<string, Row[]>; stats: Record<string, number> }`, `decodeBlock(block: RpcBlock | null, slot: number, ctx: DecodeContext): DecodedBlock` (null block = skipped slot; `tables` always includes `blocks` with one row), `mergeTables(into: Map<string, Row[]>, from: Map<string, Row[]>)`.

- [ ] **Step 1: Write the failing test**

`tests/block.test.ts`:
```ts
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { decodeBlock } from '../src/decoders/block.ts';
import { buildContext, loadFixture } from './helpers.ts';
import type { RpcBlock } from '../src/tx.ts';

function blockOf(names: string[]): RpcBlock {
  const fx = names.map(loadFixture);
  return { blockTime: fx[0]!.blockTime, blockHeight: 1, parentSlot: 457320275, transactions: fx.map((f) => f.tx) };
}

test('a synthetic block of fixtures yields the expected tables and a blocks row', () => {
  const b = blockOf(['vote', 'failed_venue_tx', 'jupiter_multihop_dlmm_swap2', 'humidifi_swap', 'dflow_route_with_fee_leg', 'propamm_oracle_update_no_transfers', 'raydium_clmm_swap']);
  const d = decodeBlock(b, 457320276, buildContext());
  assert.deepEqual(d.tables.get('blocks'), [{ slot: 457320276, block_time: '2026-10-15 23:26:23', status: 'ok' }]);
  assert.equal(d.tables.get('events/meteora_dlmm/Swap2Evt')!.length, 1);
  assert.equal(d.tables.get('swaps/humidifi')!.length, 1);
  assert.ok(d.tables.get('intents')!.length >= 2);
  assert.ok(d.tables.get('decode_failures')!.some((f) => String(f.reason).startsWith('decode:')));
  assert.equal(d.stats.tx_total, 7);
  assert.equal(d.stats.tx_vote, 1);
  assert.equal(d.stats.tx_failed, 1);
  assert.equal(d.stats.tx_venue, 5);
  // tx_index inside a synthetic block is the position in this block, not the fixture's original
  assert.equal(d.tables.get('events/meteora_dlmm/Swap2Evt')![0]!.tx_index, 2);
});

test('null block is a skipped slot', () => {
  const d = decodeBlock(null, 5, buildContext());
  assert.deepEqual(d.tables.get('blocks'), [{ slot: 5, block_time: null, status: 'skipped' }]);
  assert.equal(d.tables.size, 1);
});

test('a transaction with null meta is counted and skipped', () => {
  const b = blockOf(['humidifi_swap']);
  b.transactions[0] = { ...b.transactions[0]!, meta: null };
  const d = decodeBlock(b, 1, buildContext());
  assert.equal(d.stats.tx_null_meta, 1);
  assert.equal(d.tables.size, 1);
});
```

- [ ] **Step 2: Run to verify failure**

Run: `npm test`
Expected: FAIL, module not found.

- [ ] **Step 3: Implement `src/decoders/block.ts`**

```ts
import { buildTx, type RpcBlock } from '../tx.ts';
import { parseLogs } from '../logs.ts';
import type { Row } from '../rows.ts';
import { tsString } from '../tables.ts';
import type { DecodeContext } from './context.ts';
import { decodeTier1, type DecodeOutput } from './events.ts';
import { decodeTier2 } from './tier2.ts';
import { decodeIntents } from './intents.ts';
import { decodeDlmmPositions } from './dlmmPosition.ts';

export interface DecodedBlock { slot: number; tables: Map<string, Row[]>; stats: Record<string, number> }

export function mergeTables(into: Map<string, Row[]>, from: Map<string, Row[]>): void {
  for (const [t, rows] of from) { const arr = into.get(t); if (arr) arr.push(...rows); else into.set(t, [...rows]); }
}
function mergeStats(into: Record<string, number>, from: Record<string, number>): void {
  for (const [k, v] of Object.entries(from)) into[k] = (into[k] ?? 0) + v;
}
function absorb(d: DecodedBlock, o: DecodeOutput): void {
  mergeTables(d.tables, o.rows);
  if (o.failures.length) { const f = d.tables.get('decode_failures'); if (f) f.push(...o.failures); else d.tables.set('decode_failures', [...o.failures]); }
  mergeStats(d.stats, o.stats);
}

export function decodeBlock(block: RpcBlock | null, slot: number, ctx: DecodeContext): DecodedBlock {
  const d: DecodedBlock = { slot, tables: new Map(), stats: {} };
  if (!block) { d.tables.set('blocks', [{ slot, block_time: null, status: 'skipped' }]); return d; }
  d.tables.set('blocks', [{ slot, block_time: tsString(block.blockTime), status: 'ok' }]);
  const bump = (k: string) => { d.stats[k] = (d.stats[k] ?? 0) + 1; };
  block.transactions.forEach((raw, txIndex) => {
    bump('tx_total');
    const tx = buildTx(raw, slot, block.blockTime, txIndex);
    if (!tx) { bump('tx_null_meta'); return; }
    if (tx.isVote) { bump('tx_vote'); return; }
    if (!tx.success) { bump('tx_failed'); return; }
    let touches = false;
    for (const p of tx.programs) if (ctx.registry.isVenue(p)) { touches = true; break; }
    if (!touches) return;
    bump('tx_venue');
    const parsed = parseLogs(tx.logs);
    absorb(d, decodeTier1(tx, ctx, parsed));
    absorb(d, decodeTier2(tx, ctx));
    absorb(d, decodeIntents(tx, ctx));
    absorb(d, decodeDlmmPositions(tx, ctx));
  });
  return d;
}
```

- [ ] **Step 4: Run tests and type check**

Run: `npm test && npm run check`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/decoders/block.ts tests/block.test.ts
git commit -m "Block decoder: transaction filtering, all decoders, blocks row and stats

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 9: Parquet range writer and checkpoint

**Files:**
- Create: `src/duck.ts`, `src/writer.ts`, `src/checkpoint.ts`
- Test: `tests/writer.test.ts`, `tests/checkpoint.test.ts`

**Interfaces:**
- Consumes: `Column`, `schemaFor`, `DecodeContext`, `Row`.
- Produces:
  - `src/duck.ts`: `openDuck(path: string | ':memory:'): Promise<DuckDBConnection>` (wraps `DuckDBInstance.create` + `connect`), `q(s: string): string` (single-quote escape), `rowsJson(conn, sql): Promise<Record<string, unknown>[]>`.
  - `src/writer.ts`: `interface FileRecord { table; path /* relative to dataRoot */; rows; sha256 }`, `class RangeWriter { constructor(opts: { dataRoot: string; conn: DuckDBConnection; decoderVersion: string; schemaFor: (table: string) => Column[]; idlSha256?: string }); writeRange(lane, lo, hi, tables: Map<string, Row[]>): Promise<FileRecord[]>; static cleanupTemp(dataRoot): Promise<number> }`.
  - `src/checkpoint.ts`: `interface LaneState { lane; startSlot; targetSlot: number | null; nextSlot }`, `interface RangeRecord { lane; loSlot; hiSlot; status; decoderVersion; files: FileRecord[]; committedAt: string }`, `class Checkpoint { static open(path): Promise<Checkpoint>; getState(lane): Promise<LaneState | null>; initState(s: LaneState): Promise<void>; commitRange(lane, lo, hi, files, decoderVersion, nextSlot): Promise<void>; listRanges(lane): Promise<RangeRecord[]>; close(): Promise<void> }`.

- [ ] **Step 1: Write the failing tests**

`tests/writer.test.ts`:
```ts
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, existsSync, readdirSync, writeFileSync, mkdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { openDuck, rowsJson } from '../src/duck.ts';
import { RangeWriter } from '../src/writer.ts';
import { schemaFor } from '../src/tables.ts';
import { decodeBlock } from '../src/decoders/block.ts';
import { buildContext, loadFixture } from './helpers.ts';
import type { RpcBlock } from '../src/tx.ts';

function block(names: string[], blockTime: number | null): RpcBlock {
  return { blockTime, blockHeight: 1, parentSlot: 0, transactions: names.map((n) => loadFixture(n).tx) };
}

test('writes typed ZSTD parquet per table and date, sorted, with KV metadata', async () => {
  const root = mkdtempSync(join(tmpdir(), 'fiber-'));
  const ctx = buildContext();
  const conn = await openDuck(':memory:');
  const w = new RangeWriter({ dataRoot: root, conn, decoderVersion: '1.0.0', schemaFor: (t) => schemaFor(t, ctx) });
  const d1 = decodeBlock(block(['jupiter_multihop_dlmm_swap2', 'humidifi_swap'], 1792085183), 100, ctx);
  const d2 = decodeBlock(block(['whirlpool_traded'], 1792085183 + 86400), 101, ctx);  // next UTC day
  const tables = new Map(d1.tables);
  for (const [t, rows] of d2.tables) { const a = tables.get(t); if (a) a.push(...rows); else tables.set(t, rows); }
  const files = await w.writeRange('tail', 100, 101, tables);
  const swap = files.find((f) => f.table === 'events/meteora_dlmm/Swap2Evt')!;
  assert.equal(swap.path, 'events/meteora_dlmm/Swap2Evt/date=2026-10-15/100-101.parquet');
  assert.equal(swap.rows, 1);
  assert.equal(swap.sha256.length, 64);
  assert.ok(files.some((f) => f.path === 'blocks/date=2026-10-15/100-101.parquet'));
  assert.ok(files.some((f) => f.path === 'blocks/date=2026-10-16/100-101.parquet'));
  assert.ok(files.some((f) => f.path === 'events/orca_whirlpool/Traded/date=2026-10-16/100-101.parquet'));
  const rows = await rowsJson(conn, `SELECT slot, amount_in, fee_bps, swap_for_y, typeof(amount_in) AS t FROM read_parquet('${join(root, swap.path)}')`);
  assert.deepEqual(rows, [{ slot: '100', amount_in: '104023597531', fee_bps: '2145563', swap_for_y: true, t: 'UBIGINT' }]);
  const kv = await rowsJson(conn, `SELECT key, value FROM parquet_kv_metadata('${join(root, swap.path)}')`);
  assert.deepEqual(Object.fromEntries(kv.map((r) => [r.key, r.value])), { decoder_version: '1.0.0', idl_sha256: '', lane: 'tail', lo_slot: '100', hi_slot: '101' });
  assert.ok(!existsSync(join(root, '.staging')) || readdirSync(join(root, '.staging')).length === 0, 'staging cleaned');
  assert.ok(!readdirSync(join(root, 'events/meteora_dlmm/Swap2Evt/date=2026-10-15')).some((f) => f.endsWith('.tmp')));
});

test('null block_time rows go into the date of the nearest timed block in the range', async () => {
  const root = mkdtempSync(join(tmpdir(), 'fiber-'));
  const ctx = buildContext();
  const w = new RangeWriter({ dataRoot: root, conn: await openDuck(':memory:'), decoderVersion: '1.0.0', schemaFor: (t) => schemaFor(t, ctx) });
  const tables = new Map<string, Record<string, unknown>[]>([['blocks', [{ slot: 1, block_time: null, status: 'skipped' }, { slot: 2, block_time: '2026-10-15 00:00:00', status: 'ok' }]]]);
  const files = await w.writeRange('tail', 1, 2, tables);
  assert.deepEqual(files.map((f) => [f.path, f.rows]), [['blocks/date=2026-10-15/1-2.parquet', 2]]);
});

test('cleanupTemp removes leftover .tmp and staging files', async () => {
  const root = mkdtempSync(join(tmpdir(), 'fiber-'));
  mkdirSync(join(root, 'blocks/date=2026-10-15'), { recursive: true });
  mkdirSync(join(root, '.staging'), { recursive: true });
  writeFileSync(join(root, 'blocks/date=2026-10-15/5-6.parquet.tmp'), 'x');
  writeFileSync(join(root, '.staging/tail-5-6-blocks-2026-10-15.jsonl'), 'x');
  assert.equal(await RangeWriter.cleanupTemp(root), 2);
  assert.equal(readdirSync(join(root, 'blocks/date=2026-10-15')).length, 0);
});
```

`tests/checkpoint.test.ts`:
```ts
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Checkpoint } from '../src/checkpoint.ts';

test('state init, range commit and resume', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'fiber-ckpt-'));
  const path = join(dir, 'tail.duckdb');
  let c = await Checkpoint.open(path);
  assert.equal(await c.getState('tail'), null);
  await c.initState({ lane: 'tail', startSlot: 100, targetSlot: null, nextSlot: 100 });
  await c.commitRange('tail', 100, 104, [{ table: 'blocks', path: 'blocks/date=2026-10-15/100-104.parquet', rows: 5, sha256: 'ab' }], '1.0.0', 105);
  await c.close();
  c = await Checkpoint.open(path);
  assert.deepEqual(await c.getState('tail'), { lane: 'tail', startSlot: 100, targetSlot: null, nextSlot: 105 });
  const ranges = await c.listRanges('tail');
  assert.equal(ranges.length, 1);
  assert.equal(ranges[0]!.loSlot, 100);
  assert.equal(ranges[0]!.hiSlot, 104);
  assert.deepEqual(ranges[0]!.files[0]!.path, 'blocks/date=2026-10-15/100-104.parquet');
  await c.close();
});
```

- [ ] **Step 2: Run to verify failure**

Run: `npm test`
Expected: FAIL, modules not found.

- [ ] **Step 3: Implement `src/duck.ts`**

```ts
import { DuckDBInstance, type DuckDBConnection } from '@duckdb/node-api';
export type { DuckDBConnection };

export async function openDuck(path: string): Promise<DuckDBConnection> {
  const inst = await DuckDBInstance.create(path);
  return inst.connect();
}

export function q(s: string): string { return `'${s.replace(/'/g, "''")}'`; }

export async function rowsJson(conn: DuckDBConnection, sql: string): Promise<Record<string, unknown>[]> {
  const r = await conn.runAndReadAll(sql);
  return r.getRowObjectsJson() as Record<string, unknown>[];
}
```

- [ ] **Step 4: Implement `src/writer.ts`**

```ts
import { createHash } from 'node:crypto';
import { mkdir, open, readdir, rename, rm, writeFile } from 'node:fs/promises';
import { createReadStream } from 'node:fs';
import { join, dirname } from 'node:path';
import type { Column } from './idl.ts';
import type { Row } from './rows.ts';
import { q, type DuckDBConnection } from './duck.ts';

export interface FileRecord { table: string; path: string; rows: number; sha256: string }

export interface RangeWriterOptions { dataRoot: string; conn: DuckDBConnection; decoderVersion: string; schemaFor: (table: string) => Column[]; idlSha256?: string }

async function sha256File(path: string): Promise<string> {
  const h = createHash('sha256');
  for await (const chunk of createReadStream(path)) h.update(chunk as Buffer);
  return h.digest('hex');
}
async function fsyncFile(path: string): Promise<void> { const fh = await open(path, 'r'); try { await fh.sync(); } finally { await fh.close(); } }

export class RangeWriter {
  readonly opts: RangeWriterOptions;
  constructor(opts: RangeWriterOptions) { this.opts = opts; }

  async writeRange(lane: string, lo: number, hi: number, tables: Map<string, Row[]>): Promise<FileRecord[]> {
    const { dataRoot, conn, decoderVersion, schemaFor, idlSha256 = '' } = this.opts;
    const staging = join(dataRoot, '.staging');
    await mkdir(staging, { recursive: true });
    // Fallback date for rows without block_time: the first dated row in the range (blocks rows carry the range's dates).
    let fallbackDate: string | null = null;
    for (const rows of tables.values()) for (const r of rows) { const bt = r.block_time; if (typeof bt === 'string') { fallbackDate = bt.slice(0, 10); break; } }
    const written: string[] = [];
    const records: FileRecord[] = [];
    try {
      for (const [table, rows] of [...tables.entries()].sort(([a], [b]) => a.localeCompare(b))) {
        if (rows.length === 0) continue;
        const byDate = new Map<string, Row[]>();
        for (const r of rows) {
          const bt = r.block_time;
          const date = typeof bt === 'string' ? bt.slice(0, 10) : fallbackDate ?? 'unknown';
          const arr = byDate.get(date); if (arr) arr.push(r); else byDate.set(date, [r]);
        }
        const cols = schemaFor(table);
        const colList = cols.map((c) => `"${c.name}"`).join(', ');
        const colTypes = `{${cols.map((c) => `${q(c.name)}: ${q(c.type)}`).join(', ')}}`;
        const order = ['slot', 'tx_index', 'ix_path'].filter((c) => cols.some((x) => x.name === c)).map((c) => `"${c}"`).join(', ');
        for (const [date, group] of byDate) {
          const stage = join(staging, `${lane}-${lo}-${hi}-${table.replace(/\//g, '__')}-${date}.jsonl`);
          await writeFile(stage, group.map((r) => JSON.stringify(r)).join('\n') + '\n');
          written.push(stage);
          const rel = `${table}/date=${date}/${lo}-${hi}.parquet`;
          const final = join(dataRoot, rel);
          const tmp = final + '.tmp';
          await mkdir(dirname(final), { recursive: true });
          written.push(tmp);
          const kv = `{decoder_version: ${q(decoderVersion)}, idl_sha256: ${q(idlSha256)}, lane: ${q(lane)}, lo_slot: ${q(String(lo))}, hi_slot: ${q(String(hi))}}`;
          await conn.run(
            `COPY (SELECT ${colList} FROM read_json(${q(stage)}, format='newline_delimited', columns=${colTypes}, maximum_object_size=134217728) ORDER BY ${order}) ` +
            `TO ${q(tmp)} (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 3, KV_METADATA ${kv})`,
          );
          await fsyncFile(tmp);
          await rename(tmp, final);
          written.pop();
          records.push({ table, path: rel, rows: group.length, sha256: await sha256File(final) });
          await rm(stage, { force: true });
          written.splice(written.indexOf(stage), 1);
        }
      }
      return records;
    } catch (e) {
      for (const p of written) await rm(p, { force: true });
      for (const r of records) await rm(join(dataRoot, r.path), { force: true });
      throw e;
    }
  }

  static async cleanupTemp(dataRoot: string): Promise<number> {
    let n = 0;
    const staging = join(dataRoot, '.staging');
    try { for (const f of await readdir(staging)) { await rm(join(staging, f), { force: true }); n++; } } catch { /* no staging dir */ }
    const walk = async (dir: string): Promise<void> => {
      let entries;
      try { entries = await readdir(dir, { withFileTypes: true }); } catch { return; }
      for (const e of entries) {
        const p = join(dir, e.name);
        if (e.isDirectory()) { if (e.name !== '.staging') await walk(p); }
        else if (e.name.endsWith('.parquet.tmp')) { await rm(p, { force: true }); n++; }
      }
    };
    await walk(dataRoot);
    return n;
  }
}
```

- [ ] **Step 5: Implement `src/checkpoint.ts`**

```ts
import { mkdir } from 'node:fs/promises';
import { dirname } from 'node:path';
import { openDuck, q, rowsJson, type DuckDBConnection } from './duck.ts';
import type { FileRecord } from './writer.ts';

export interface LaneState { lane: string; startSlot: number; targetSlot: number | null; nextSlot: number }
export interface RangeRecord { lane: string; loSlot: number; hiSlot: number; status: string; decoderVersion: string; files: FileRecord[]; committedAt: string }

export class Checkpoint {
  readonly conn: DuckDBConnection;
  private constructor(conn: DuckDBConnection) { this.conn = conn; }

  static async open(path: string): Promise<Checkpoint> {
    await mkdir(dirname(path), { recursive: true });
    const conn = await openDuck(path);
    await conn.run(`CREATE TABLE IF NOT EXISTS lane_state (lane VARCHAR PRIMARY KEY, start_slot UBIGINT, target_slot UBIGINT, next_slot UBIGINT)`);
    await conn.run(`CREATE TABLE IF NOT EXISTS ranges (lane VARCHAR, lo_slot UBIGINT, hi_slot UBIGINT, status VARCHAR, decoder_version VARCHAR, files JSON, committed_at TIMESTAMP, PRIMARY KEY (lane, lo_slot))`);
    return new Checkpoint(conn);
  }

  async getState(lane: string): Promise<LaneState | null> {
    const rows = await rowsJson(this.conn, `SELECT lane, start_slot, target_slot, next_slot FROM lane_state WHERE lane = ${q(lane)}`);
    const r = rows[0];
    if (!r) return null;
    return { lane, startSlot: Number(r.start_slot), targetSlot: r.target_slot === null ? null : Number(r.target_slot), nextSlot: Number(r.next_slot) };
  }

  async initState(s: LaneState): Promise<void> {
    await this.conn.run(`INSERT OR REPLACE INTO lane_state VALUES (${q(s.lane)}, ${s.startSlot}, ${s.targetSlot === null ? 'NULL' : s.targetSlot}, ${s.nextSlot})`);
  }

  async commitRange(lane: string, lo: number, hi: number, files: FileRecord[], decoderVersion: string, nextSlot: number): Promise<void> {
    await this.conn.run('BEGIN');
    try {
      await this.conn.run(`INSERT OR REPLACE INTO ranges VALUES (${q(lane)}, ${lo}, ${hi}, 'committed', ${q(decoderVersion)}, ${q(JSON.stringify(files))}, now())`);
      await this.conn.run(`UPDATE lane_state SET next_slot = ${nextSlot} WHERE lane = ${q(lane)}`);
      await this.conn.run('COMMIT');
    } catch (e) { await this.conn.run('ROLLBACK'); throw e; }
  }

  async listRanges(lane: string): Promise<RangeRecord[]> {
    const rows = await rowsJson(this.conn, `SELECT lane, lo_slot, hi_slot, status, decoder_version, files::VARCHAR AS files, committed_at::VARCHAR AS committed_at FROM ranges WHERE lane = ${q(lane)} ORDER BY lo_slot`);
    return rows.map((r) => ({ lane, loSlot: Number(r.lo_slot), hiSlot: Number(r.hi_slot), status: String(r.status), decoderVersion: String(r.decoder_version), files: JSON.parse(String(r.files)) as FileRecord[], committedAt: String(r.committed_at) }));
  }

  async close(): Promise<void> { this.conn.closeSync(); }
}
```
If `closeSync` does not exist in `@duckdb/node-api 1.5.6-r.1`, use `this.conn.disconnectSync()`; check `node -e "console.log(Object.getOwnPropertyNames(require('@duckdb/node-api').DuckDBConnection.prototype))"`.

- [ ] **Step 6: Run tests and type check**

Run: `npm test && npm run check`
Expected: all pass. If the KV metadata test fails on `idl_sha256: ''`, confirm DuckDB keeps empty-string metadata; if it drops it, pass `idlSha256: 'none'` from callers and expect that.

- [ ] **Step 7: Commit**

```bash
git add src/duck.ts src/writer.ts src/checkpoint.ts tests/writer.test.ts tests/checkpoint.test.ts
git commit -m "Range writer: JSONL staging to typed ZSTD Parquet with fsync and rename; DuckDB checkpoint

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 10: RPC client and rate limiter

**Files:**
- Create: `src/rpc.ts`
- Test: `tests/rpc.test.ts`

**Interfaces:**
- Produces: `class RateLimiter { constructor(perSecond: number); take(): Promise<void>; setRate(perSecond): void }`, `class RpcError extends Error { code: number | null; retryable: boolean }`, `type BlockResult = { kind: 'block'; block: RpcBlock } | { kind: 'skipped' }`, `class RpcClient { constructor(url, limiter, opts?: { timeoutMs?: number; maxRetries?: number; fetchImpl?: typeof fetch; sleep?: (ms) => Promise<void> }); getSlot(): Promise<number>; getBlock(slot): Promise<BlockResult>; getAccountExecutable(pubkey): Promise<boolean | null> }`.

- [ ] **Step 1: Write the failing test**

`tests/rpc.test.ts`:
```ts
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { RpcClient, RateLimiter, RpcError } from '../src/rpc.ts';

function fakeFetch(responses: Array<{ status?: number; body?: unknown; throws?: boolean }>) {
  const calls: unknown[] = [];
  const f = (async (_url: string | URL | Request, init?: RequestInit) => {
    calls.push(JSON.parse(String(init?.body)));
    const r = responses.shift() ?? { status: 200, body: { jsonrpc: '2.0', id: 1, result: null } };
    if (r.throws) throw new Error('socket hang up');
    return new Response(JSON.stringify(r.body ?? {}), { status: r.status ?? 200, headers: { 'content-type': 'application/json' } });
  }) as unknown as typeof fetch;
  return { f, calls };
}
const noSleep = async () => {};

test('getBlock sends the required parameters', async () => {
  const { f, calls } = fakeFetch([{ body: { jsonrpc: '2.0', id: 1, result: { blockTime: 1, blockHeight: 1, parentSlot: 9, transactions: [] } } }]);
  const c = new RpcClient('https://rpc.example/key', new RateLimiter(1000), { fetchImpl: f, sleep: noSleep });
  const r = await c.getBlock(10);
  assert.equal(r.kind, 'block');
  const req = calls[0] as { method: string; params: unknown[] };
  assert.equal(req.method, 'getBlock');
  assert.deepEqual(req.params, [10, { encoding: 'json', transactionDetails: 'full', maxSupportedTransactionVersion: 1, rewards: false, commitment: 'finalized' }]);
});

test('skipped-slot error codes return skipped', async () => {
  for (const code of [-32007, -32009]) {
    const { f } = fakeFetch([{ body: { jsonrpc: '2.0', id: 1, error: { code, message: 'Slot was skipped' } } }]);
    const c = new RpcClient('https://rpc.example/key', new RateLimiter(1000), { fetchImpl: f, sleep: noSleep });
    assert.deepEqual(await c.getBlock(10), { kind: 'skipped' });
  }
});

test('429 and -32004 are retried, then succeed; retries exhausted throw a retryable RpcError without the URL', async () => {
  const ok = { body: { jsonrpc: '2.0', id: 1, result: { blockTime: 1, blockHeight: 1, parentSlot: 9, transactions: [] } } };
  const { f } = fakeFetch([{ status: 429, body: {} }, { body: { jsonrpc: '2.0', id: 1, error: { code: -32004, message: 'Block not available for slot 10' } } }, ok]);
  const c = new RpcClient('https://rpc.example/secret-key', new RateLimiter(1000), { fetchImpl: f, sleep: noSleep, maxRetries: 5 });
  assert.equal((await c.getBlock(10)).kind, 'block');
  const bad = fakeFetch([{ status: 429 }, { status: 429 }, { status: 429 }]);
  const c2 = new RpcClient('https://rpc.example/secret-key', new RateLimiter(1000), { fetchImpl: bad.f, sleep: noSleep, maxRetries: 2 });
  await assert.rejects(() => c2.getBlock(10), (e: unknown) => e instanceof RpcError && e.retryable && !e.message.includes('secret-key'));
});

test('rate limiter spaces calls', async () => {
  const l = new RateLimiter(50);
  const t0 = Date.now();
  for (let i = 0; i < 6; i++) await l.take();
  assert.ok(Date.now() - t0 >= 90, 'six takes at 50/s need at least ~100ms');
});
```

- [ ] **Step 2: Run to verify failure**

Run: `npm test`
Expected: FAIL, module not found.

- [ ] **Step 3: Implement `src/rpc.ts`**

```ts
import type { RpcBlock } from './tx.ts';

export class RateLimiter {
  private perSecond: number;
  private tokens: number;
  private last = Date.now();
  constructor(perSecond: number) { this.perSecond = perSecond; this.tokens = Math.max(1, perSecond); }
  setRate(perSecond: number): void { this.perSecond = Math.max(0.1, perSecond); }
  async take(): Promise<void> {
    for (;;) {
      const now = Date.now();
      this.tokens = Math.min(this.perSecond, this.tokens + ((now - this.last) / 1000) * this.perSecond);
      this.last = now;
      if (this.tokens >= 1) { this.tokens -= 1; return; }
      await new Promise((r) => setTimeout(r, Math.ceil(((1 - this.tokens) / this.perSecond) * 1000)));
    }
  }
}

export class RpcError extends Error {
  code: number | null; retryable: boolean;
  constructor(message: string, code: number | null, retryable: boolean) { super(message); this.code = code; this.retryable = retryable; }
}

export type BlockResult = { kind: 'block'; block: RpcBlock } | { kind: 'skipped' };

const SKIPPED = new Set([-32007, -32009]);
const RETRY_CODES = new Set([-32004, -32005, -32014, -32016]);
const sleepDefault = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

export class RpcClient {
  private url: string; private limiter: RateLimiter; private timeoutMs: number; private maxRetries: number;
  private fetchImpl: typeof fetch; private sleep: (ms: number) => Promise<void>;
  constructor(url: string, limiter: RateLimiter, opts: { timeoutMs?: number; maxRetries?: number; fetchImpl?: typeof fetch; sleep?: (ms: number) => Promise<void> } = {}) {
    this.url = url; this.limiter = limiter; this.timeoutMs = opts.timeoutMs ?? 60_000; this.maxRetries = opts.maxRetries ?? 8;
    this.fetchImpl = opts.fetchImpl ?? fetch; this.sleep = opts.sleep ?? sleepDefault;
  }

  private async call(method: string, params: unknown[]): Promise<{ result?: unknown; error?: { code: number; message: string } }> {
    let attempt = 0;
    for (;;) {
      await this.limiter.take();
      try {
        const res = await this.fetchImpl(this.url, {
          method: 'POST', headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ jsonrpc: '2.0', id: 1, method, params }), signal: AbortSignal.timeout(this.timeoutMs),
        });
        if (res.status === 429 || res.status >= 500) throw new RpcError(`${method}: HTTP ${res.status}`, null, true);
        if (!res.ok) throw new RpcError(`${method}: HTTP ${res.status}`, null, false);
        const json = (await res.json()) as { result?: unknown; error?: { code: number; message: string } };
        if (json.error && RETRY_CODES.has(json.error.code)) throw new RpcError(`${method}: ${json.error.code} ${json.error.message}`, json.error.code, true);
        return json;
      } catch (e) {
        const err = e instanceof RpcError ? e : new RpcError(`${method}: ${e instanceof Error ? e.message : String(e)}`, null, true);
        if (!err.retryable || attempt >= this.maxRetries) throw err;
        attempt++;
        await this.sleep(Math.min(30_000, 250 * 2 ** attempt));
      }
    }
  }

  async getSlot(): Promise<number> {
    const r = await this.call('getSlot', [{ commitment: 'finalized' }]);
    if (r.error) throw new RpcError(`getSlot: ${r.error.code} ${r.error.message}`, r.error.code, false);
    return Number(r.result);
  }

  async getBlock(slot: number): Promise<BlockResult> {
    const r = await this.call('getBlock', [slot, { encoding: 'json', transactionDetails: 'full', maxSupportedTransactionVersion: 1, rewards: false, commitment: 'finalized' }]);
    if (r.error) {
      if (SKIPPED.has(r.error.code)) return { kind: 'skipped' };
      throw new RpcError(`getBlock(${slot}): ${r.error.code} ${r.error.message}`, r.error.code, false);
    }
    if (r.result === null || r.result === undefined) return { kind: 'skipped' };
    return { kind: 'block', block: r.result as RpcBlock };
  }

  async getAccountExecutable(pubkey: string): Promise<boolean | null> {
    const r = await this.call('getAccountInfo', [pubkey, { encoding: 'base64', commitment: 'finalized' }]);
    const v = (r.result as { value?: { executable?: boolean } | null } | undefined)?.value;
    return v ? Boolean(v.executable) : null;
  }
}
```

- [ ] **Step 4: Run tests and type check**

Run: `npm test && npm run check`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/rpc.ts tests/rpc.test.ts
git commit -m "RPC client with token-bucket limiter, skipped-slot classification and retry without leaking the URL

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 11: Config, lanes, decode pool, CLI

**Files:**
- Create: `src/config.ts`, `config/collector.json`, `src/lane.ts`, `src/decodePool.ts`, `src/worker.ts`, `src/cli.ts`
- Test: `tests/lane.test.ts`

**Interfaces:**
- Consumes: Tasks 8–10.
- Produces:
  - `src/config.ts`: `interface CollectorConfig { rangeSlots; dataRoot; stateRoot; workers; tail: { blocksPerSecond; concurrency; tipMargin }; backfill: { blocksPerSecond; concurrency } }`, `loadConfig(path = 'config/collector.json'): CollectorConfig`, `loadEnv(): { rpcUrl: string }` (reads `.env` if present, then `process.env.SOLANA_RPC_URL`; throws if missing).
  - `src/decodePool.ts`: `interface Decoder { decode(block: RpcBlock | null, slot: number): Promise<DecodedBlock>; close(): Promise<void> }`, `inlineDecoder(ctx): Decoder`, `workerDecoder(n: number): Decoder` (workers load their own `DecodeContext` from `config/`).
  - `src/lane.ts`: `interface LaneDeps { lane: 'tail' | 'backfill'; rpc: RpcClient; checkpoint: Checkpoint; writer: RangeWriter; decoder: Decoder; rangeSlots; concurrency; decoderVersion; tipMargin?: number; targetSlot?: number; fromSlot?: number; maxRanges?: number; log?: (m: string) => void }`, `processRange(deps, lo, hi): Promise<FileRecord[]>`, `runLane(deps): Promise<void>`.
  - `src/cli.ts`: commands `tail`, `backfill --until <YYYY-MM-DD|slot>`, `status`, `verify`, `redecode`, `query --sql`, `registry check`; `--max-ranges N` on `tail`/`backfill` for bounded runs.

- [ ] **Step 1: Write `config/collector.json` and `src/config.ts`**

`config/collector.json`:
```json
{
  "rangeSlots": 5000,
  "dataRoot": "data",
  "stateRoot": "state",
  "workers": 0,
  "tail": { "blocksPerSecond": 3, "concurrency": 4, "tipMargin": 32 },
  "backfill": { "blocksPerSecond": 10, "concurrency": 8 }
}
```

`src/config.ts`:
```ts
import { existsSync, readFileSync } from 'node:fs';

export interface LaneConfig { blocksPerSecond: number; concurrency: number; tipMargin?: number }
export interface CollectorConfig { rangeSlots: number; dataRoot: string; stateRoot: string; workers: number; tail: LaneConfig & { tipMargin: number }; backfill: LaneConfig }

export function loadConfig(path = 'config/collector.json'): CollectorConfig {
  const c = JSON.parse(readFileSync(path, 'utf8')) as CollectorConfig;
  if (process.env.FIBER_DATA_ROOT) c.dataRoot = process.env.FIBER_DATA_ROOT;
  if (process.env.FIBER_STATE_ROOT) c.stateRoot = process.env.FIBER_STATE_ROOT;
  return c;
}

export function loadEnv(): { rpcUrl: string } {
  if (existsSync('.env')) {
    for (const line of readFileSync('.env', 'utf8').split('\n')) {
      const m = /^\s*([A-Z0-9_]+)\s*=\s*(.*?)\s*$/.exec(line);
      if (m && process.env[m[1]!] === undefined) process.env[m[1]!] = m[2]!.replace(/^["']|["']$/g, '');
    }
  }
  const rpcUrl = process.env.SOLANA_RPC_URL;
  if (!rpcUrl) throw new Error('SOLANA_RPC_URL is not set (put it in .env)');
  return { rpcUrl };
}
```

- [ ] **Step 2: Write the failing lane test**

`tests/lane.test.ts`:
```ts
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { runLane, processRange } from '../src/lane.ts';
import { Checkpoint } from '../src/checkpoint.ts';
import { RangeWriter } from '../src/writer.ts';
import { openDuck, rowsJson } from '../src/duck.ts';
import { schemaFor } from '../src/tables.ts';
import { inlineDecoder } from '../src/decodePool.ts';
import { buildContext, loadFixture } from './helpers.ts';
import type { RpcClient, BlockResult } from '../src/rpc.ts';
import type { RpcBlock } from '../src/tx.ts';

function fakeRpc(tip: number, blocks: Map<number, RpcBlock | 'skipped'>, failOnce: Set<number> = new Set()): RpcClient {
  return {
    getSlot: async () => tip,
    getBlock: async (slot: number): Promise<BlockResult> => {
      if (failOnce.has(slot)) { failOnce.delete(slot); throw Object.assign(new Error('HTTP 429'), { retryable: true }); }
      const b = blocks.get(slot);
      if (b === undefined || b === 'skipped') return { kind: 'skipped' };
      return { kind: 'block', block: b };
    },
  } as unknown as RpcClient;
}
function blk(names: string[], t: number): RpcBlock { return { blockTime: t, blockHeight: 1, parentSlot: 0, transactions: names.map((n) => loadFixture(n).tx) }; }

async function setup() {
  const root = mkdtempSync(join(tmpdir(), 'fiber-lane-'));
  const ctx = buildContext();
  const checkpoint = await Checkpoint.open(join(root, 'state', 'tail.duckdb'));
  const writer = new RangeWriter({ dataRoot: join(root, 'data'), conn: await openDuck(':memory:'), decoderVersion: '1.0.0', schemaFor: (t) => schemaFor(t, ctx) });
  return { root, ctx, checkpoint, writer };
}

test('tail processes ranges up to the tip margin, records skipped slots, commits and resumes', async () => {
  const { root, ctx, checkpoint, writer } = await setup();
  const blocks = new Map<number, RpcBlock | 'skipped'>([[100, blk(['humidifi_swap'], 1792085183)], [101, 'skipped'], [102, blk(['whirlpool_traded'], 1792085184)], [103, blk([], 1792085185)]]);
  const deps = { lane: 'tail' as const, rpc: fakeRpc(135, blocks), checkpoint, writer, decoder: inlineDecoder(ctx), rangeSlots: 2, concurrency: 2, decoderVersion: '1.0.0', tipMargin: 32, fromSlot: 100, maxRanges: 2 };
  await runLane(deps);                                     // ranges 100-101 and 102-103
  const st = await checkpoint.getState('tail');
  assert.equal(st?.nextSlot, 104);
  const ranges = await checkpoint.listRanges('tail');
  assert.deepEqual(ranges.map((r) => [r.loSlot, r.hiSlot]), [[100, 101], [102, 103]]);
  const conn = await openDuck(':memory:');
  const rows = await rowsJson(conn, `SELECT slot, status FROM read_parquet('${join(root, 'data', 'blocks', '*', '*.parquet')}') ORDER BY slot`);
  assert.deepEqual(rows.map((r) => [Number(r.slot), r.status]), [[100, 'ok'], [101, 'skipped'], [102, 'ok'], [103, 'ok']]);
  assert.ok(existsSync(join(root, 'data', 'swaps', 'humidifi')));
  // resume: tip unchanged, nothing new within margin -> returns without new ranges when maxRanges is 0
  await runLane({ ...deps, maxRanges: 0 });
  assert.equal((await checkpoint.listRanges('tail')).length, 2);
  await checkpoint.close();
});

test('a retryable fetch error inside a range is retried by the client; the lane never records it as skipped', async () => {
  const { ctx, checkpoint, writer } = await setup();
  const blocks = new Map<number, RpcBlock | 'skipped'>([[10, blk([], 1)], [11, blk([], 2)]]);
  // fakeRpc throws once for slot 11; processRange must propagate (the real client retries internally), and nothing is committed
  const deps = { lane: 'tail' as const, rpc: fakeRpc(100, blocks, new Set([11])), checkpoint, writer, decoder: inlineDecoder(ctx), rangeSlots: 2, concurrency: 2, decoderVersion: '1.0.0' };
  await assert.rejects(() => processRange(deps, 10, 11));
  assert.equal((await checkpoint.listRanges('tail')).length, 0);
  const files = await processRange(deps, 10, 11);          // second attempt succeeds
  assert.ok(files.some((f) => f.table === 'blocks' && f.rows === 2));
  await checkpoint.close();
});

test('backfill walks backward from fromSlot to targetSlot', async () => {
  const { checkpoint, writer, ctx } = await setup();
  const blocks = new Map<number, RpcBlock | 'skipped'>();
  for (let s = 90; s <= 99; s++) blocks.set(s, blk([], 1792085183 + s));
  await runLane({ lane: 'backfill', rpc: fakeRpc(1000, blocks), checkpoint, writer, decoder: inlineDecoder(ctx), rangeSlots: 4, concurrency: 4, decoderVersion: '1.0.0', fromSlot: 99, targetSlot: 90 });
  const ranges = await checkpoint.listRanges('backfill');
  assert.deepEqual(ranges.map((r) => [r.loSlot, r.hiSlot]).sort((a, b) => a[0]! - b[0]!), [[90, 91], [92, 95], [96, 99]]);
  assert.equal((await checkpoint.getState('backfill'))?.nextSlot, 89);
  await checkpoint.close();
});
```

- [ ] **Step 3: Run to verify failure**

Run: `npm test`
Expected: FAIL, modules not found.

- [ ] **Step 4: Implement `src/decodePool.ts` and `src/worker.ts`**

`src/decodePool.ts`:
```ts
import { Worker } from 'node:worker_threads';
import type { RpcBlock } from './tx.ts';
import { decodeBlock, type DecodedBlock } from './decoders/block.ts';
import type { DecodeContext } from './decoders/context.ts';

export interface Decoder { decode(block: RpcBlock | null, slot: number): Promise<DecodedBlock>; close(): Promise<void> }

export function inlineDecoder(ctx: DecodeContext): Decoder {
  return { decode: async (block, slot) => decodeBlock(block, slot, ctx), close: async () => {} };
}

interface Pending { resolve: (d: DecodedBlock) => void; reject: (e: Error) => void }

export function workerDecoder(n: number): Decoder {
  const workers = Array.from({ length: n }, () => new Worker(new URL('./worker.ts', import.meta.url)));
  const pending = new Map<number, Pending>();
  let next = 0, rr = 0;
  for (const w of workers) {
    w.on('message', (m: { id: number; ok: boolean; tables?: [string, Record<string, unknown>[]][]; stats?: Record<string, number>; slot?: number; error?: string }) => {
      const p = pending.get(m.id); if (!p) return; pending.delete(m.id);
      if (m.ok) p.resolve({ slot: m.slot!, tables: new Map(m.tables), stats: m.stats ?? {} }); else p.reject(new Error(m.error));
    });
    w.on('error', (e) => { for (const p of pending.values()) p.reject(e); pending.clear(); });
  }
  return {
    decode: (block, slot) => new Promise((resolve, reject) => {
      const id = next++; pending.set(id, { resolve, reject });
      workers[rr++ % workers.length]!.postMessage({ id, slot, block });
    }),
    close: async () => { await Promise.all(workers.map((w) => w.terminate())); },
  };
}
```

`src/worker.ts`:
```ts
import { parentPort } from 'node:worker_threads';
import { loadRegistry } from './registry.ts';
import { buildDecodeContext } from './decoders/context.ts';
import { decodeBlock } from './decoders/block.ts';
import type { RpcBlock } from './tx.ts';

const ctx = buildDecodeContext(loadRegistry('config/venues.json', 'config/routers.json'));
parentPort!.on('message', (m: { id: number; slot: number; block: RpcBlock | null }) => {
  try {
    const d = decodeBlock(m.block, m.slot, ctx);
    parentPort!.postMessage({ id: m.id, ok: true, slot: d.slot, tables: [...d.tables.entries()], stats: d.stats });
  } catch (e) {
    parentPort!.postMessage({ id: m.id, ok: false, error: e instanceof Error ? e.stack ?? e.message : String(e) });
  }
});
```

- [ ] **Step 5: Implement `src/lane.ts`**

```ts
import type { RpcClient } from './rpc.ts';
import type { Checkpoint } from './checkpoint.ts';
import { RangeWriter, type FileRecord } from './writer.ts';
import type { Decoder } from './decodePool.ts';
import { mergeTables } from './decoders/block.ts';
import type { Row } from './rows.ts';

export interface LaneDeps {
  lane: 'tail' | 'backfill'; rpc: RpcClient; checkpoint: Checkpoint; writer: RangeWriter; decoder: Decoder;
  rangeSlots: number; concurrency: number; decoderVersion: string;
  tipMargin?: number; targetSlot?: number; fromSlot?: number; maxRanges?: number; log?: (m: string) => void;
}

async function mapLimit<T, R>(items: T[], limit: number, fn: (t: T) => Promise<R>): Promise<R[]> {
  const out: R[] = new Array(items.length);
  let i = 0;
  const workers = Array.from({ length: Math.min(limit, items.length) }, async () => { for (;;) { const k = i++; if (k >= items.length) return; out[k] = await fn(items[k]!); } });
  await Promise.all(workers);
  return out;
}

export async function processRange(deps: LaneDeps, lo: number, hi: number): Promise<FileRecord[]> {
  const slots = Array.from({ length: hi - lo + 1 }, (_, k) => lo + k);
  const decoded = await mapLimit(slots, deps.concurrency, async (slot) => {
    const r = await deps.rpc.getBlock(slot);
    return deps.decoder.decode(r.kind === 'block' ? r.block : null, slot);
  });
  const tables = new Map<string, Row[]>();
  const stats: Record<string, number> = {};
  for (const d of decoded) { mergeTables(tables, d.tables); for (const [k, v] of Object.entries(d.stats)) stats[k] = (stats[k] ?? 0) + v; }
  const files = await deps.writer.writeRange(deps.lane, lo, hi, tables);
  deps.log?.(`${deps.lane} range ${lo}-${hi}: ${JSON.stringify(stats)} -> ${files.length} files`);
  return files;
}

export async function runLane(deps: LaneDeps): Promise<void> {
  const { lane, checkpoint, rangeSlots } = deps;
  let state = await checkpoint.getState(lane);
  if (!state) {
    if (lane === 'tail') {
      const start = deps.fromSlot ?? (await deps.rpc.getSlot()) - (deps.tipMargin ?? 32);
      state = { lane, startSlot: start, targetSlot: null, nextSlot: start };
    } else {
      if (deps.fromSlot === undefined || deps.targetSlot === undefined) throw new Error('backfill needs fromSlot and targetSlot on first start');
      state = { lane, startSlot: deps.fromSlot, targetSlot: deps.targetSlot, nextSlot: deps.fromSlot };
    }
    await checkpoint.initState(state);
  }
  let ranges = 0;
  for (;;) {
    if (deps.maxRanges !== undefined && ranges >= deps.maxRanges) return;
    let lo: number, hi: number, nextAfter: number;
    if (lane === 'tail') {
      const tip = (await deps.rpc.getSlot()) - (deps.tipMargin ?? 32);
      hi = Math.min(state.nextSlot + rangeSlots - 1, tip);
      lo = state.nextSlot;
      if (hi < lo) { await new Promise((r) => setTimeout(r, 2000)); continue; }
      nextAfter = hi + 1;
    } else {
      const target = state.targetSlot!;
      if (state.nextSlot < target) return;
      hi = state.nextSlot;
      lo = Math.max(hi - rangeSlots + 1, target);
      nextAfter = lo - 1;
    }
    const files = await processRange(deps, lo, hi);
    await checkpoint.commitRange(lane, lo, hi, files, deps.decoderVersion, nextAfter);
    state = { ...state, nextSlot: nextAfter };
    ranges++;
  }
}
```

- [ ] **Step 6: Implement `src/cli.ts`**

```ts
import { join } from 'node:path';
import { loadConfig, loadEnv } from './config.ts';
import { loadRegistry } from './registry.ts';
import { buildDecodeContext } from './decoders/context.ts';
import { RpcClient, RateLimiter } from './rpc.ts';
import { Checkpoint } from './checkpoint.ts';
import { RangeWriter } from './writer.ts';
import { openDuck } from './duck.ts';
import { schemaFor } from './tables.ts';
import { inlineDecoder, workerDecoder } from './decodePool.ts';
import { runLane } from './lane.ts';
import { DECODER_VERSION } from './version.ts';
import { status } from './commands/status.ts';
import { verify } from './commands/verify.ts';
import { redecode } from './commands/redecode.ts';
import { registryCheck } from './commands/registryCheck.ts';
import { query } from './commands/query.ts';

function args(argv: string[]): { positional: string[]; flags: Record<string, string> } {
  const positional: string[] = []; const flags: Record<string, string> = {};
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i]!;
    if (a.startsWith('--')) { const k = a.slice(2); const v = argv[i + 1]; if (v !== undefined && !v.startsWith('--')) { flags[k] = v; i++; } else flags[k] = 'true'; }
    else positional.push(a);
  }
  return { positional, flags };
}

async function resolveUntil(rpc: RpcClient, until: string, tipSlot: number): Promise<number> {
  if (/^\d+$/.test(until)) return Number(until);
  const targetMs = Date.parse(until + 'T00:00:00Z');
  if (Number.isNaN(targetMs)) throw new Error(`--until must be a slot or YYYY-MM-DD, got ${until}`);
  const secondsBack = Math.max(0, (Date.now() - targetMs) / 1000);
  return Math.max(0, tipSlot - Math.round(secondsBack / 0.4));   // 400 ms slots; verify refines nothing here, the lane stops at this slot
}

async function main(): Promise<void> {
  const { positional, flags } = args(process.argv.slice(2));
  const cmd = positional[0];
  const cfg = loadConfig();
  const registry = loadRegistry('config/venues.json', 'config/routers.json');
  const ctx = buildDecodeContext(registry);
  const log = (m: string) => console.log(`${new Date().toISOString()} ${m}`);

  if (cmd === 'tail' || cmd === 'backfill') {
    const { rpcUrl } = loadEnv();
    const laneCfg = cmd === 'tail' ? cfg.tail : cfg.backfill;
    const rpc = new RpcClient(rpcUrl, new RateLimiter(laneCfg.blocksPerSecond));
    await RangeWriter.cleanupTemp(cfg.dataRoot);
    const checkpoint = await Checkpoint.open(join(cfg.stateRoot, `${cmd}.duckdb`));
    const writer = new RangeWriter({ dataRoot: cfg.dataRoot, conn: await openDuck(':memory:'), decoderVersion: DECODER_VERSION, schemaFor: (t) => schemaFor(t, ctx), idlSha256: [...ctx.idlsByLabel.values()].map((i) => i.sha256.slice(0, 8)).join(',') });
    const decoder = cfg.workers > 0 ? workerDecoder(cfg.workers) : inlineDecoder(ctx);
    let fromSlot: number | undefined, targetSlot: number | undefined;
    if (cmd === 'backfill' && !(await checkpoint.getState('backfill'))) {
      const tailState = await (await Checkpoint.open(join(cfg.stateRoot, 'tail.duckdb'))).getState('tail');
      if (!tailState) throw new Error('start the tail lane once before backfill so the start slot is recorded');
      fromSlot = tailState.startSlot - 1;
      if (!flags.until) throw new Error('backfill needs --until <YYYY-MM-DD|slot> on first start');
      targetSlot = await resolveUntil(rpc, flags.until, tailState.startSlot);
      log(`backfill from ${fromSlot} down to ${targetSlot}`);
    }
    try {
      await runLane({ lane: cmd, rpc, checkpoint, writer, decoder, rangeSlots: cfg.rangeSlots, concurrency: laneCfg.concurrency, decoderVersion: DECODER_VERSION, tipMargin: cfg.tail.tipMargin, fromSlot, targetSlot, maxRanges: flags['max-ranges'] ? Number(flags['max-ranges']) : undefined, log });
    } finally { await decoder.close(); await checkpoint.close(); }
    return;
  }
  if (cmd === 'status') return status(cfg);
  if (cmd === 'verify') return verify(cfg, flags.lane ?? 'tail');
  if (cmd === 'redecode') return redecode(cfg, ctx, flags);
  if (cmd === 'registry' && positional[1] === 'check') return registryCheck(registry, new RpcClient(loadEnv().rpcUrl, new RateLimiter(5)));
  if (cmd === 'query') return query(cfg, flags.sql ?? positional.slice(1).join(' '));
  console.error('usage: fiber-data <tail|backfill --until D|status|verify [--lane L]|redecode|registry check|query --sql S> [--max-ranges N]');
  process.exit(2);
}

main().catch((e) => { console.error(e instanceof Error ? e.message : String(e)); process.exit(1); });
```
The command modules are written in Task 12; until then create them as stubs that `throw new Error('not implemented')` so the type check passes.

- [ ] **Step 7: Run tests and type check**

Run: `npm test && npm run check`
Expected: all pass, including the three lane tests.

- [ ] **Step 8: Commit**

```bash
git add src/config.ts config/collector.json src/lane.ts src/decodePool.ts src/worker.ts src/cli.ts src/commands tests/lane.test.ts
git commit -m "Lanes (tail, backfill), decode pool, config and CLI

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 12: Operational commands, Docker, docs, tiny live run

**Files:**
- Create: `src/commands/status.ts`, `src/commands/verify.ts`, `src/commands/redecode.ts`, `src/commands/registryCheck.ts`, `src/commands/query.ts`, `Dockerfile`, `compose.yaml`, `README.md`, `docs/architecture.md`
- Test: `tests/verify.test.ts`

**Interfaces:**
- Produces: `status(cfg)`, `verify(cfg, lane): Promise<void>` (exit code 1 on any problem) with a pure helper `verifyLane(dataRoot, checkpoint, lane): Promise<{ problems: string[]; ranges: number; files: number }>`, `redecode(cfg, ctx, flags)`, `registryCheck(registry, rpc)`, `query(cfg, sql)`.

- [ ] **Step 1: Write the failing verify test**

`tests/verify.test.ts`:
```ts
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { verifyLane } from '../src/commands/verify.ts';
import { Checkpoint } from '../src/checkpoint.ts';
import { RangeWriter } from '../src/writer.ts';
import { openDuck } from '../src/duck.ts';
import { schemaFor } from '../src/tables.ts';
import { buildContext } from './helpers.ts';

test('verify passes on a consistent lane, flags a changed file and a slot gap', async () => {
  const root = mkdtempSync(join(tmpdir(), 'fiber-verify-'));
  const ctx = buildContext();
  const ckpt = await Checkpoint.open(join(root, 'state', 'tail.duckdb'));
  await ckpt.initState({ lane: 'tail', startSlot: 1, targetSlot: null, nextSlot: 1 });
  const writer = new RangeWriter({ dataRoot: join(root, 'data'), conn: await openDuck(':memory:'), decoderVersion: '1.0.0', schemaFor: (t) => schemaFor(t, ctx) });
  const blocks = (lo: number, hi: number) => Array.from({ length: hi - lo + 1 }, (_, i) => ({ slot: lo + i, block_time: '2026-10-15 00:00:00', status: 'ok' }));
  let files = await writer.writeRange('tail', 1, 3, new Map([['blocks', blocks(1, 3)]]));
  await ckpt.commitRange('tail', 1, 3, files, '1.0.0', 4);
  files = await writer.writeRange('tail', 4, 5, new Map([['blocks', blocks(4, 5)]]));
  await ckpt.commitRange('tail', 4, 5, files, '1.0.0', 6);
  let r = await verifyLane(join(root, 'data'), ckpt, 'tail');
  assert.deepEqual(r.problems, []);
  assert.equal(r.ranges, 2);
  // tamper with a file
  writeFileSync(join(root, 'data', files[0]!.path), 'corrupt');
  r = await verifyLane(join(root, 'data'), ckpt, 'tail');
  assert.ok(r.problems.some((p) => p.includes('sha256')));
  // gap
  files = await writer.writeRange('tail', 8, 9, new Map([['blocks', blocks(8, 9)]]));
  await ckpt.commitRange('tail', 8, 9, files, '1.0.0', 10);
  r = await verifyLane(join(root, 'data'), ckpt, 'tail');
  assert.ok(r.problems.some((p) => p.includes('gap')));
  await ckpt.close();
});
```

- [ ] **Step 2: Run to verify failure**

Run: `npm test -- --test-name-pattern=verify`
Expected: FAIL (stub throws or module missing).

- [ ] **Step 3: Implement the command modules**

`src/commands/verify.ts`:
```ts
import { createHash } from 'node:crypto';
import { createReadStream } from 'node:fs';
import { join } from 'node:path';
import type { CollectorConfig } from '../config.ts';
import { Checkpoint } from '../checkpoint.ts';
import { openDuck, rowsJson, q } from '../duck.ts';

async function sha256(path: string): Promise<string | null> {
  try { const h = createHash('sha256'); for await (const c of createReadStream(path)) h.update(c as Buffer); return h.digest('hex'); } catch { return null; }
}

export async function verifyLane(dataRoot: string, checkpoint: Checkpoint, lane: string): Promise<{ problems: string[]; ranges: number; files: number }> {
  const problems: string[] = [];
  const ranges = await checkpoint.listRanges(lane);
  const sorted = [...ranges].sort((a, b) => a.loSlot - b.loSlot);
  for (let i = 1; i < sorted.length; i++) {
    const prev = sorted[i - 1]!, cur = sorted[i]!;
    if (cur.loSlot !== prev.hiSlot + 1) problems.push(`gap or overlap between ranges ${prev.loSlot}-${prev.hiSlot} and ${cur.loSlot}-${cur.hiSlot}`);
  }
  let files = 0;
  const conn = await openDuck(':memory:');
  for (const r of sorted) {
    for (const f of r.files) {
      files++;
      const actual = await sha256(join(dataRoot, f.path));
      if (actual === null) problems.push(`missing file ${f.path}`);
      else if (actual !== f.sha256) problems.push(`sha256 mismatch ${f.path}`);
    }
    const blockFiles = r.files.filter((f) => f.table === 'blocks').map((f) => join(dataRoot, f.path));
    if (blockFiles.length === 0) { problems.push(`range ${r.loSlot}-${r.hiSlot} has no blocks file`); continue; }
    try {
      const rows = await rowsJson(conn, `SELECT count(*) AS n, min(slot) AS lo, max(slot) AS hi FROM read_parquet([${blockFiles.map(q).join(', ')}])`);
      const n = Number(rows[0]!.n), lo = Number(rows[0]!.lo), hi = Number(rows[0]!.hi);
      if (n !== r.hiSlot - r.loSlot + 1 || lo !== r.loSlot || hi !== r.hiSlot) problems.push(`range ${r.loSlot}-${r.hiSlot}: blocks rows ${n} (${lo}-${hi}) do not cover the range`);
    } catch (e) { problems.push(`range ${r.loSlot}-${r.hiSlot}: cannot read blocks (${e instanceof Error ? e.message : String(e)})`); }
  }
  return { problems, ranges: ranges.length, files };
}

export async function verify(cfg: CollectorConfig, lane: string): Promise<void> {
  const ckpt = await Checkpoint.open(join(cfg.stateRoot, `${lane}.duckdb`));
  try {
    const r = await verifyLane(cfg.dataRoot, ckpt, lane);
    console.log(`${lane}: ${r.ranges} ranges, ${r.files} files, ${r.problems.length} problems`);
    for (const p of r.problems) console.log(`  ${p}`);
    if (r.problems.length) process.exitCode = 1;
  } finally { await ckpt.close(); }
}
```

`src/commands/status.ts`:
```ts
import { join } from 'node:path';
import { existsSync } from 'node:fs';
import type { CollectorConfig } from '../config.ts';
import { Checkpoint } from '../checkpoint.ts';

export async function status(cfg: CollectorConfig): Promise<void> {
  for (const lane of ['tail', 'backfill']) {
    const path = join(cfg.stateRoot, `${lane}.duckdb`);
    if (!existsSync(path)) { console.log(`${lane}: not started`); continue; }
    const c = await Checkpoint.open(path);
    try {
      const s = await c.getState(lane);
      const ranges = await c.listRanges(lane);
      const rows = new Map<string, number>();
      let bytesHint = 0;
      for (const r of ranges) for (const f of r.files) { rows.set(f.table, (rows.get(f.table) ?? 0) + f.rows); bytesHint++; }
      console.log(`${lane}: start ${s?.startSlot} next ${s?.nextSlot} target ${s?.targetSlot ?? '-'} ranges ${ranges.length} files ${bytesHint}`);
      for (const [t, n] of [...rows.entries()].sort((a, b) => b[1] - a[1]).slice(0, 25)) console.log(`  ${t.padEnd(56)} ${n}`);
    } finally { await c.close(); }
  }
}
```

`src/commands/query.ts`:
```ts
import { join } from 'node:path';
import { existsSync, readdirSync } from 'node:fs';
import type { CollectorConfig } from '../config.ts';
import { openDuck, q } from '../duck.ts';

// Registers one view per table directory (events/<p>/<E> -> events__p__E, swaps/<p> -> swaps__p, intents, blocks, decode_failures) then runs the SQL.
export async function query(cfg: CollectorConfig, sql: string): Promise<void> {
  const conn = await openDuck(':memory:');
  const root = cfg.dataRoot;
  const views: string[] = [];
  const addView = (name: string, dir: string) => { if (existsSync(join(root, dir))) views.push(`CREATE VIEW "${name}" AS SELECT * FROM read_parquet(${q(join(root, dir, '*', '*.parquet'))}, union_by_name=true)`); };
  for (const t of ['intents', 'blocks', 'decode_failures']) addView(t, t);
  for (const group of ['events', 'swaps', 'instructions']) {
    const gdir = join(root, group); if (!existsSync(gdir)) continue;
    for (const p of readdirSync(gdir)) {
      const pdir = join(gdir, p);
      const subs = readdirSync(pdir);
      if (subs.some((s) => s.startsWith('date='))) addView(`${group}__${p}`, join(group, p));
      else for (const e of subs) addView(`${group}__${p}__${e}`, join(group, p, e));
    }
  }
  for (const v of views) await conn.run(v);
  const r = await conn.runAndReadAll(sql);
  console.table(r.getRowObjectsJson());
}
```

`src/commands/registryCheck.ts`:
```ts
import type { Registry } from '../registry.ts';
import type { RpcClient } from '../rpc.ts';

export async function registryCheck(registry: Registry, rpc: RpcClient): Promise<void> {
  let bad = 0;
  for (const [pid, v] of [...registry.tier1, ...registry.tier2]) {
    const exec = await rpc.getAccountExecutable(pid);
    const ok = exec === true;
    if (!ok) bad++;
    console.log(`${ok ? 'ok  ' : 'FAIL'} tier${v.tier} ${v.label.padEnd(28)} ${pid}${exec === null ? ' (no account)' : exec === false ? ' (not executable)' : ''}`);
  }
  console.log(bad === 0 ? 'all registry programs are executable on-chain' : `${bad} registry programs failed`);
  if (bad) process.exitCode = 1;
}
```

`src/commands/redecode.ts`:
```ts
import { join } from 'node:path';
import type { CollectorConfig } from '../config.ts';
import type { DecodeContext } from '../decoders/context.ts';
import { openDuck, rowsJson, q } from '../duck.ts';
import { decodeStruct } from '../borsh.ts';
import { jsonSafe, type Row } from '../rows.ts';
import { RangeWriter } from '../writer.ts';
import { schemaFor, eventTable } from '../tables.ts';
import { DECODER_VERSION } from '../version.ts';

// Re-runs tier 1 event decoding over decode_failures rows whose reason starts with "decode:" or is "unknown_discriminator",
// using the current IDLs. Recovered rows are written as <lo>-<hi>-redecode-<version>.parquet files; trader and
// outer_program are null because the transaction is not available offline (spec §5, redecode).
export async function redecode(cfg: CollectorConfig, ctx: DecodeContext, flags: Record<string, string>): Promise<void> {
  const conn = await openDuck(':memory:');
  const glob = join(cfg.dataRoot, 'decode_failures', '*', '*.parquet');
  const where = flags.from && flags.to ? `AND slot BETWEEN ${Number(flags.from)} AND ${Number(flags.to)}` : '';
  const rows = await rowsJson(conn, `SELECT signature, slot, block_time::VARCHAR AS block_time, tx_index, ix_path, program, reason, payload FROM read_parquet(${q(glob)}) WHERE (reason LIKE 'decode:%' OR reason = 'unknown_discriminator') AND payload IS NOT NULL ${where}`);
  const tables = new Map<string, Row[]>();
  let recovered = 0, still = 0;
  for (const r of rows) {
    const idl = ctx.idls.get(String(r.program));
    if (!idl) { still++; continue; }
    const payload = Buffer.from(String(r.payload), 'base64');
    const ev = idl.events.get(payload.subarray(0, 8).toString('hex'));
    // payload stored for a failed CPI/log event is the bytes after the discriminator; unknown_discriminator rows store disc+payload
    const body = ev ? payload.subarray(8) : payload;
    const ev2 = ev ?? [...idl.events.values()].find((e) => { try { decodeStruct(e.fields, body, idl.types); return true; } catch { return false; } });
    if (!ev2) { still++; continue; }
    try {
      const fields = jsonSafe(decodeStruct(ev2.fields, body, idl.types)) as Row;
      const row: Row = { signature: r.signature, slot: Number(r.slot), block_time: r.block_time, tx_index: Number(r.tx_index), ix_path: r.ix_path, event_ordinal: 0, outer_program: null, trader: null, ...fields };
      const t = eventTable(idl.label, ev2.name);
      const arr = tables.get(t); if (arr) arr.push(row); else tables.set(t, [row]);
      recovered++;
    } catch { still++; }
  }
  if (recovered) {
    const lo = Math.min(...rows.map((r) => Number(r.slot))), hi = Math.max(...rows.map((r) => Number(r.slot)));
    const writer = new RangeWriter({ dataRoot: cfg.dataRoot, conn, decoderVersion: DECODER_VERSION, schemaFor: (t) => schemaFor(t, ctx) });
    const files = await writer.writeRange(`redecode-${DECODER_VERSION}`, lo, hi, tables);
    console.log(`recovered ${recovered} rows into ${files.length} files; ${still} still failing`);
  } else console.log(`nothing recovered; ${still} still failing`);
}
```
Note: `writeRange` names files `<lo>-<hi>.parquet`; to keep redecode outputs distinct, add an optional `suffix` option to `RangeWriterOptions` (default `''`) and use `${lo}-${hi}${suffix}.parquet` in the writer; pass `suffix: '-redecode-' + DECODER_VERSION` here.

- [ ] **Step 4: Run tests and type check**

Run: `npm test && npm run check`
Expected: all pass.

- [ ] **Step 5: Write `Dockerfile`, `compose.yaml`, `README.md`, `docs/architecture.md`**

`Dockerfile`:
```dockerfile
FROM node:24-slim
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev
COPY src ./src
COPY config ./config
COPY idl ./idl
VOLUME ["/app/data", "/app/state"]
ENTRYPOINT ["node", "src/cli.ts"]
```

`compose.yaml`:
```yaml
services:
  tail:
    build: .
    command: ["tail"]
    env_file: .env
    volumes: ["./data:/app/data", "./state:/app/state"]
    restart: unless-stopped
  backfill:
    build: .
    command: ["backfill", "--until", "${BACKFILL_UNTIL:-2026-07-17}"]
    env_file: .env
    volumes: ["./data:/app/data", "./state:/app/state"]
    restart: unless-stopped
    depends_on: [tail]
```

`README.md` must contain: what the collector stores (one paragraph), setup (`cp .env.example .env`, `npm install`, `npm test`), running (`npm run fiber-data -- tail --max-ranges 1`, `docker compose up -d`), the table list with a one-line description each, a Mermaid flowchart of the pipeline (copy the §2 diagram from the spec), the entity diagram (copy the §4 `erDiagram` from the spec), and a pointer to the spec and `idl/SOURCES.md`.

`docs/architecture.md` must contain: the lane and range mechanics with the §5 sequence and state diagrams from the spec, the decoding flowchart, the trader rule verbatim from the spec, and the failure-handling table. Every Mermaid block in both files must render; check with the same headless-Chrome method used for the spec (see the scratchpad `mermaid_check.html` approach: load mermaid 11.4.1 from jsDelivr, call `mermaid.parse` on each block).

- [ ] **Step 6: Tiny live run on this machine**

With `.env` set (copy `RPC_URL` from `/root/solana/pumpdotfun/listenerJS/.env` into `SOLANA_RPC_URL`; never commit `.env`):

```bash
sed -i 's/"rangeSlots": 5000/"rangeSlots": 50/' config/collector.json   # temporary, revert after
npm run fiber-data -- registry check
npm run fiber-data -- tail --max-ranges 2
npm run fiber-data -- status
npm run fiber-data -- verify --lane tail
npm run fiber-data -- query --sql "SELECT count(*) AS n FROM events__meteora_dlmm__Swap2Evt"
npm run fiber-data -- query --sql "SELECT kind, count(*) FROM intents GROUP BY 1"
git checkout config/collector.json
```
Expected: `registry check` reports every tier 1 program executable (tier 2 failures for stale long-tail IDs are acceptable and should be moved to `kind: excluded` with reason `not executable on <date>` in `build-venues.ts`); `tail` commits two 50-slot ranges in under two minutes; `verify` reports 0 problems; the queries return non-zero counts. Record the measured bytes per slot from `du -sh data` in the README's sizing note.

- [ ] **Step 7: Commit**

```bash
git add src/commands Dockerfile compose.yaml README.md docs/architecture.md tests/verify.test.ts scripts config/venues.json
git commit -m "Operational commands, Docker, documentation with diagrams, live smoke test

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

## Self-review notes

- Spec coverage: §2 architecture (Tasks 8–11), §3 registry (Task 3), §4 data model incl. envelope, type mapping, tier 2 netting, intents, DLMM positions, blocks, decode_failures (Tasks 6–8), §5 mechanics incl. range atomicity, skipped slots, log attribution, dedupe, truncation, rate limiting, commands (Tasks 9–12), §6 infrastructure (Task 12), §7 error handling (Tasks 9–11), §8 testing (fixtures in every task), §10 open items: `registry check` and the CLMM IDL mismatch are exercised in Tasks 6 and 12.
- Review Focus items are pinned: 1 → Task 9 null block_time test; 2 → Task 5 unknown-mint test; 3 → Task 3 and Task 8 null-meta tests; 4 → Task 3 null stackHeight test; 5 → Task 10 retry test and Task 11 range test.
- Type consistency: `DecodeOutput` gains `stats` in Task 7; `decodeTier1` must use `newOutput()` after that change. `RangeWriterOptions.suffix` is added in Task 12 and is optional, so Tasks 9–11 are unaffected. Table labels for tier 2 come from `build-venues.ts` slugs; tests in Task 7 use `humidifi`, `tesserav`, `zerofi`.
- Known limitation carried from the spec: `redecode` cannot recompute `trader` or `outer_program` because transactions are not stored; recovered rows have those null.
