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
