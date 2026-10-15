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
