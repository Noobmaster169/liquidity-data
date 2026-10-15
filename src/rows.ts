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
