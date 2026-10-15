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
