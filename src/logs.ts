export interface LogEvent { programId: string; path: string; kind: 'data' | 'raylog'; payload: Uint8Array }

// A program id never contains ':' or whitespace, which excludes `Program log:` / `Program data:` / `Program return:` lines.
const INVOKE = /^Program ([^\s:]+) invoke \[(\d+)\]$/;
const END = /^Program [^\s:]+ (success$|failed: )/;

/** The log lines of one instruction invocation (its invoke line through its success/failed line), by path. */
export function logLinesFor(logs: string[], path: string): string[] {
  const counters: number[] = [];
  let depthOfTarget = -1; const out: string[] = [];
  for (const line of logs) {
    const inv = INVOKE.exec(line);
    if (inv) {
      const depth = Number(inv[2]);
      counters.length = Math.min(counters.length, depth);
      while (counters.length < depth) counters.push(0);
      counters[depth - 1]!++;
      const p = depth === 1 ? String(counters[0]! - 1) : `${counters[0]! - 1}.${counters.slice(1).join('.')}`;
      if (depthOfTarget < 0 && p === path) depthOfTarget = depth;
    }
    if (depthOfTarget >= 0) out.push(line);
    if (depthOfTarget >= 0 && END.test(line) && counters.length === depthOfTarget) return out;
  }
  return out;
}

export function parseLogs(logs: string[]): { events: LogEvent[]; truncated: boolean } {
  const events: LogEvent[] = [];
  const stack: { programId: string; path: string }[] = [];
  const counters: number[] = [];
  let truncated = false;
  for (const line of logs) {
    const inv = INVOKE.exec(line);
    if (inv) {
      const programId = inv[1]!;
      const depth = Number(inv[2]);
      counters.length = Math.min(counters.length, depth);
      while (counters.length < depth) counters.push(0);
      counters[depth - 1]!++;
      const path = depth === 1 ? String(counters[0]! - 1) : `${counters[0]! - 1}.${counters.slice(1).join('.')}`;
      stack.push({ programId, path });
    } else if (END.test(line)) {
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
