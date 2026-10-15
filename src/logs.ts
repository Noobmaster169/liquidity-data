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
