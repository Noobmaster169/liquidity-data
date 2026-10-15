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
