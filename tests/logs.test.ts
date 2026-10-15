import { test } from 'node:test';
import assert from 'node:assert/strict';
import { parseLogs, logLinesFor } from '../src/logs.ts';
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

test('program log text that looks like invoke or success lines does not corrupt the stack', () => {
  const logs = [
    'Program A invoke [1]', 'Program log: Program B invoke [2]', 'Program log: this one ends with success',
    'Program log: Program X failed: nope', 'Program data: AQ==', 'Program A success',
    'Program E invoke [1]', 'Program data: Ag==', 'Program E success',
  ];
  const { events } = parseLogs(logs);
  assert.deepEqual(events.map((e) => [e.programId, e.path]), [['A', '0'], ['E', '1']]);
});

test('logLinesFor returns the lines of one instruction invocation', () => {
  const logs = ['Program A invoke [1]', 'Program log: hello', 'Program B invoke [2]', 'Program log: inner', 'Program B success', 'Program A consumed 1 of 2 compute units', 'Program A success'];
  assert.deepEqual(logLinesFor(logs, '0.1'), ['Program B invoke [2]', 'Program log: inner', 'Program B success']);
  assert.equal(logLinesFor(logs, '0').length, 7);
});
