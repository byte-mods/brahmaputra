'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { parseSamples, summarize, validatePhaseWindow } = require('./bench-resource-summary.cjs');
const MiB = 1048576;
const header = 'uptime_seconds,cpu_usage_usec,memory_working_set_bytes,memory_total_bytes\n';

test('clock discontinuities cannot publish a workload outside its sampling window', () => {
  validatePhaseWindow({ elapsedSeconds: 5.15 }, 3270);
  assert.throws(() => validatePhaseWindow({ elapsedSeconds: 5.15 }, 310270));
  assert.throws(() => validatePhaseWindow({ elapsedSeconds: 5.15 }, -100));
  assert.throws(() => validatePhaseWindow({ elapsedSeconds: 5.15 }, NaN));
});

test('subsecond cumulative counters retain CPU work and weight memory by time', () => {
  const result = summarize([[
    [10, 100000, 10 * MiB, 30 * MiB],
    [10.1, 300000, 20 * MiB, 30 * MiB],
    [10.4, 600000, 20 * MiB, 30 * MiB],
  ]]);
  assert.ok(Math.abs(result.cpuSeconds - 0.5) < 1e-9);
  assert.ok(Math.abs(result.averageCpuPercent - 125) < 1e-9);
  assert.ok(Math.abs(result.peakCpuPercent - 200) < 1e-9);
  assert.ok(Math.abs(result.averageMemoryMiB - 18.75) < 1e-9);
  assert.equal(result.peakMemoryMiB, 20);
});

test('cluster aggregation uses the overlapping window and simultaneous memory', () => {
  const result = summarize([
    [[0, 0, 10 * MiB, 20 * MiB], [2, 2000000, 20 * MiB, 20 * MiB]],
    [[1, 4000000, 20 * MiB, 20 * MiB], [3, 8000000, 10 * MiB, 20 * MiB]],
  ]);
  assert.equal(result.elapsedSeconds, 1);
  assert.equal(result.cpuSeconds, 3);
  assert.equal(result.averageCpuPercent, 300);
  assert.equal(result.peakCpuPercent, 300);
  assert.equal(result.averageMemoryMiB, 35);
  assert.equal(result.peakMemoryMiB, 35); // not the sum of non-simultaneous peaks
});

test('invalid, restarted, incomplete and non-overlapping samples are rejected', () => {
  for (const text of [
    header, header + '1,3,2,1\n2,4,2,3',
    header + '1,3,1,2\n2,2,1,2', header + '2,3,1,2\n1,4,1,2',
    header + '1,3,1,2\n2,NaN,1,2', header + '1,3,1,2\n2,4,1',
    header + '1,3,1,2\n2,4,,2',
  ]) assert.throws(() => parseSamples(text));
  assert.throws(() => summarize([]));
  assert.throws(() => summarize([[[0, 0, 1, 2], [1, 1, 1, 2]], [[2, 1, 1, 2], [3, 2, 1, 2]]]));
  assert.equal(parseSamples(header + '1,3,1,2\n1,4,1,2\n2,5,1,2').length, 2);
});
