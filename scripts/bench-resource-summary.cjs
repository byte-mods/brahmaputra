#!/usr/bin/env node
'use strict';
const fs = require('node:fs');
const path = require('node:path');

function parseSamples(text) {
  const lines = text.trim().split(/\r?\n/);
  if (lines.shift() !== 'uptime_seconds,cpu_usage_usec,memory_working_set_bytes,memory_total_bytes') {
    throw new Error('unexpected cgroup sample header');
  }
  const samples = [];
  for (const line of lines) {
    const fields = line.split(',');
    const row = fields.map(Number);
    if (row.length !== 4 || fields.some(value => !/^\d+(?:\.\d+)?$/.test(value)) ||
        row.some(value => !Number.isFinite(value) || value < 0) || row[2] > row[3]) {
      throw new Error('invalid cgroup sample');
    }
    const previous = samples.at(-1);
    if (previous && (row[0] < previous[0] || row[1] < previous[1])) {
      throw new Error('cgroup clock or CPU counter moved backwards');
    }
    if (previous && row[0] === previous[0]) samples[samples.length - 1] = row;
    else samples.push(row);
  }
  if (samples.length < 2) throw new Error('at least two distinct sample times are required');
  return samples;
}

function summarize(series) {
  if (!series.length) throw new Error('no containers sampled');
  const start = Math.max(...series.map(samples => samples[0][0]));
  const end = Math.min(...series.map(samples => samples.at(-1)[0]));
  if (end <= start) throw new Error('container sampling windows do not overlap');
  // Every sampler starts before the workload and stops after it. Their common
  // window covers the complete workload without summing mismatched durations.
  const at = (samples, time, column) => {
    let low = 0, high = samples.length - 1;
    while (high - low > 1) {
      const middle = (low + high) >> 1;
      if (samples[middle][0] <= time) low = middle;
      else high = middle;
    }
    const a = samples[low], b = samples[high];
    return a[column] + (b[column] - a[column]) * (time - a[0]) / (b[0] - a[0]);
  };
  const totalAt = (time, column) => series.reduce((sum, samples) => sum + at(samples, time, column), 0);
  const times = [...new Set([start, end, ...series.flatMap(samples => samples.map(row => row[0]))])]
    .filter(time => time >= start && time <= end).sort((a, b) => a - b);
  const cpuSeconds = (totalAt(end, 1) - totalAt(start, 1)) / 1e6;
  let memoryArea = 0, peakMemory = totalAt(start, 2), peakCpu = 0;
  for (let i = 1; i < times.length; i++) {
    const a = times[i - 1], b = times[i], elapsed = b - a;
    const memoryA = totalAt(a, 2), memoryB = totalAt(b, 2);
    memoryArea += (memoryA + memoryB) / 2 * elapsed;
    peakMemory = Math.max(peakMemory, memoryA, memoryB);
    peakCpu = Math.max(peakCpu, (totalAt(b, 1) - totalAt(a, 1)) / 1e6 / elapsed * 100);
  }
  return {
    method: 'cgroup-v2-common-window-linear-interpolation',
    containers: series.length,
    sampleCounts: series.map(samples => samples.length),
    startUptimeSeconds: start,
    endUptimeSeconds: end,
    elapsedSeconds: end - start,
    cpuSeconds,
    averageCpuPercent: cpuSeconds / (end - start) * 100,
    peakCpuPercent: peakCpu,
    averageMemoryMiB: memoryArea / (end - start) / 1048576,
    peakMemoryMiB: peakMemory / 1048576,
  };
}

function validatePhaseWindow(result, elapsedMs) {
  if (!Number.isFinite(elapsedMs) || elapsedMs <= 0 ||
      !Number.isFinite(result.elapsedSeconds) || result.elapsedSeconds <= 0 ||
      elapsedMs > result.elapsedSeconds * 1000 + 250) {
    throw new Error('phase duration is outside its resource observation window; host timing is invalid');
  }
}

if (require.main === module) {
  try {
    const [output, ...inputs] = process.argv.slice(2);
    if (output === '--check-window') {
      if (inputs.length !== 2) throw new Error('expected a resource summary and elapsed milliseconds');
      validatePhaseWindow(JSON.parse(fs.readFileSync(inputs[0], 'utf8')), Number(inputs[1]));
      process.exit(0);
    }
    if (output === '--report') {
      if (inputs.length !== 1) throw new Error('expected a resource report directory');
      const rows = fs.readdirSync(inputs[0]).filter(file => file.endsWith('.resources.json')).sort();
      process.stdout.write('\n| Phase artifact | Sampling window (s) | CPU time (core-seconds) | Average cores |\n|---|---:|---:|---:|\n');
      for (const file of rows) {
        const result = JSON.parse(fs.readFileSync(path.join(inputs[0], file), 'utf8'));
        process.stdout.write(`| ${file.replace('.resources.json', '')} | ${result.elapsedSeconds.toFixed(3)} | ${result.cpuSeconds.toFixed(3)} | ${(result.averageCpuPercent / 100).toFixed(3)} |\n`);
      }
      process.exit(0);
    }
    if (!output || !inputs.length) throw new Error('usage: bench-resource-summary.cjs output.json samples.csv [...]');
    const result = summarize(inputs.map(file => parseSamples(fs.readFileSync(file, 'utf8'))));
    fs.writeFileSync(output, `${JSON.stringify(result, null, 2)}\n`);
    process.stdout.write([
      result.averageCpuPercent.toFixed(1), result.peakCpuPercent.toFixed(1),
      result.averageMemoryMiB.toFixed(1), result.peakMemoryMiB.toFixed(1),
    ].join(' ') + '\n');
  } catch (error) {
    process.stderr.write(`Resource measurement failed: ${error.message}\n`);
    process.exitCode = 1;
  }
}
module.exports = { parseSamples, summarize, validatePhaseWindow };
