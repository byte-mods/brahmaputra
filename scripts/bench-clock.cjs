#!/usr/bin/env node
'use strict';
// A system-clock correction must not turn a two-second workload into five
// minutes (or a negative duration). hrtime uses the OS monotonic clock;
// separate invocations on the same host share its arbitrary epoch.
process.stdout.write(`${process.hrtime.bigint() / 1000000n}\n`);
