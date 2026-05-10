#!/usr/bin/env node
'use strict';

/**
 * Test runner for strsql-node.
 * Runs all test files sequentially and exits with code 1 if any fail.
 *
 * Usage:
 *   node test/run-tests.js          # all tests
 *   node test/run-tests.js --unit   # unit tests only (no CLI spawns)
 *   node test/run-tests.js --cli    # CLI integration tests only
 */

const { spawnSync } = require('child_process');
const path = require('path');

const UNIT_TESTS = [
  'profiles.test.js',
  'history.test.js',
  'create.test.js',
  'formatter.test.js',
  'splitArgs.test.js',
  'migrate.test.js',
  'seedRunner.test.js',
];

const CLI_TESTS = [
  'cli.test.js',
];

const arg = process.argv[2];
let files;
if (arg === '--unit') {
  files = UNIT_TESTS;
} else if (arg === '--cli') {
  files = CLI_TESTS;
} else {
  files = [...UNIT_TESTS, ...CLI_TESTS];
}

let totalPassed = 0;
let totalFailed = 0;

for (const file of files) {
  const result = spawnSync(process.execPath, [path.join(__dirname, file)], {
    stdio: 'inherit',
    encoding: 'utf8',
  });

  if (result.status !== 0) {
    totalFailed++;
  } else {
    totalPassed++;
  }
}

const total = totalPassed + totalFailed;
console.log('─'.repeat(50));
if (totalFailed === 0) {
  console.log(`\n✅  All ${total} test file${total !== 1 ? 's' : ''} passed.\n`);
} else {
  console.log(`\n❌  ${totalFailed} of ${total} test file${total !== 1 ? 's' : ''} failed.\n`);
  process.exitCode = 1;
}
