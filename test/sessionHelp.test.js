'use strict';

const assert = require('assert');
const { STRSQLSession } = require('../src/cli/session');

let passed = 0;
let failed = 0;

function test(name, fn) {
  try {
    fn();
    console.log(`  ✅ ${name}`);
    passed++;
  } catch (err) {
    console.error(`  ❌ ${name}`);
    console.error(`     ${err.message}`);
    failed++;
  }
}

function captureConsole(fn) {
  const original = console.log;
  const lines = [];
  console.log = (value = '') => lines.push(String(value));
  try {
    fn();
  } finally {
    console.log = original;
  }
  return lines.join('\n');
}

console.log('\nsession command help');

test('export help explains format-specific parameters', () => {
  const session = new STRSQLSession();
  const output = captureConsole(() => session._printCommandHelp('export'));
  assert.ok(output.includes('\\export <file.csv>'), 'missing CSV usage');
  assert.ok(output.includes('CSV/JSON export needs only an output file'), 'missing CSV/JSON note');
  assert.ok(output.includes('<file.merge.sql>'), 'missing MERGE usage');
  assert.ok(output.includes('--keys'), 'missing MERGE keys option');
});

test('pipe help shows source and target options', () => {
  const session = new STRSQLSession();
  const output = captureConsole(() => session._printCommandHelp('pipe'));
  assert.ok(output.includes('\\pipe <src-table>'), 'missing source table usage');
  assert.ok(output.includes('--target-profile'), 'missing target profile option');
  assert.ok(output.includes('--target-host'), 'missing target host option');
  assert.ok(output.includes('--sql'), 'missing SQL source option');
});

test('subcommand help supports migrations run', () => {
  const session = new STRSQLSession();
  const output = captureConsole(() => session._printCommandHelp('migrations', ['run']));
  assert.ok(output.includes('\\migrations run'), 'missing migrations run usage');
  assert.ok(output.includes('--migration-table'), 'missing migration table option');
});

console.log(`\n  ${passed} passed, ${failed} failed\n`);
if (failed > 0) process.exitCode = 1;
