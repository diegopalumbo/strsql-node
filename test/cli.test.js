'use strict';

/**
 * Tests for the CLI commands via child_process.spawnSync (no ODBC required).
 * Covers: --help outputs, profiles add/list/remove.
 */

const assert       = require('assert');
const { spawnSync } = require('child_process');
const fs            = require('fs');
const os            = require('os');
const path          = require('path');

const BIN = path.resolve(__dirname, '../bin/strsql.js');

// ─── helpers ─────────────────────────────────────────────────────────────────

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

function cli(...args) {
  const result = spawnSync(process.execPath, [BIN, ...args], {
    encoding: 'utf8',
    timeout: 10000,
  });
  return {
    stdout:   result.stdout || '',
    stderr:   result.stderr || '',
    status:   result.status,
    combined: (result.stdout || '') + (result.stderr || ''),
  };
}

// Isolate profile tests to a tmp dir so they don't touch ~/.strsql-node
const TMP_PROFILE_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'strsql-cli-'));
const TMP_PROFILES_FILE = path.join(TMP_PROFILE_DIR, 'profiles.json');

// Seed the tmp profiles file as empty JSON so the module doesn't try to read home
fs.writeFileSync(TMP_PROFILES_FILE, '{}', 'utf8');

function cliWithTmpProfiles(...args) {
  // We can't monkey-patch the running process, but we CAN point the env variable
  // that Node uses for home dir — if the module respects HOME/USERPROFILE.
  // strsql-node uses os.homedir(), so we override HOME.
  const result = spawnSync(process.execPath, [BIN, ...args], {
    encoding: 'utf8',
    timeout: 10000,
    env: { ...process.env, HOME: TMP_PROFILE_DIR },
  });
  return {
    stdout:   result.stdout || '',
    stderr:   result.stderr || '',
    status:   result.status,
    combined: (result.stdout || '') + (result.stderr || ''),
  };
}

// ─── help outputs ─────────────────────────────────────────────────────────────

console.log('\nCLI — --help outputs');

test('top-level --help exits 0', () => {
  const r = cli('--help');
  assert.strictEqual(r.status, 0, `exit code was ${r.status}\n${r.combined}`);
});

test('top-level --help mentions main commands', () => {
  const r = cli('--help');
  assert.ok(r.combined.includes('session'),    'missing "session"');
  assert.ok(r.combined.includes('migrations'), 'missing "migrations"');
  assert.ok(r.combined.includes('seeds'),      'missing "seeds"');
  assert.ok(r.combined.includes('profiles'),   'missing "profiles"');
});

test('migrations --help exits 0', () => {
  const r = cli('migrations', '--help');
  assert.strictEqual(r.status, 0);
});

test('migrations --help shows run and create subcommands', () => {
  const r = cli('migrations', '--help');
  assert.ok(r.combined.includes('run'),    'missing "run"');
  assert.ok(r.combined.includes('create'), 'missing "create"');
});

test('migrations run --help shows --host option', () => {
  const r = cli('migrations', 'run', '--help');
  assert.ok(r.combined.includes('-h') || r.combined.includes('--host'), 'missing --host/-h');
});

test('seeds --help exits 0', () => {
  const r = cli('seeds', '--help');
  assert.strictEqual(r.status, 0);
});

test('seeds --help shows run and create subcommands', () => {
  const r = cli('seeds', '--help');
  assert.ok(r.combined.includes('run'),    'missing "run"');
  assert.ok(r.combined.includes('create'), 'missing "create"');
});

test('seeds create --help shows --format option', () => {
  const r = cli('seeds', 'create', '--help');
  assert.ok(r.combined.includes('--format'), 'missing --format');
});

test('profiles --help exits 0', () => {
  const r = cli('profiles', '--help');
  assert.strictEqual(r.status, 0);
});

test('profiles add --help shows -h/--host', () => {
  const r = cli('profiles', 'add', '--help');
  assert.ok(r.combined.includes('--host') || r.combined.includes('-h'), 'missing --host');
});

test('profiles add --help shows -p/--password', () => {
  const r = cli('profiles', 'add', '--help');
  assert.ok(r.combined.includes('--password') || r.combined.includes('-p'), 'missing --password');
});

// ─── profiles add / list / remove ─────────────────────────────────────────────

console.log('\nCLI — profiles (isolated to tmp dir)');

test('profiles add creates a profile without error', () => {
  const r = cliWithTmpProfiles('profiles', 'add', 'test_profile',
    '-h', 'myibmi.example.com', '-u', 'TESTUSER');
  assert.strictEqual(r.status, 0, `non-zero exit: ${r.combined}`);
});

test('profiles list shows added profile', () => {
  const r = cliWithTmpProfiles('profiles', 'list');
  assert.ok(r.combined.includes('test_profile'), `expected "test_profile" in:\n${r.combined}`);
});

test('profiles list shows host', () => {
  const r = cliWithTmpProfiles('profiles', 'list');
  assert.ok(r.combined.includes('myibmi.example.com'), `host not found in:\n${r.combined}`);
});

test('profiles add with migration-table option succeeds', () => {
  const r = cliWithTmpProfiles('profiles', 'add', 'mig_profile',
    '-h', 'host2', '--migration-table', 'MYLIB.MIGLOG');
  assert.strictEqual(r.status, 0, `non-zero exit: ${r.combined}`);
});

test('profiles remove deletes a profile', () => {
  const r = cliWithTmpProfiles('profiles', 'remove', 'test_profile');
  assert.strictEqual(r.status, 0, `non-zero exit: ${r.combined}`);
});

test('profiles remove non-existent profile exits non-zero', () => {
  const r = cliWithTmpProfiles('profiles', 'remove', '__nonexistent__');
  assert.notStrictEqual(r.status, 0, 'expected non-zero exit for missing profile');
});

// ─── unknown commands ─────────────────────────────────────────────────────────

console.log('\nCLI — unknown/invalid inputs');

test('unknown command reports an error or shows help', () => {
  const r = cli('__totally_unknown__');
  // Commander v11 may exit 0 but must print something (error to stderr or help to stdout)
  const hasOutput = r.combined.trim().length > 0;
  assert.ok(hasOutput, 'expected some output for unknown command');
});

// ─── cleanup + summary ───────────────────────────────────────────────────────

fs.rmSync(TMP_PROFILE_DIR, { recursive: true, force: true });

console.log(`\n  ${passed} passed, ${failed} failed\n`);
if (failed > 0) process.exitCode = 1;
