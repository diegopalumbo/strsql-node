'use strict';

/**
 * Unit tests for ProfileManager (no ODBC connection required).
 */

const assert = require('assert');
const fs     = require('fs');
const os     = require('os');
const path   = require('path');

// ─── isolate from real ~/.strsql-node ────────────────────────────────────────

const TMP_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'strsql-profiles-'));
const TMP_FILE = path.join(TMP_DIR, 'profiles.json');

// Patch the module before requiring it
const profilesMod = path.resolve(__dirname, '../src/lib/profiles.js');
// We override PROFILES_DIR/FILE by monkey-patching after require
const { ProfileManager } = require(profilesMod);

// Redirect file I/O to tmp dir
const pm = new ProfileManager();
pm._ensureDir = () => { if (!fs.existsSync(TMP_DIR)) fs.mkdirSync(TMP_DIR, { recursive: true }); };
pm._load = () => {
  if (!fs.existsSync(TMP_FILE)) return {};
  return JSON.parse(fs.readFileSync(TMP_FILE, 'utf8'));
};
pm._save = (profiles) => fs.writeFileSync(TMP_FILE, JSON.stringify(profiles, null, 2), 'utf8');

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

// ─── tests ───────────────────────────────────────────────────────────────────

console.log('\nProfileManager');

test('set + get roundtrip', () => {
  pm.set('t1', { host: 'myhost', username: 'BOB', defaultSchema: 'MYLIB' });
  const p = pm.get('t1');
  assert.strictEqual(p.host, 'myhost');
  assert.strictEqual(p.username, 'BOB');
  assert.strictEqual(p.defaultSchema, 'MYLIB');
});

test('set + list includes entry', () => {
  pm.set('t2', { host: 'other', username: 'ALICE' });
  const list = pm.list();
  const names = list.map(x => x.name);
  assert.ok(names.includes('t1'));
  assert.ok(names.includes('t2'));
});

test('exists returns true for saved profile', () => {
  assert.ok(pm.exists('t1'));
});

test('exists returns false for unknown profile', () => {
  assert.ok(!pm.exists('__never__'));
});

test('remove deletes profile', () => {
  pm.set('toremove', { host: 'x' });
  pm.remove('toremove');
  assert.ok(!pm.exists('toremove'));
});

test('remove throws for unknown profile', () => {
  assert.throws(() => pm.remove('__never__'), /not found/i);
});

test('resolve merges profile + ENV override', () => {
  pm.set('env_test', { host: 'profile_host', username: 'PROFILE_USER', defaultSchema: 'PFL' });
  const prev = process.env.STRSQL_HOST;
  process.env.STRSQL_HOST = 'env_host';
  const resolved = pm.resolve('env_test');
  assert.strictEqual(resolved.host, 'env_host');      // ENV wins
  assert.strictEqual(resolved.username, 'PROFILE_USER'); // profile kept
  if (prev === undefined) delete process.env.STRSQL_HOST;
  else process.env.STRSQL_HOST = prev;
});

test('resolve without name falls back to ENV', () => {
  delete process.env.STRSQL_HOST;
  const resolved = pm.resolve(null);
  assert.strictEqual(resolved.host, undefined);
});

test('set stores migrationTable and seedTable', () => {
  pm.set('mig', { host: 'h', migrationTable: 'MYLIB.MIGRALOG', seedTable: 'MYLIB.SEEDLOG' });
  const p = pm.get('mig');
  assert.strictEqual(p.migrationTable, 'MYLIB.MIGRALOG');
  assert.strictEqual(p.seedTable, 'MYLIB.SEEDLOG');
});

test('get throws for unknown profile', () => {
  assert.throws(() => pm.get('__never__'), /not found/i);
});

// ─── cleanup + summary ───────────────────────────────────────────────────────

fs.rmSync(TMP_DIR, { recursive: true, force: true });

console.log(`\n  ${passed} passed, ${failed} failed\n`);
if (failed > 0) process.exitCode = 1;
