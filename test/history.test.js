'use strict';

/**
 * Unit tests for HistoryManager (no ODBC connection required).
 */

const assert = require('assert');
const fs     = require('fs');
const os     = require('os');
const path   = require('path');

// ─── isolate from real ~/.strsql-node ────────────────────────────────────────

const TMP_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'strsql-history-'));
const TMP_FILE = path.join(TMP_DIR, 'history.json');

const { HistoryManager } = require('../src/lib/history');

// Patch I/O to tmp dir
function makeHistory() {
  const h = new HistoryManager(50);
  h._load  = () => { try { return JSON.parse(fs.readFileSync(TMP_FILE, 'utf8')); } catch { return []; } };
  h._save  = () => fs.writeFileSync(TMP_FILE, JSON.stringify(h._entries.slice(-500), null, 2));
  h._entries = h._load();
  return h;
}

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

console.log('\nHistoryManager');

test('add + all roundtrip', () => {
  const h = makeHistory();
  h.add('SELECT 1 FROM SYSIBM.SYSDUMMY1');
  h.add('SELECT 2 FROM SYSIBM.SYSDUMMY1');
  assert.strictEqual(h.all().length, 2);
  assert.strictEqual(h.all()[0], 'SELECT 1 FROM SYSIBM.SYSDUMMY1');
});

test('add deduplicates consecutive entries', () => {
  const h = makeHistory();
  h.add('SELECT 1');
  h.add('SELECT 1');
  assert.strictEqual(h.all().filter(e => e === 'SELECT 1').length, 1);
});

test('add allows same entry non-consecutively', () => {
  const h = makeHistory();
  h.add('SELECT 1');
  h.add('SELECT 2');
  h.add('SELECT 1');
  assert.strictEqual(h.all().filter(e => e === 'SELECT 1').length, 2);
});

test('add ignores empty/whitespace input', () => {
  const h = makeHistory();
  const before = h.all().length;
  h.add('');
  h.add('   ');
  assert.strictEqual(h.all().length, before);
});

test('forReadline returns most-recent first', () => {
  const h = makeHistory();
  h.add('A');
  h.add('B');
  h.add('C');
  const rl = h.forReadline();
  assert.strictEqual(rl[0], 'C');
  assert.strictEqual(rl[1], 'B');
  assert.strictEqual(rl[2], 'A');
});

test('search finds matching entries', () => {
  const h = makeHistory();
  h.add('SELECT * FROM ORDERS');
  h.add('SELECT * FROM CUSTOMERS');
  h.add('INSERT INTO ORDERS VALUES (1)');
  const results = h.search('orders');
  assert.ok(results.length >= 2);
  assert.ok(results.every(e => e.toLowerCase().includes('orders')));
});

test('search is case-insensitive', () => {
  const h = makeHistory();
  h.add('select * from qsys2.systables');
  const results = h.search('QSYS2');
  assert.ok(results.length >= 1);
});

test('search returns empty array for no match', () => {
  const h = makeHistory();
  assert.deepStrictEqual(h.search('__NOTFOUND__'), []);
});

test('clear empties history', () => {
  const h = makeHistory();
  h.add('SOMETHING');
  h.clear();
  assert.strictEqual(h.all().length, 0);
});

// ─── cleanup + summary ───────────────────────────────────────────────────────

fs.rmSync(TMP_DIR, { recursive: true, force: true });

console.log(`\n  ${passed} passed, ${failed} failed\n`);
if (failed > 0) process.exitCode = 1;
