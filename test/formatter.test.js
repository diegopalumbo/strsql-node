'use strict';

/**
 * Unit tests for formatter functions (no ODBC connection required).
 * Covers: toCSV, toJSON, toInsert, toMerge, formatTable
 */

const assert = require('assert');
const { toCSV, toJSON, toInsert, toMerge, formatTable } = require('../src/lib/formatter');

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

function makeResult(cols, rows) {
  return {
    columns:   cols.map(n => ({ name: n })),
    rows:      rows.map(r => {
      const obj = {};
      cols.forEach((c, i) => { obj[c] = r[i]; });
      return obj;
    }),
    rowCount:  rows.length,
    elapsed:   0,
    statement: 'SELECT ...',
  };
}

// ─── toCSV ───────────────────────────────────────────────────────────────────

console.log('\nformatter — toCSV');

test('produces header + data rows', () => {
  const res = makeResult(['ID', 'NAME'], [[1, 'Alice'], [2, 'Bob']]);
  const csv = toCSV(res);
  const lines = csv.split('\r\n');
  assert.strictEqual(lines[0], 'ID,NAME');
  assert.ok(lines[1].includes('Alice'));
  assert.ok(lines[2].includes('Bob'));
});

test('returns empty string for empty columns', () => {
  const res = { columns: [], rows: [], rowCount: 0, elapsed: 0 };
  assert.strictEqual(toCSV(res), '');
});

test('NULL values produce empty field', () => {
  const res = makeResult(['A', 'B'], [[null, 'x'], [undefined, 'y']]);
  const csv = toCSV(res);
  const lines = csv.split('\r\n');
  assert.ok(lines[1].startsWith(','));
});

test('values with commas are quoted', () => {
  const res = makeResult(['V'], [['hello, world']]);
  const csv = toCSV(res);
  assert.ok(csv.includes('"hello, world"'));
});

// ─── toJSON ──────────────────────────────────────────────────────────────────

console.log('\nformatter — toJSON');

test('produces valid JSON', () => {
  const res = makeResult(['X'], [[42]]);
  const parsed = JSON.parse(toJSON(res));
  assert.strictEqual(parsed.rows[0].X, 42);
});

test('contains rowCount and columns metadata', () => {
  const res = makeResult(['A', 'B'], [[1, 2]]);
  const parsed = JSON.parse(toJSON(res));
  assert.strictEqual(parsed.rowCount, 1);
  assert.deepStrictEqual(parsed.columns, ['A', 'B']);
});

// ─── toInsert ────────────────────────────────────────────────────────────────

console.log('\nformatter — toInsert');

test('generates INSERT statement for single row', () => {
  const res = makeResult(['ID', 'NAME'], [[1, 'Alice']]);
  const sql = toInsert(res, { table: 'MYLIB.ORDERS', batch: 1 });
  assert.ok(sql.includes('INSERT INTO MYLIB.ORDERS'));
  assert.ok(sql.includes('Alice'));
});

test('batches multiple rows', () => {
  const rows = [[1, 'A'], [2, 'B'], [3, 'C']];
  const res = makeResult(['ID', 'NAME'], rows);
  const sql = toInsert(res, { table: 'T', batch: 2 });
  // 2 INSERT blocks: one for rows 1-2, one for row 3
  const insertCount = (sql.match(/INSERT INTO/g) || []).length;
  assert.strictEqual(insertCount, 2);
});

test('returns empty string for no columns', () => {
  const res = { columns: [], rows: [], rowCount: 0, elapsed: 0 };
  assert.strictEqual(toInsert(res, { table: 'T' }), '');
});

test('NULL values produce NULL literal', () => {
  const res = makeResult(['A'], [[null]]);
  const sql = toInsert(res, { table: 'T', batch: 1 });
  assert.ok(sql.toUpperCase().includes('NULL'));
});

// ─── toMerge ─────────────────────────────────────────────────────────────────

console.log('\nformatter — toMerge');

test('generates MERGE statement', () => {
  const res = makeResult(['ID', 'VAL'], [[1, 'x']]);
  const sql = toMerge(res, { table: 'T', keys: ['ID'], dialect: 'ibmi' });
  assert.ok(sql.toUpperCase().includes('MERGE'));
  assert.ok(sql.includes('T'));
});

test('throws when keys is empty', () => {
  const res = makeResult(['ID'], [[1]]);
  assert.throws(() => toMerge(res, { table: 'T', keys: [] }), /keys/i);
});

test('throws when key column not in result', () => {
  const res = makeResult(['ID'], [[1]]);
  assert.throws(() => toMerge(res, { table: 'T', keys: ['MISSING'] }), /not found/i);
});

test('returns empty string for no columns', () => {
  const res = { columns: [], rows: [], rowCount: 0, elapsed: 0 };
  assert.strictEqual(toMerge(res, { table: 'T', keys: [] }), '');
});

// ─── formatTable ─────────────────────────────────────────────────────────────

console.log('\nformatter — formatTable');

test('renders table string with column header', () => {
  const res = makeResult(['ID', 'NAME'], [[1, 'Alice']]);
  const out = formatTable(res, { plain: true, asciiBorders: true });
  assert.ok(out.includes('ID'));
  assert.ok(out.includes('NAME'));
  assert.ok(out.includes('Alice'));
});

test('shows rowCount in footer', () => {
  const res = makeResult(['X'], [[1], [2], [3]]);
  const out = formatTable(res, { plain: true, asciiBorders: true });
  assert.ok(out.includes('3 rows'));
});

test('returns message for empty columns result', () => {
  const res = { columns: [], rows: [], rowCount: 0, elapsed: 5 };
  const out = formatTable(res, { plain: true });
  assert.ok(out.includes('0 rows'));
});

test('truncates long cell values', () => {
  const long = 'A'.repeat(200);
  const res = makeResult(['COL'], [[long]]);
  const out = formatTable(res, { plain: true, asciiBorders: true, maxCellWidth: 10 });
  assert.ok(!out.includes(long)); // must have been truncated
});

// ─── summary ─────────────────────────────────────────────────────────────────

console.log(`\n  ${passed} passed, ${failed} failed\n`);
if (failed > 0) process.exitCode = 1;
