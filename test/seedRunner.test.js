'use strict';

/**
 * Unit tests for seedRunner.js logic with a mock connection (no ODBC required).
 */

const assert = require('assert');
const fs     = require('fs');
const os     = require('os');
const path   = require('path');

const { runSeedsUp, runSeedsDown } = require('../src/lib/migrations/seedRunner');

// ─── helpers ─────────────────────────────────────────────────────────────────

let passed = 0;
let failed = 0;

function test(name, fn) {
  const result = fn();
  if (result && typeof result.then === 'function') {
    return result.then(
      () => { console.log(`  ✅ ${name}`); passed++; },
      (err) => { console.error(`  ❌ ${name}`); console.error(`     ${err.message}`); failed++; }
    );
  }
  console.log(`  ✅ ${name}`);
  passed++;
  return Promise.resolve();
}

function makeMock(appliedFiles = []) {
  const execLog = [];
  return {
    execLog,
    execute: async (sql, params) => { execLog.push({ sql, params }); },
    query:   async ()            => ({ rows: appliedFiles.map(f => ({ FILENAME: f })) }),
  };
}

function writeFilePair(dir, name, upContent, downContent) {
  fs.writeFileSync(path.join(dir, `${name}.up.sql`),   upContent   || `-- up ${name}\n`);
  fs.writeFileSync(path.join(dir, `${name}.down.sql`), downContent || `-- down ${name}\n`);
}

// ─── runSeedsUp ───────────────────────────────────────────────────────────────

console.log('\nseedRunner — runSeedsUp');

const tests = [];

tests.push(test('applies all pending seed files', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'strsql-seed-up-'));
  writeFilePair(dir, '20240101000000_load', "INSERT INTO T VALUES (1);");
  const conn = makeMock([]);
  await runSeedsUp(conn, dir, 'SEED_LOG', '');
  const inserts = conn.execLog.filter(e => /INSERT INTO SEED_LOG/i.test(e.sql));
  assert.strictEqual(inserts.length, 1);
  fs.rmSync(dir, { recursive: true, force: true });
}));

tests.push(test('skips already-applied seed files', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'strsql-seed-up-'));
  writeFilePair(dir, '20240101000000_done', '-- already done');
  writeFilePair(dir, '20240102000000_new',  '-- to apply');
  const conn = makeMock(['20240101000000_done.up.sql']);
  await runSeedsUp(conn, dir, 'SEED_LOG', '');
  const inserts = conn.execLog.filter(e => /INSERT INTO SEED_LOG/i.test(e.sql));
  assert.strictEqual(inserts.length, 1);
  assert.deepStrictEqual(inserts[0].params, ['20240102000000_new.up.sql']);
  fs.rmSync(dir, { recursive: true, force: true });
}));

tests.push(test('executes multi-statement seed files', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'strsql-seed-up-'));
  writeFilePair(
    dir,
    '20240101000000_multi',
    "INSERT INTO T VALUES (1);\nINSERT INTO T VALUES (2);"
  );
  const conn = makeMock([]);
  await runSeedsUp(conn, dir, 'SEED_LOG', '');
  const inserts = conn.execLog.filter(e => /INSERT INTO T/i.test(e.sql));
  assert.strictEqual(inserts.length, 2);
  fs.rmSync(dir, { recursive: true, force: true });
}));

tests.push(test('uses schema when provided', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'strsql-seed-up-'));
  writeFilePair(dir, '20240101000000_s', '-- up');
  const conn = makeMock([]);
  await runSeedsUp(conn, dir, 'SEED_LOG', 'MYLIB');
  const inserts = conn.execLog.filter(e => /INSERT INTO MYLIB\.SEED_LOG/i.test(e.sql));
  assert.strictEqual(inserts.length, 1);
  fs.rmSync(dir, { recursive: true, force: true });
}));

tests.push(test('ignores table-already-exists error via odbcErrors', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'strsql-seed-up-'));
  writeFilePair(dir, '20240101000000_t', '-- up');
  const conn = makeMock([]);
  const originalExecute = conn.execute;
  let callCount = 0;
  conn.execute = async (sql, params) => {
    callCount++;
    if (/CREATE TABLE/i.test(sql)) {
      const err = new Error('[odbc] Error executing the sql statement');
      err.odbcErrors = [{ state: 'SQL0601', message: 'SEED_LOG already exists.' }];
      throw err;
    }
    return originalExecute(sql, params);
  };
  // should not throw
  await runSeedsUp(conn, dir, 'SEED_LOG', '');
  fs.rmSync(dir, { recursive: true, force: true });
}));

// ─── runSeedsDown ─────────────────────────────────────────────────────────────

console.log('\nseedRunner — runSeedsDown');

tests.push(test('rolls back last applied seed', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'strsql-seed-dn-'));
  writeFilePair(dir, '20240101000000_a', '-- up', 'DELETE FROM T WHERE id = 1;');
  writeFilePair(dir, '20240102000000_b', '-- up', 'DELETE FROM T WHERE id = 2;');
  const conn = makeMock([
    '20240101000000_a.up.sql',
    '20240102000000_b.up.sql',
  ]);
  await runSeedsDown(conn, dir, 'SEED_LOG', '');
  const del = conn.execLog.find(e => /DELETE FROM SEED_LOG/i.test(e.sql));
  assert.ok(del);
  assert.deepStrictEqual(del.params, ['20240102000000_b.up.sql']);
  fs.rmSync(dir, { recursive: true, force: true });
}));

tests.push(test('does nothing when no seeds applied', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'strsql-seed-dn-'));
  const conn = makeMock([]);
  await runSeedsDown(conn, dir, 'SEED_LOG', '');
  const dels = conn.execLog.filter(e => /DELETE FROM SEED_LOG/i.test(e.sql));
  assert.strictEqual(dels.length, 0);
  fs.rmSync(dir, { recursive: true, force: true });
}));

tests.push(test('continues gracefully when .down.sql is missing', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'strsql-seed-dn-'));
  // Only write .up.sql, no .down.sql
  fs.writeFileSync(path.join(dir, '20240101000000_nodown.up.sql'), '-- up only');
  const conn = makeMock(['20240101000000_nodown.up.sql']);
  // seedRunner warns but does not throw
  await runSeedsDown(conn, dir, 'SEED_LOG', '');
  fs.rmSync(dir, { recursive: true, force: true });
}));

// ─── await all + summary ──────────────────────────────────────────────────────

Promise.all(tests).then(() => {
  console.log(`\n  ${passed} passed, ${failed} failed\n`);
  if (failed > 0) process.exitCode = 1;
});
