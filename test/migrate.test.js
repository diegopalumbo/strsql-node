'use strict';

/**
 * Unit tests for migrate.js logic with a mock connection (no ODBC required).
 */

const assert = require('assert');
const fs     = require('fs');
const os     = require('os');
const path   = require('path');

const {
  ensureMigrationTable,
  getAppliedMigrations,
  applyMigration,
  rollbackMigration,
  runUp,
  runDown,
} = require('../src/lib/migrations/migrate');

// ─── helpers ─────────────────────────────────────────────────────────────────

let passed = 0;
let failed = 0;

function test(name, fn) {
  try {
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
  } catch (err) {
    console.error(`  ❌ ${name}`);
    console.error(`     ${err.message}`);
    failed++;
    return Promise.resolve();
  }
}

/**
 * Create a minimal mock connection.
 * @param {string[]} appliedFiles  Files that conn.query() will report as applied.
 */
function makeMock(appliedFiles = []) {
  const execLog = [];
  const conn = {
    execLog,
    execute: async (sql, params) => {
      execLog.push({ sql, params });
    },
    query: async (sql) => ({
      rows: appliedFiles.map(f => ({ FILENAME: f })),
    }),
  };
  return conn;
}

/** Write a .up.sql + .down.sql pair to a directory. */
function writeFilePair(dir, name, upContent, downContent) {
  fs.writeFileSync(path.join(dir, `${name}.up.sql`),   upContent   || `-- up ${name}\n`);
  fs.writeFileSync(path.join(dir, `${name}.down.sql`), downContent || `-- down ${name}\n`);
}

// ─── ensureMigrationTable ─────────────────────────────────────────────────────

console.log('\nmigrate — ensureMigrationTable');

const tests = [];

tests.push(test('creates table when it does not exist', async () => {
  const conn = makeMock();
  await ensureMigrationTable(conn, 'MIGRATION_LOG', 'MYLIB');
  assert.ok(conn.execLog.some(e => /CREATE TABLE MYLIB\.MIGRATION_LOG/i.test(e.sql)));
}));

tests.push(test('ignores "already exists" error via err.message', async () => {
  const conn = makeMock();
  conn.execute = async (sql) => {
    if (/CREATE TABLE/i.test(sql)) {
      const err = new Error('Table already exists');
      throw err;
    }
  };
  // Should not throw
  await ensureMigrationTable(conn, 'MIGRATION_LOG', '');
}));

tests.push(test('ignores SQL0601 via odbcErrors array', async () => {
  const conn = makeMock();
  conn.execute = async (sql) => {
    if (/CREATE TABLE/i.test(sql)) {
      const err = new Error('[odbc] Error executing the sql statement');
      err.odbcErrors = [{ state: '42S01', message: 'MIGRATION_LOG already exists in MYLIB' }];
      throw err;
    }
  };
  await ensureMigrationTable(conn, 'MIGRATION_LOG', 'MYLIB');
}));

tests.push(test('re-throws unexpected errors', async () => {
  const conn = makeMock();
  conn.execute = async (sql) => {
    if (/CREATE TABLE/i.test(sql)) throw new Error('unexpected DB error');
  };
  await assert.rejects(() => ensureMigrationTable(conn, 'T', ''), /unexpected DB error/);
}));

// ─── getAppliedMigrations ─────────────────────────────────────────────────────

console.log('\nmigrate — getAppliedMigrations');

tests.push(test('returns list from FILENAME column', async () => {
  const conn = makeMock(['20240101000000_a.up.sql', '20240102000000_b.up.sql']);
  const applied = await getAppliedMigrations(conn, 'MIGRATION_LOG', '');
  assert.deepStrictEqual(applied, ['20240101000000_a.up.sql', '20240102000000_b.up.sql']);
}));

tests.push(test('returns empty array when no rows', async () => {
  const conn = makeMock([]);
  const applied = await getAppliedMigrations(conn, 'MIGRATION_LOG', '');
  assert.deepStrictEqual(applied, []);
}));

// ─── applyMigration ───────────────────────────────────────────────────────────

console.log('\nmigrate — applyMigration');

tests.push(test('executes SQL statements in file', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'strsql-mg-apply-'));
  const file = '20240101000000_x.up.sql';
  fs.writeFileSync(path.join(dir, file), 'CREATE TABLE T1 (ID INT);\nCREATE TABLE T2 (ID INT);');
  const conn = makeMock();
  await applyMigration(conn, dir, 'MIGRATION_LOG', '', file);
  const ddls = conn.execLog.filter(e => /CREATE TABLE/i.test(e.sql));
  assert.strictEqual(ddls.length, 2);
  fs.rmSync(dir, { recursive: true, force: true });
}));

tests.push(test('inserts filename into tracking table', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'strsql-mg-apply-'));
  const file = '20240101000000_y.up.sql';
  fs.writeFileSync(path.join(dir, file), 'SELECT 1 FROM SYSIBM.SYSDUMMY1');
  const conn = makeMock();
  await applyMigration(conn, dir, 'MIGRATION_LOG', 'LIB', file);
  const insertCall = conn.execLog.find(e => /INSERT INTO LIB\.MIGRATION_LOG/i.test(e.sql));
  assert.ok(insertCall);
  assert.deepStrictEqual(insertCall.params, [file]);
  fs.rmSync(dir, { recursive: true, force: true });
}));

// ─── rollbackMigration ────────────────────────────────────────────────────────

console.log('\nmigrate — rollbackMigration');

tests.push(test('executes .down.sql statements', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'strsql-mg-rb-'));
  const upFile = '20240101000000_z.up.sql';
  writeFilePair(dir, '20240101000000_z', '-- up', 'DROP TABLE T1;');
  const conn = makeMock();
  await rollbackMigration(conn, dir, 'MIGRATION_LOG', '', upFile);
  const drops = conn.execLog.filter(e => /DROP TABLE/i.test(e.sql));
  assert.strictEqual(drops.length, 1);
  fs.rmSync(dir, { recursive: true, force: true });
}));

tests.push(test('deletes filename from tracking table on rollback', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'strsql-mg-rb-'));
  const upFile = '20240101000000_w.up.sql';
  writeFilePair(dir, '20240101000000_w', '-- up', '-- down');
  const conn = makeMock();
  await rollbackMigration(conn, dir, 'MIGRATION_LOG', 'LIB', upFile);
  const del = conn.execLog.find(e => /DELETE FROM LIB\.MIGRATION_LOG/i.test(e.sql));
  assert.ok(del);
  assert.deepStrictEqual(del.params, [upFile]);
  fs.rmSync(dir, { recursive: true, force: true });
}));

tests.push(test('throws when .down.sql not found', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'strsql-mg-rb-'));
  const upFile = '20240101000000_nodown.up.sql';
  fs.writeFileSync(path.join(dir, upFile), '-- up only');
  const conn = makeMock();
  await assert.rejects(
    () => rollbackMigration(conn, dir, 'MIGRATION_LOG', '', upFile),
    /not found/i
  );
  fs.rmSync(dir, { recursive: true, force: true });
}));

// ─── runUp ────────────────────────────────────────────────────────────────────

console.log('\nmigrate — runUp');

tests.push(test('applies only pending migrations', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'strsql-mg-runup-'));
  writeFilePair(dir, '20240101000000_first');
  writeFilePair(dir, '20240102000000_second');
  // first already applied
  const conn = makeMock(['20240101000000_first.up.sql']);
  await runUp(conn, dir, 'MIGRATION_LOG', '');
  const inserts = conn.execLog.filter(e => /INSERT INTO MIGRATION_LOG/i.test(e.sql));
  // Only second should be inserted
  assert.strictEqual(inserts.length, 1);
  assert.deepStrictEqual(inserts[0].params, ['20240102000000_second.up.sql']);
  fs.rmSync(dir, { recursive: true, force: true });
}));

tests.push(test('does nothing when all migrations applied', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'strsql-mg-runup-'));
  writeFilePair(dir, '20240101000000_done');
  const conn = makeMock(['20240101000000_done.up.sql']);
  await runUp(conn, dir, 'MIGRATION_LOG', '');
  const inserts = conn.execLog.filter(e => /INSERT INTO/i.test(e.sql));
  assert.strictEqual(inserts.length, 0);
  fs.rmSync(dir, { recursive: true, force: true });
}));

// ─── runDown ──────────────────────────────────────────────────────────────────

console.log('\nmigrate — runDown');

tests.push(test('rolls back last applied migration', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'strsql-mg-rundn-'));
  writeFilePair(dir, '20240101000000_a', '-- up', '-- down');
  writeFilePair(dir, '20240102000000_b', '-- up', '-- down');
  const conn = makeMock([
    '20240101000000_a.up.sql',
    '20240102000000_b.up.sql',
  ]);
  await runDown(conn, dir, 'MIGRATION_LOG', '');
  const del = conn.execLog.find(e => /DELETE FROM MIGRATION_LOG/i.test(e.sql));
  assert.ok(del);
  assert.deepStrictEqual(del.params, ['20240102000000_b.up.sql']);
  fs.rmSync(dir, { recursive: true, force: true });
}));

tests.push(test('does nothing when no migrations applied', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'strsql-mg-rundn-'));
  const conn = makeMock([]);
  await runDown(conn, dir, 'MIGRATION_LOG', '');
  const dels = conn.execLog.filter(e => /DELETE FROM/i.test(e.sql));
  assert.strictEqual(dels.length, 0);
  fs.rmSync(dir, { recursive: true, force: true });
}));

// ─── await all + summary ──────────────────────────────────────────────────────

Promise.all(tests).then(() => {
  console.log(`\n  ${passed} passed, ${failed} failed\n`);
  if (failed > 0) process.exitCode = 1;
});
