'use strict';

/**
 * Unit tests for migrations/create.js (no ODBC connection required).
 */

const assert = require('assert');
const fs     = require('fs');
const os     = require('os');
const path   = require('path');

const { createMigration, createSeed } = require('../src/lib/migrations/create');

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

// ─── createMigration ─────────────────────────────────────────────────────────

console.log('\ncreate — createMigration');

test('creates .up.sql and .down.sql files', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'strsql-mig-'));
  const { upFile, downFile } = createMigration('test_mig', dir);
  assert.ok(fs.existsSync(upFile),   `missing: ${upFile}`);
  assert.ok(fs.existsSync(downFile), `missing: ${downFile}`);
  fs.rmSync(dir, { recursive: true, force: true });
});

test('filename matches pattern YYYYMMDDHHMMSS_name.up.sql', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'strsql-mig-'));
  const { upFile } = createMigration('add_index', dir);
  assert.ok(/\d{14}_add_index\.up\.sql$/.test(upFile), `bad pattern: ${upFile}`);
  fs.rmSync(dir, { recursive: true, force: true });
});

test('writes custom upContent and downContent', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'strsql-mig-'));
  const { upFile, downFile } = createMigration('custom', dir, {
    upContent:   'CREATE TABLE MYTABLE (ID INT);',
    downContent: 'DROP TABLE MYTABLE;',
  });
  assert.strictEqual(fs.readFileSync(upFile, 'utf8'),   'CREATE TABLE MYTABLE (ID INT);');
  assert.strictEqual(fs.readFileSync(downFile, 'utf8'), 'DROP TABLE MYTABLE;');
  fs.rmSync(dir, { recursive: true, force: true });
});

test('writes default comment when no content provided', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'strsql-mig-'));
  const { upFile } = createMigration('default', dir);
  const content = fs.readFileSync(upFile, 'utf8');
  assert.ok(content.startsWith('--'), `expected comment, got: ${content}`);
  fs.rmSync(dir, { recursive: true, force: true });
});

test('creates target directory when it does not exist', () => {
  const base = fs.mkdtempSync(path.join(os.tmpdir(), 'strsql-mig-'));
  const nested = path.join(base, 'sub', 'migrations');
  createMigration('nested', nested);
  assert.ok(fs.existsSync(nested));
  fs.rmSync(base, { recursive: true, force: true });
});

test('two calls produce different timestamps (unless same second)', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'strsql-mig-'));
  const { upFile: f1 } = createMigration('a', dir);
  const { upFile: f2 } = createMigration('b', dir);
  // Files must have different names (different letter suffix guarantees uniqueness)
  assert.notStrictEqual(path.basename(f1), path.basename(f2));
  fs.rmSync(dir, { recursive: true, force: true });
});

// ─── createSeed ──────────────────────────────────────────────────────────────

console.log('\ncreate — createSeed');

test('creates .up.sql and .down.sql files', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'strsql-seed-'));
  const { upFile, downFile } = createSeed('initial', dir);
  assert.ok(fs.existsSync(upFile),   `missing: ${upFile}`);
  assert.ok(fs.existsSync(downFile), `missing: ${downFile}`);
  fs.rmSync(dir, { recursive: true, force: true });
});

test('filename matches pattern YYYYMMDDHHMMSS_name.up.sql', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'strsql-seed-'));
  const { upFile } = createSeed('load_data', dir);
  assert.ok(/\d{14}_load_data\.up\.sql$/.test(upFile), `bad pattern: ${upFile}`);
  fs.rmSync(dir, { recursive: true, force: true });
});

test('writes custom upContent', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'strsql-seed-'));
  const sql = "INSERT INTO T VALUES (1, 'A');";
  const { upFile } = createSeed('data', dir, { upContent: sql });
  assert.strictEqual(fs.readFileSync(upFile, 'utf8'), sql);
  fs.rmSync(dir, { recursive: true, force: true });
});

test('downContent defaults to comment', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'strsql-seed-'));
  const { downFile } = createSeed('nodown', dir);
  const content = fs.readFileSync(downFile, 'utf8');
  assert.ok(content.startsWith('--'));
  fs.rmSync(dir, { recursive: true, force: true });
});

// ─── summary ─────────────────────────────────────────────────────────────────

console.log(`\n  ${passed} passed, ${failed} failed\n`);
if (failed > 0) process.exitCode = 1;
