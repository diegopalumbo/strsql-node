'use strict';

const fs   = require('fs');
const path = require('path');

// Returns "schema.table" when schema is non-empty, plain "table" otherwise.
function qualifiedName(schema, table) {
  return schema ? `${schema}.${table}` : table;
}

async function ensureMigrationTable(conn, migrationTable, schema) {
  const qname = qualifiedName(schema, migrationTable);
  try {
    await conn.execute(
      `CREATE TABLE ${qname} (` +
      `filename VARCHAR(255) NOT NULL, ` +
      `applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, ` +
      `PRIMARY KEY (filename))`
    );
    console.log(`🆕 Created table ${qname}.`);
  } catch (err) {
    // Ignore "table already exists" errors across all DB types.
    const msg = String(err.message || '');
    if (!/already exists|duplicate|42S01|42710|object.*already/i.test(msg)) throw err;
    console.log(`🟡 ${qname} already exists.`);
  }
}

async function getAppliedMigrations(conn, migrationTable, schema) {
  const qname  = qualifiedName(schema, migrationTable);
  const result = await conn.query(`SELECT filename FROM ${qname} ORDER BY filename`);
  return result.rows.map(r => r.FILENAME || r.filename);
}

async function applyMigration(conn, migrationsPath, migrationTable, schema, file) {
  const sql        = fs.readFileSync(path.join(migrationsPath, file), 'utf8');
  const statements = sql.split(';').map(s => s.trim()).filter(s => s.length > 0);
  for (const stmt of statements) {
    await conn.execute(stmt);
  }
  const qname = qualifiedName(schema, migrationTable);
  await conn.execute(`INSERT INTO ${qname} (filename) VALUES (?)`, [file]);
  console.log(`✅ Applied: ${file}`);
}

async function rollbackMigration(conn, migrationsPath, migrationTable, schema, file) {
  const downFile = file.replace('.up.sql', '.down.sql');
  const downPath = path.join(migrationsPath, downFile);
  if (!fs.existsSync(downPath)) {
    throw new Error(`Rollback file not found: ${downFile}`);
  }
  const sql        = fs.readFileSync(downPath, 'utf8');
  const statements = sql.split(';').map(s => s.trim()).filter(s => s.length > 0);
  for (const stmt of statements) {
    await conn.execute(stmt);
  }
  const qname = qualifiedName(schema, migrationTable);
  await conn.execute(`DELETE FROM ${qname} WHERE filename = ?`, [file]);
  console.log(`↩️  Rolled back: ${file}`);
}

async function runUp(conn, migrationsPath, migrationTable, schema) {
  await ensureMigrationTable(conn, migrationTable, schema);
  const applied = await getAppliedMigrations(conn, migrationTable, schema);
  const files   = fs.readdirSync(migrationsPath)
    .filter(f => f.endsWith('.up.sql'))
    .sort();
  for (const file of files) {
    if (!applied.includes(file)) {
      console.log(`🟢 Applying: ${file}`);
      await applyMigration(conn, migrationsPath, migrationTable, schema, file);
    } else {
      console.log(`🟡 Already applied: ${file}`);
    }
  }
}

async function runDown(conn, migrationsPath, migrationTable, schema) {
  await ensureMigrationTable(conn, migrationTable, schema);
  const applied = await getAppliedMigrations(conn, migrationTable, schema);
  if (applied.length === 0) {
    console.log('ℹ️  No migration to rollback.');
    return;
  }
  const last = applied[applied.length - 1];
  console.log(`🔁 Rolling back: ${last}`);
  await rollbackMigration(conn, migrationsPath, migrationTable, schema, last);
}

module.exports = {
  ensureMigrationTable,
  getAppliedMigrations,
  applyMigration,
  rollbackMigration,
  runUp,
  runDown,
};
