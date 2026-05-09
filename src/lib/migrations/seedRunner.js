'use strict';

const fs   = require('fs');
const path = require('path');
const { createConnection } = require('../connection');

function qualifiedName(schema, table) {
  return schema ? `${schema}.${table}` : table;
}

async function ensureSeedTable(conn, seedTable, schema) {
  const qname = qualifiedName(schema, seedTable);
  try {
    await conn.execute(
      `CREATE TABLE ${qname} (` +
      `filename VARCHAR(255) NOT NULL, ` +
      `applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, ` +
      `PRIMARY KEY (filename))`
    );
    console.log(`🆕 Created table ${qname}.`);
  } catch (err) {
    const msg = String(err.message || '');
    if (!/already exists|duplicate|42S01|42710|object.*already/i.test(msg)) throw err;
    console.log(`🟡 ${qname} already exists.`);
  }
}

async function getAppliedSeeds(conn, seedTable, schema) {
  const qname  = qualifiedName(schema, seedTable);
  const result = await conn.query(`SELECT filename FROM ${qname} ORDER BY filename`);
  return result.rows.map(r => r.FILENAME || r.filename);
}

async function applySeed(conn, seedsPath, seedTable, schema, file) {
  const sql        = fs.readFileSync(path.join(seedsPath, file), 'utf8');
  const statements = sql.split(';').map(s => s.trim()).filter(s => s.length > 0);
  for (const stmt of statements) {
    await conn.execute(stmt);
  }
  const qname = qualifiedName(schema, seedTable);
  await conn.execute(`INSERT INTO ${qname} (filename) VALUES (?)`, [file]);
  console.log(`✅ Seeded: ${file}`);
}

async function rollbackSeed(conn, seedsPath, seedTable, schema, file) {
  const downFile = file.replace('.up.sql', '.down.sql');
  const downPath = path.join(seedsPath, downFile);
  if (!fs.existsSync(downPath)) {
    console.warn(`⚠️  No .down.sql found for: ${file}`);
    return;
  }
  const sql        = fs.readFileSync(downPath, 'utf8');
  const statements = sql.split(';').map(s => s.trim()).filter(s => s.length > 0);
  for (const stmt of statements) {
    await conn.execute(stmt);
  }
  const qname = qualifiedName(schema, seedTable);
  await conn.execute(`DELETE FROM ${qname} WHERE filename = ?`, [file]);
  console.log(`↩️  Rolled back seed: ${file}`);
}

/**
 * Run seeds against the specified database.
 *
 * @param {string} action  'up' | 'down'
 * @param {object} opts
 * @param {object} opts.config       Connection config (from ProfileManager.resolve).
 * @param {string} opts.seedsPath    Absolute path to the seeds directory.
 * @param {string} [opts.seedTable='SEED_LOG']  Tracking table name.
 * @param {string} [opts.schema='']  Schema that qualifies the tracking table.
 */
async function runSeeds(action, { config, seedsPath, seedTable = 'SEED_LOG', schema = '' }) {
  if (!['up', 'down'].includes(action)) {
    throw new Error(`Unknown action "${action}". Use "up" or "down".`);
  }
  const conn = createConnection(config);
  try {
    await conn.connect();
    await ensureSeedTable(conn, seedTable, schema);
    const applied = await getAppliedSeeds(conn, seedTable, schema);

    if (action === 'up') {
      const files = fs.readdirSync(seedsPath)
        .filter(f => f.endsWith('.up.sql'))
        .sort();
      for (const file of files) {
        if (!applied.includes(file)) {
          console.log(`🌱 Applying seed: ${file}`);
          await applySeed(conn, seedsPath, seedTable, schema, file);
        } else {
          console.log(`🟡 Already applied: ${file}`);
        }
      }
    } else {
      if (applied.length === 0) {
        console.log('ℹ️  No seed to rollback.');
      } else {
        const last = applied[applied.length - 1];
        console.log(`🔁 Rolling back seed: ${last}`);
        await rollbackSeed(conn, seedsPath, seedTable, schema, last);
      }
    }
  } finally {
    await conn.disconnect().catch(() => {});
  }
}

module.exports = { runSeeds };
