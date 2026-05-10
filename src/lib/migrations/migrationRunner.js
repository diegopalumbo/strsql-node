'use strict';

const { createConnection } = require('../connection');
const { runUp, runDown }   = require('./migrate');

/**
 * Run migrations against the specified database.
 *
 * @param {string} action  'up' | 'down'
 * @param {object} opts
 * @param {object} opts.config          Connection config (from ProfileManager.resolve).
 * @param {string} opts.migrationsPath  Absolute path to the migrations directory.
 * @param {string} [opts.migrationTable='MIGRATION_LOG']  Tracking table name.
 * @param {string} [opts.schema='']     Schema that qualifies the tracking table.
 */
async function runMigrations(action, { config, migrationsPath, migrationTable = 'MIGRATION_LOG', schema = '' }) {
  if (!['up', 'down'].includes(action)) {
    throw new Error(`Unknown action "${action}". Use "up" or "down".`);
  }
  const conn = createConnection(config);
  try {
    await conn.connect();
    if (action === 'up') {
      await runUp(conn, migrationsPath, migrationTable, schema);
    } else {
      await runDown(conn, migrationsPath, migrationTable, schema);
    }
  } finally {
    await conn.disconnect().catch(() => {});
  }
}

module.exports = { runMigrations };
