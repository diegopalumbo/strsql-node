'use strict';

/**
 * Basic usage example (requires a real ODBC connection to run).
 * Run with: node test/basic.js
 */

const { IBMiConnection, formatTable, toCSV, toJSON, exportToFile } = require('../src/lib/index');
const { ProfileManager } = require('../src/lib/profiles');

async function main() {
  const pm = new ProfileManager();

  // ── Example 1: save and resolve a profile ──────────────────────────────────
  pm.set('test', {
    host: process.env.STRSQL_HOST || '10.0.0.1',
    username: process.env.STRSQL_USER || 'TESTUSER',
    password: process.env.STRSQL_PASSWORD || 'secret',
    defaultSchema: process.env.STRSQL_SCHEMA || 'QGPL',
  });

  const config = pm.resolve('test');
  console.log('Resolved config:', { ...config, password: '***' });

  // ── Example 2: connection + query (requires real IBM i) ───────────────────
  if (!process.env.STRSQL_HOST) {
    console.log('\nSet STRSQL_HOST to run a live connection test.');
    return;
  }

  const conn = new IBMiConnection(config);
  await conn.connect();
  console.log('Connected ✓');

  // SELECT
  const result = await conn.query(
    "SELECT TABLE_SCHEMA, TABLE_NAME FROM QSYS2.SYSTABLES FETCH FIRST 5 ROWS ONLY"
  );
  console.log('\nTable output:');
  console.log(formatTable(result));

  console.log('\nCSV:');
  console.log(toCSV(result));

  // List tables
  const tables = await conn.listTables(config.defaultSchema);
  console.log('\nlistTables:');
  console.log(formatTable(tables));

  await conn.disconnect();
  console.log('Disconnected ✓');
}

main().catch(console.error);
