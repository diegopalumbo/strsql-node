'use strict';

const { getDriver } = require('./drivers');

function parseLibraryList(libs) {
  if (!libs) return [];
  const raw = Array.isArray(libs) ? libs.join(',') : String(libs);
  const parts = raw
    .trim()
    .replace(/^[[(]\s*/, '')
    .replace(/\s*[\])]$/, '')
    .split(/[,\s]+/)
    .map(l => l.trim().replace(/^["'`]+|["'`]+$/g, ''))
    .filter(Boolean);
  return parts[0]?.toLowerCase() === 'set' ? parts.slice(1) : parts;
}

function loadConnector() {
  try {
    return require('idb-pconnector');
  } catch (err) {
    err.message =
      'Cannot load idb-pconnector. The idb adapter runs only on IBM i/PASE with ' +
      'IBM\'s native Db2 for i Node.js connector installed. Install it on IBM i ' +
      'or use adapter=odbc. Original error: ' + err.message;
    throw err;
  }
}

function normalizeRows(rows) {
  if (!rows) return [];
  return Array.isArray(rows) ? rows : Array.from(rows);
}

function columnsFromRows(rows) {
  const first = rows.find(row => row && typeof row === 'object');
  if (!first) return [];
  return Object.keys(first).map(name => ({ name }));
}

function systemName(name) {
  const value = String(name || '').trim();
  if (!/^[A-Za-z0-9_$#@]+$/.test(value)) {
    throw new Error(`Invalid IBM i identifier: ${name}`);
  }
  return value.toUpperCase();
}

function isDmlStatement(sql) {
  return /^(INSERT|UPDATE|DELETE|MERGE)\b/i.test(String(sql || '').trim());
}

function hasNoCommitClause(sql) {
  return /\bWITH\s+(NC|NONE)\b/i.test(String(sql || ''));
}

function needsNoCommitRetry(sql, err) {
  const message = String(err?.message || '');
  return isDmlStatement(sql) &&
    !hasNoCommitClause(sql) &&
    (message.includes('SQLSTATE=55019') || message.includes('SQLCODE=-7008'));
}

function appendNoCommitClause(sql) {
  const trimmed = String(sql || '').trim().replace(/;\s*$/, '');
  return `${trimmed} WITH NC`;
}

class Db2iIdbConnection {
  constructor(config = {}) {
    this.config = { type: 'ibmi', adapter: 'idb', ...config };
    this.type = 'ibmi';
    this.adapter = 'idb';
    this.driver = getDriver(this.type);
    this.conn = null;
    this.connected = false;
  }

  buildConnectionString() {
    return this.config.connectionUrl || this.config.host || '*LOCAL';
  }

  async connect() {
    const { Connection } = loadConnector();
    const url = this.buildConnectionString();

    // Connection({ url }) connects immediately in the constructor.
    this.conn = new Connection({ url });
    this.connected = true;

    // Attempt to set no-commitment-control (SQL_ATTR_COMMIT=240, SQL_TXN_NO_COMMIT=5)
    // so DML is permanent without explicit COMMIT on non-journaled tables.
    // setConnAttr() is async in idb-pconnector (called after connect).
    try { await this.conn.setConnAttr(240, 5); } catch { /* ignore if not supported */ }

    if (this.config.defaultSchema && !this.config.libraryList) {
      try {
        await this.execute(`SET SCHEMA ${systemName(this.config.defaultSchema)}`);
      } catch { /* non-fatal */ }
    }

    if (this.config.libraryList) {
      const libs = parseLibraryList(this.config.libraryList);
      if (libs.length > 0) await this.setLibraryList(libs);
    }
  }

  async disconnect() {
    if (this.conn && this.connected) {
      try { await this.conn.disconn(); } catch {}
      try { await this.conn.close(); } catch {}
      this.connected = false;
      this.conn = null;
    }
  }

  _statement() {
    if (!this.connected) throw new Error('Not connected. Call connect() first.');
    return this.conn.getStatement();
  }

  async _runStatement(sql, params = []) {
    const statement = this._statement();
    try {
      if (!params || params.length === 0) {
        // exec() returns Promise<Array|null> — perfect for SELECT
        const rows = await statement.exec(sql);
        return normalizeRows(rows);
      }

      await statement.prepare(sql);
      await statement.bindParameters(params);
      await statement.execute();
      const rows = await statement.fetchAll();
      return normalizeRows(rows);
    } finally {
      try { await statement.close(); } catch {}
    }
  }

  async _executeStatement(sql, params = []) {
    let rowCount = 0;
    let statement = this._statement();
    try {
      const run = async (statementSql) => {
        await statement.prepare(statementSql);
        if (params && params.length > 0) {
          await statement.bindParameters(params);
        }
        await statement.execute();
        try { rowCount = await statement.numRows() || 0; } catch {}
      };

      try {
        await run(sql);
      } catch (err) {
        if (!needsNoCommitRetry(sql, err)) throw err;
        try { await statement.close(); } catch {}
        statement = this._statement();
        await run(appendNoCommitClause(sql));
      }

      // Commit for journaled tables; silently ignored for non-journaled (SQL_TXN_NO_COMMIT)
      try { await statement.commit(); } catch {}
    } finally {
      try { await statement.close(); } catch {}
    }
    return rowCount;
  }

  async query(sql, params = []) {
    const start = Date.now();
    const rows = await this._runStatement(sql, params);
    return {
      columns: columnsFromRows(rows),
      rows,
      rowCount: rows.length,
      elapsed: Date.now() - start,
      statement: sql,
    };
  }

  async execute(sql, params = []) {
    const start = Date.now();
    const rowCount = await this._executeStatement(sql, params);
    return {
      rowCount,
      elapsed: Date.now() - start,
      statement: sql,
    };
  }

  async listTables(schema) {
    const s = schema || this.config.defaultSchema || 'QGPL';
    const { sql, params } = this.driver.listTablesSql(s);
    return this.query(sql, params);
  }

  async _resolveSchemaFromLibl(tableName) {
    const libs = this.config.libraryList;
    if (!libs || (Array.isArray(libs) && libs.length === 0)) return '';
    const arr = parseLibraryList(libs);
    const placeholders = arr.map(() => '?').join(',');
    const result = await this.query(
      `SELECT TABLE_SCHEMA FROM QSYS2.SYSTABLES WHERE TABLE_NAME = ? AND TABLE_SCHEMA IN (${placeholders}) FETCH FIRST 1 ROWS ONLY`,
      [tableName.toUpperCase(), ...arr.map(l => l.toUpperCase())]
    );
    return result.rows.length > 0 ? String(result.rows[0].TABLE_SCHEMA).trim() : '';
  }

  async describeTable(table, schema) {
    let s = schema || this.config.defaultSchema || '';
    const [schemaName, tableName] = table.includes('.')
      ? table.split('.')
      : [s, table];
    let resolvedSchema = schemaName;
    if (!resolvedSchema && this.config.libraryList) {
      resolvedSchema = await this._resolveSchemaFromLibl(tableName);
    }
    const spec = this.driver.describeSQL(resolvedSchema, tableName);
    return this.query(spec.sql, spec.params);
  }

  async primaryKeys(table, schema) {
    let s = schema || this.config.defaultSchema || '';
    const [schemaName, tableName] = table.includes('.')
      ? table.split('.')
      : [s, table];
    let resolvedSchema = schemaName;
    if (!resolvedSchema && this.config.libraryList) {
      resolvedSchema = await this._resolveSchemaFromLibl(tableName);
    }
    const spec = this.driver.primaryKeysSQL(resolvedSchema, tableName);
    const _extract = (s, raw) => {
      const rows = s.mapRow ? raw.rows.map(s.mapRow).filter(Boolean) : raw.rows;
      return rows.map(r => (r.COLUMN_NAME || r.column_name || '').toUpperCase());
    };
    try {
      const raw  = await this.query(spec.sql, spec.params);
      const cols = _extract(spec, raw);
      if (cols.length === 0 && spec.fallback) {
        const raw2 = await this.query(spec.fallback.sql, spec.fallback.params);
        return new Set(_extract(spec.fallback, raw2));
      }
      return new Set(cols);
    } catch (err) {
      if (spec.fallback) {
        try {
          const raw2 = await this.query(spec.fallback.sql, spec.fallback.params);
          return new Set(_extract(spec.fallback, raw2));
        } catch { /* fall through to warning */ }
      }
      process.stderr.write(`[warn] primaryKeys query failed: ${err.message}\n`);
      return new Set();
    }
  }

  paginateSQL(innerSQL, offset, limit) {
    return this.driver.paginateSQL(innerSQL, offset, limit);
  }

  async _libraryListInfo() {
    try {
      const result = await this.query(
        `SELECT SYSTEM_SCHEMA_NAME, TYPE
         FROM QSYS2.LIBRARY_LIST_INFO
         WHERE TYPE IN ('CURRENT', 'USER')
         ORDER BY ORDINAL_POSITION`
      );
      const currentRow = result.rows.find(r => String(r.TYPE || '').toUpperCase() === 'CURRENT');
      const userRows = result.rows.filter(r => String(r.TYPE || '').toUpperCase() === 'USER');
      return {
        current: currentRow?.SYSTEM_SCHEMA_NAME || '',
        user: userRows.map(r => r.SYSTEM_SCHEMA_NAME).filter(Boolean),
      };
    } catch {
      return { current: '', user: [] };
    }
  }

  async setLibraryList(libs) {
    if (!this.connected) throw new Error('Not connected.');
    const arr = parseLibraryList(libs);
    if (arr.length === 0) throw new Error('Empty library list.');
    await this.execute(this.driver.setLibraryList(arr));
    const info = await this._libraryListInfo();
    const currentLib = info.current || arr[0];
    if (currentLib && !this.config.defaultSchema) this.config.defaultSchema = currentLib;
    this.config.libraryList = info.user.length > 0 ? info.user : arr;

    // After CHGLIBL, set the schema to the current library so that
    // unqualified table names resolve correctly.
    const schema = this.config.defaultSchema || currentLib;
    if (schema) {
      try { await this.execute(`SET SCHEMA ${systemName(schema)}`); } catch { /* non-fatal */ }
    }
  }

  quoteIdentifier(name) {
    return this.driver.quoteId(name);
  }

  isConnected() { return this.connected; }
  get dbType() { return this.type; }
  get dbLabel() { return `${this.driver.label} / idb-pconnector`; }
}

module.exports = { Db2iIdbConnection };
