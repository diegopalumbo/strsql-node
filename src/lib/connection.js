'use strict';

const { getDriver } = require('./drivers');
const { Db2iIdbConnection } = require('./idbConnection');

function wantsIdbAdapter(config = {}) {
  const adapter = String(config.adapter || config.driver || '').toLowerCase();
  return (config.type || 'ibmi').toLowerCase() === 'ibmi' &&
    ['idb', 'idb-connector', 'native'].includes(adapter);
}

function loadOdbc() {
  try {
    return require('odbc');
  } catch (err) {
    err.message =
      'Cannot load odbc. Install the odbc package and required ODBC driver, ' +
      'or use IBM i native adapter=idb on IBM i/PASE. Original error: ' + err.message;
    throw err;
  }
}

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

/**
 * Universal ODBC connection wrapper.
 * Supports any database type defined in drivers.js.
 *
 * config keys:
 *   type           {string}  DB type: ibmi|sqlserver|postgresql|mysql|oracle|db2|sqlite
 *                            Defaults to 'ibmi' for backward compatibility.
 *   connectionString {string}  Raw ODBC string — bypasses builder entirely.
 *   host           {string}
 *   port           {number}
 *   username       {string}
 *   password       {string}
 *   database       {string}  database/catalog name (non-IBM i types)
 *   defaultSchema  {string}  schema to SET after connect
 *   -- IBM i specific --
 *   namingMode     {string}  'sql' | 'system'
 *   translate      {boolean}
 *   -- SQL Server specific --
 *   instanceName   {string}
 *   encrypt        {boolean}
 *   -- Oracle specific --
 *   serviceName    {string}
 *   -- PostgreSQL specific --
 *   sslMode        {string}
 */
class ODBCConnection {
  constructor(config) {
    if (wantsIdbAdapter(config)) {
      return new Db2iIdbConnection(config);
    }
    this.config    = config;
    this.type      = (config.type || 'ibmi').toLowerCase();
    this.driver    = getDriver(this.type);
    this.conn      = null;
    this.connected = false;
  }

  buildConnectionString() {
    return this.driver.buildConnStr(this.config);
  }

  async connect() {
    const connStr = this.buildConnectionString();
    const odbc = loadOdbc();
    this.conn      = await odbc.connect(connStr);
    this.connected = true;

    // When libraryList is set on IBM i, system naming is forced (NAM=1)
    // so unqualified names resolve through the library list.
    // Skip SET SCHEMA in that case — it would override library list resolution.
    const hasLibraryList = this.config.libraryList && this.driver.setLibraryList;

    if (this.config.defaultSchema && this.driver.setSchema && !hasLibraryList) {
      try {
        const sql = this.driver.setSchema.includes('?')
          ? this.driver.setSchema.replace('?', this.config.defaultSchema)
          : `${this.driver.setSchema} ${this.config.defaultSchema}`;
        await this.conn.query(sql);
      } catch { /* non-fatal */ }
    }

    // IBM i library list: run CHGLIBL after connect
    if (hasLibraryList) {
      const libs = parseLibraryList(this.config.libraryList);
      if (libs.length > 0) {
        try {
          await this.setLibraryList(libs);
        } catch (err) {
          process.stderr.write(`[warn] setLibraryList failed: ${err.message}\n`);
        }
      }
    }
  }

  async disconnect() {
    if (this.conn && this.connected) {
      await this.conn.close();
      this.connected = false;
      this.conn      = null;
    }
  }

  async query(sql, params = []) {
    if (!this.connected) throw new Error('Not connected. Call connect() first.');
    const start   = Date.now();
    const result  = await this.conn.query(sql, params);
    const elapsed = Date.now() - start;
    const columns = result.columns
      ? result.columns.map(c => ({
          name: c.name, dataType: c.dataType,
          columnSize: c.columnSize, nullable: c.nullable,
        }))
      : [];
    return {
      columns,
      rows:     Array.from(result),
      rowCount: result.count !== undefined ? result.count : result.length,
      elapsed,
      statement: sql,
    };
  }

  async execute(sql, params = []) {
    if (!this.connected) throw new Error('Not connected.');
    const start  = Date.now();
    const result = await this.conn.query(sql, params);
    return {
      rowCount: result.count !== undefined ? result.count : 0,
      elapsed:  Date.now() - start,
      statement: sql,
    };
  }

  async listTables(schema) {
    const s = schema || this.config.defaultSchema || 'public';
    const { sql, params } = this.driver.listTablesSql(s);
    return this.query(sql, params);
  }

  /**
   * Resolve the schema for an unqualified table name by searching the library list.
   * Returns the first library that contains the table, or '' if not found.
   * Only applies to IBM i with an active library list.
   */
  async _resolveSchemaFromLibl(tableName) {
    if (this.type !== 'ibmi') return '';
    const libs = this.config.libraryList;
    if (!libs || (Array.isArray(libs) && libs.length === 0)) return '';
    const arr = parseLibraryList(libs);
    const placeholders = arr.map(() => '?').join(',');
    const result = await this.query(
      `SELECT TABLE_SCHEMA FROM QSYS2.SYSTABLES WHERE TABLE_NAME = ? AND TABLE_SCHEMA IN (${placeholders}) FETCH FIRST 1 ROWS ONLY`,
      [tableName.toUpperCase(), ...arr.map(l => l.toUpperCase())]
    );
    return result.rows.length > 0 ? result.rows[0].TABLE_SCHEMA.trim() : '';
  }

  async describeTable(table, schema) {
    let s = schema || this.config.defaultSchema || '';
    const [schemaName, tableName] = table.includes('.')
      ? table.split('.')
      : [s, table];
    // If no schema and library list is active, resolve from library list
    let resolvedSchema = schemaName;
    if (!resolvedSchema && this.config.libraryList) {
      resolvedSchema = await this._resolveSchemaFromLibl(tableName);
    }
    const spec = this.driver.describeSQL(resolvedSchema, tableName);
    const raw  = await this.query(spec.sql, spec.params);
    if (spec.mapRow) {
      return {
        ...raw,
        rows: raw.rows.map(spec.mapRow),
        columns: [
          { name: 'COLUMN_NAME' }, { name: 'DATA_TYPE' }, { name: 'LENGTH' },
          { name: 'NUMERIC_SCALE' }, { name: 'IS_NULLABLE' }, { name: 'COLUMN_DEFAULT' },
        ],
      };
    }
    return raw;
  }

  async primaryKeys(table, schema) {
    let s = schema || this.config.defaultSchema || '';
    const [schemaName, tableName] = table.includes('.')
      ? table.split('.')
      : [s, table];
    if (!this.driver.primaryKeysSQL) return new Set();
    // If no schema and library list is active, resolve from library list
    let resolvedSchema = schemaName;
    if (!resolvedSchema && this.config.libraryList) {
      resolvedSchema = await this._resolveSchemaFromLibl(tableName);
    }
    // For IBM i, prefer the ODBC SQLPrimaryKeys catalog function (conn.primaryKeys())
    // which correctly handles both SQL PRIMARY KEY constraints and DDS physical file
    // key fields — without relying on SQL catalog views that may not cover DDS objects.
    if (this.type === 'ibmi' && typeof this.conn.primaryKeys === 'function') {
      try {
        const rows = await this.conn.primaryKeys(
          null,
          resolvedSchema || null,
          tableName.toUpperCase()
        );
        if (rows && rows.length > 0) {
          return new Set(rows.map(r => (r.COLUMN_NAME || '').toUpperCase()));
        }
      } catch { /* fall through to SQL approach */ }
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
    if (this.type !== 'ibmi') return { current: '', user: [] };
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
    if (!this.driver.setLibraryList) throw new Error(`Library list not supported for ${this.type}.`);
    const arr = parseLibraryList(libs);
    if (arr.length === 0) throw new Error('Empty library list.');

    // System naming (NAM=1) is required for unqualified table names to resolve
    // through the library list.  If the current connection was opened without it,
    // we must reconnect with NAM=1 before setting the library list.
    const needsReconnect = this.type === 'ibmi' && this.config.namingMode !== 'system' && !this.config.libraryList;
    if (needsReconnect) {
      this.config.libraryList = arr;
      this.config.namingMode  = 'system';
      await this.conn.close();
      this.connected = false;
      const connStr = this.buildConnectionString();
      const odbc = loadOdbc();
      this.conn      = await odbc.connect(connStr);
      this.connected = true;
    }

    const sql = this.driver.setLibraryList(arr);
    await this.conn.query(sql);
    const info = await this._libraryListInfo();
    if (info.current && !this.config.defaultSchema) this.config.defaultSchema = info.current;
    this.config.libraryList = info.user.length > 0 ? info.user : arr;
  }

  quoteIdentifier(name) {
    return this.driver.quoteId(name);
  }

  isConnected()  { return this.connected; }
  get dbType()   { return this.type; }
  get dbLabel()  { return this.driver.label; }
}

// backward-compat alias — new IBMiConnection(config) still works unchanged
function IBMiConnection(config) {
  return wantsIdbAdapter(config)
    ? new Db2iIdbConnection(config)
    : new ODBCConnection(config);
}

function createConnection(config) {
  return wantsIdbAdapter(config)
    ? new Db2iIdbConnection(config)
    : new ODBCConnection(config);
}

module.exports = { ODBCConnection, IBMiConnection, Db2iIdbConnection, createConnection, parseLibraryList };
