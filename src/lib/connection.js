'use strict';

const odbc      = require('odbc');
const { getDriver } = require('./drivers');

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
    this.conn      = await odbc.connect(connStr);
    this.connected = true;

    if (this.config.defaultSchema && this.driver.setSchema) {
      try {
        const sql = this.driver.setSchema.includes('?')
          ? this.driver.setSchema.replace('?', this.config.defaultSchema)
          : `${this.driver.setSchema} ${this.config.defaultSchema}`;
        await this.conn.query(sql);
      } catch { /* non-fatal */ }
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

  async describeTable(table, schema) {
    const s = schema || this.config.defaultSchema || '';
    const [schemaName, tableName] = table.includes('.')
      ? table.split('.')
      : [s, table];
    const spec = this.driver.describeSQL(schemaName, tableName);
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

  paginateSQL(innerSQL, offset, limit) {
    return this.driver.paginateSQL(innerSQL, offset, limit);
  }

  quoteIdentifier(name) {
    return this.driver.quoteId(name);
  }

  isConnected()  { return this.connected; }
  get dbType()   { return this.type; }
  get dbLabel()  { return this.driver.label; }
}

// backward-compat alias — new IBMiConnection(config) still works unchanged
const IBMiConnection = ODBCConnection;

module.exports = { ODBCConnection, IBMiConnection };
