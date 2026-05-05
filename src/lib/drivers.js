'use strict';

/**
 * Registry of supported ODBC database types.
 *
 * Each entry defines:
 *   label         {string}    human-readable name shown in \profiles list
 *   buildConnStr  {function}  (config) → ODBC connection string
 *   setSchema     {string|fn} SQL to run after connect to set default schema (? = schemaName)
 *                             Pass null to skip.
 *   listTablesSql {fn}        (schema) → { sql, params }
 *   describeSQL   {fn}        (schema, table) → { sql, params }
 *   paginateSQL   {fn}        (innerSQL, offset, limit) → paged SQL string
 *   quoteId       {fn}        (identifier) → quoted identifier string
 *
 * Config keys used by builders:
 *   host, port, username, password, database, defaultSchema,
 *   instanceName (SQL Server), serviceName (Oracle), sslMode (PG),
 *   namingMode (IBM i), translate (IBM i), connectionString (raw override)
 */

const DRIVERS = {

  // ── IBM i / AS400 ──────────────────────────────────────────────────────────
  ibmi: {
    label: 'IBM i (AS/400)',
    buildConnStr(cfg) {
      if (cfg.connectionString) return cfg.connectionString;
      const p = [
        `DRIVER={${cfg.driverName || 'IBM i Access ODBC Driver'}}`,
        `SYSTEM=${cfg.host}`,
      ];
      if (cfg.username)      p.push(`UID=${cfg.username}`);
      if (cfg.password)      p.push(`PWD=${cfg.password}`);
      if (cfg.defaultSchema) p.push(`DBQ=${cfg.defaultSchema}`);
      // When libraryList is set, force system naming (NAM=1) so that
      // unqualified table references resolve through the library list.
      if (cfg.libraryList || cfg.namingMode === 'system') p.push(`NAM=1`);
      if (cfg.translate)     p.push(`TRANSLATE=1`);
      return p.join(';') + ';';
    },
    setSchema: `SET SCHEMA ?`,
    setLibraryList(libs) {
      const libStr = libs.length > 0
        ? libs.map(l => l.trim().toUpperCase()).join(' ')
        : '*NONE';
      return `CALL QSYS2.QCMDEXC('CHGLIBL LIBL(${libStr})')`;
    },
    listTablesSql(schema) {
      return {
        sql: `SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
              FROM QSYS2.SYSTABLES
              WHERE TABLE_SCHEMA = ?
              ORDER BY TABLE_NAME`,
        params: [schema.toUpperCase()],
      };
    },
    describeSQL(schema, table) {
      return {
        sql: `SELECT COLUMN_NAME, DATA_TYPE, LENGTH, NUMERIC_SCALE, IS_NULLABLE, 
              COLUMN_DEFAULT,
              COALESCE(NULLIF(TRIM(COLUMN_HEADING), ''), COLUMN_TEXT) AS COLUMN_HEADING
              FROM QSYS2.SYSCOLUMNS
              WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
              ORDER BY ORDINAL_POSITION`,
        params: [schema.toUpperCase(), table.toUpperCase()],
      };
    },
    primaryKeysSQL(schema, table) {
      // QSYS2.SYSKEYS holds key columns for both SQL-constraint PKs and DDS
      // physical file keys. The primary access path of a physical file has
      // INDEX_NAME = TABLE_NAME and INDEX_SCHEMA = TABLE_SCHEMA.
      return {
        sql: `SELECT COLUMN_NAME
              FROM QSYS2.SYSKEYS
              WHERE INDEX_SCHEMA = ? AND INDEX_NAME = ?
              ORDER BY ORDINAL_POSITION`,
        params: [schema.toUpperCase(), table.toUpperCase()],
      };
    },
    paginateSQL(inner, offset, limit) {
      if (offset > 0) {
        return `SELECT * FROM (${inner}) AS SUBQ OFFSET ${offset} ROWS FETCH NEXT ${limit} ROWS ONLY`;
      }
      return `SELECT * FROM (${inner}) AS SUBQ FETCH FIRST ${limit} ROWS ONLY`;
    },
    quoteId: id => `"${id}"`,
  },

  // ── SQL Server ─────────────────────────────────────────────────────────────
  sqlserver: {
    label: 'SQL Server',
    buildConnStr(cfg) {
      if (cfg.connectionString) return cfg.connectionString;
      const server = cfg.instanceName
        ? `${cfg.host}\\${cfg.instanceName}`
        : cfg.host;
      const port = cfg.port ? `,${cfg.port}` : '';
      const p = [
        `DRIVER={${cfg.driverName || 'ODBC Driver 18 for SQL Server'}}`,
        `SERVER=${server}${port}`,
      ];
      if (cfg.database)  p.push(`DATABASE=${cfg.database}`);
      if (cfg.username)  p.push(`UID=${cfg.username}`);
      if (cfg.password)  p.push(`PWD=${cfg.password}`);
      // Trust server cert by default (common in dev; override via connectionString for prod)
      p.push(`TrustServerCertificate=yes`);
      if (cfg.encrypt === false) p.push(`Encrypt=no`);
      return p.join(';') + ';';
    },
    setSchema: `USE ?`,   // SQL Server uses USE <db> not SET SCHEMA; schema = database name
    listTablesSql(schema) {
      const db = schema || 'dbo';
      return {
        sql: `SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
              FROM INFORMATION_SCHEMA.TABLES
              WHERE TABLE_SCHEMA = ?
              ORDER BY TABLE_NAME`,
        params: [db],
      };
    },
    describeSQL(schema, table) {
      return {
        sql: `SELECT COLUMN_NAME, DATA_TYPE,
                     CHARACTER_MAXIMUM_LENGTH AS LENGTH,
                     NUMERIC_SCALE,
                     IS_NULLABLE,
                     COLUMN_DEFAULT
              FROM INFORMATION_SCHEMA.COLUMNS
              WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
              ORDER BY ORDINAL_POSITION`,
        params: [schema, table],
      };
    },
    primaryKeysSQL(schema, table) {
      return {
        sql: `SELECT COLUMN_NAME
              FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
              WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') = 1
                AND TABLE_SCHEMA = ? AND TABLE_NAME = ?
              ORDER BY ORDINAL_POSITION`,
        params: [schema, table],
      };
    },
    paginateSQL(inner, offset, limit) {
      // SQL Server requires ORDER BY for OFFSET … FETCH
      const hasOrder = /ORDER\s+BY/i.test(inner);
      const ordered  = hasOrder ? inner : `${inner} ORDER BY (SELECT NULL)`;
      return `SELECT * FROM (${ordered}) AS _P
              ORDER BY (SELECT NULL)
              OFFSET ${offset} ROWS FETCH NEXT ${limit} ROWS ONLY`;
    },
    quoteId: id => `[${id}]`,
  },

  // ── PostgreSQL ─────────────────────────────────────────────────────────────
  postgresql: {
    label: 'PostgreSQL',
    buildConnStr(cfg) {
      if (cfg.connectionString) return cfg.connectionString;
      const p = [`DRIVER={${cfg.driverName || 'PostgreSQL Unicode'}}`];
      if (cfg.host)     p.push(`SERVER=${cfg.host}`);
      if (cfg.port)     p.push(`PORT=${cfg.port || 5432}`);
      if (cfg.database) p.push(`DATABASE=${cfg.database}`);
      if (cfg.username) p.push(`UID=${cfg.username}`);
      if (cfg.password) p.push(`PWD=${cfg.password}`);
      if (cfg.sslMode)  p.push(`SSLMode=${cfg.sslMode}`);
      return p.join(';') + ';';
    },
    setSchema: `SET search_path TO ?`,
    listTablesSql(schema) {
      return {
        sql: `SELECT table_schema AS TABLE_SCHEMA, table_name AS TABLE_NAME, table_type AS TABLE_TYPE
              FROM information_schema.tables
              WHERE table_schema = ?
              ORDER BY table_name`,
        params: [schema],
      };
    },
    describeSQL(schema, table) {
      return {
        sql: `SELECT column_name AS COLUMN_NAME,
                     data_type   AS DATA_TYPE,
                     character_maximum_length AS LENGTH,
                     numeric_scale AS NUMERIC_SCALE,
                     is_nullable   AS IS_NULLABLE,
                     column_default AS COLUMN_DEFAULT
              FROM information_schema.columns
              WHERE table_schema = ? AND table_name = ?
              ORDER BY ordinal_position`,
        params: [schema, table],
      };
    },
    primaryKeysSQL(schema, table) {
      return {
        sql: `SELECT kcu.column_name AS COLUMN_NAME
              FROM information_schema.table_constraints tc
              JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
               AND tc.table_schema    = kcu.table_schema
               AND tc.table_name      = kcu.table_name
              WHERE tc.constraint_type = 'PRIMARY KEY'
                AND tc.table_schema = ? AND tc.table_name = ?
              ORDER BY kcu.ordinal_position`,
        params: [schema, table],
      };
    },
    paginateSQL(inner, offset, limit) {
      return `SELECT * FROM (${inner}) AS _p LIMIT ${limit} OFFSET ${offset}`;
    },
    quoteId: id => `"${id}"`,
  },

  // ── MySQL / MariaDB ────────────────────────────────────────────────────────
  mysql: {
    label: 'MySQL / MariaDB',
    buildConnStr(cfg) {
      if (cfg.connectionString) return cfg.connectionString;
      const p = [`DRIVER={${cfg.driverName || 'MySQL ODBC 8.0 Unicode Driver'}}`];
      if (cfg.host)     p.push(`SERVER=${cfg.host}`);
      if (cfg.port)     p.push(`PORT=${cfg.port || 3306}`);
      if (cfg.database) p.push(`DATABASE=${cfg.database}`);
      if (cfg.username) p.push(`UID=${cfg.username}`);
      if (cfg.password) p.push(`PWD=${cfg.password}`);
      return p.join(';') + ';';
    },
    setSchema: `USE ?`,
    listTablesSql(schema) {
      return {
        sql: `SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
              FROM information_schema.TABLES
              WHERE TABLE_SCHEMA = ?
              ORDER BY TABLE_NAME`,
        params: [schema],
      };
    },
    describeSQL(schema, table) {
      return {
        sql: `SELECT COLUMN_NAME, DATA_TYPE,
                     CHARACTER_MAXIMUM_LENGTH AS LENGTH,
                     NUMERIC_SCALE,
                     IS_NULLABLE,
                     COLUMN_DEFAULT
              FROM information_schema.COLUMNS
              WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
              ORDER BY ORDINAL_POSITION`,
        params: [schema, table],
      };
    },
    primaryKeysSQL(schema, table) {
      return {
        sql: `SELECT kcu.COLUMN_NAME
              FROM information_schema.TABLE_CONSTRAINTS tc
              JOIN information_schema.KEY_COLUMN_USAGE kcu
                ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
               AND tc.TABLE_SCHEMA    = kcu.TABLE_SCHEMA
               AND tc.TABLE_NAME      = kcu.TABLE_NAME
              WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                AND tc.TABLE_SCHEMA = ? AND tc.TABLE_NAME = ?
              ORDER BY kcu.ORDINAL_POSITION`,
        params: [schema, table],
      };
    },
    paginateSQL(inner, offset, limit) {
      return `SELECT * FROM (${inner}) AS _p LIMIT ${limit} OFFSET ${offset}`;
    },
    quoteId: id => `\`${id}\``,
  },

  // ── Oracle ─────────────────────────────────────────────────────────────────
  oracle: {
    label: 'Oracle',
    buildConnStr(cfg) {
      if (cfg.connectionString) return cfg.connectionString;
      const p = [`DRIVER={${cfg.driverName || 'Oracle 21 ODBC driver'}}`];
      // DBQ accepts EZConnect: host[:port]/serviceName
      const port    = cfg.port || 1521;
      const service = cfg.serviceName || cfg.database || 'ORCL';
      p.push(`DBQ=${cfg.host}:${port}/${service}`);
      if (cfg.username) p.push(`UID=${cfg.username}`);
      if (cfg.password) p.push(`PWD=${cfg.password}`);
      return p.join(';') + ';';
    },
    setSchema: `ALTER SESSION SET CURRENT_SCHEMA = ?`,
    listTablesSql(schema) {
      return {
        sql: `SELECT OWNER AS TABLE_SCHEMA, TABLE_NAME, 'TABLE' AS TABLE_TYPE
              FROM ALL_TABLES
              WHERE OWNER = ?
              ORDER BY TABLE_NAME`,
        params: [schema.toUpperCase()],
      };
    },
    describeSQL(schema, table) {
      return {
        sql: `SELECT COLUMN_NAME, DATA_TYPE,
                     DATA_LENGTH AS LENGTH,
                     DATA_SCALE  AS NUMERIC_SCALE,
                     NULLABLE    AS IS_NULLABLE,
                     DATA_DEFAULT AS COLUMN_DEFAULT
              FROM ALL_TAB_COLUMNS
              WHERE OWNER = ? AND TABLE_NAME = ?
              ORDER BY COLUMN_ID`,
        params: [schema.toUpperCase(), table.toUpperCase()],
      };
    },
    primaryKeysSQL(schema, table) {
      return {
        sql: `SELECT acc.COLUMN_NAME
              FROM ALL_CONSTRAINTS ac
              JOIN ALL_CONS_COLUMNS acc
                ON ac.CONSTRAINT_NAME = acc.CONSTRAINT_NAME
               AND ac.OWNER           = acc.OWNER
              WHERE ac.CONSTRAINT_TYPE = 'P'
                AND ac.OWNER = ? AND ac.TABLE_NAME = ?
              ORDER BY acc.POSITION`,
        params: [schema.toUpperCase(), table.toUpperCase()],
      };
    },
    paginateSQL(inner, offset, limit) {
      // Oracle 12c+ ROW_LIMITING
      return `SELECT * FROM (${inner}) FETCH FIRST ${limit} ROWS ONLY` +
             (offset > 0 ? ` OFFSET ${offset} ROWS` : '');
    },
    quoteId: id => `"${id}"`,
  },

  // ── DB2 LUW (Linux / Windows / AIX) ───────────────────────────────────────
  db2: {
    label: 'DB2 LUW',
    buildConnStr(cfg) {
      if (cfg.connectionString) return cfg.connectionString;
      const p = [`DRIVER={${cfg.driverName || 'IBM DB2 ODBC DRIVER'}}`];
      if (cfg.database) p.push(`DATABASE=${cfg.database}`);
      if (cfg.host)     p.push(`HOSTNAME=${cfg.host}`);
      if (cfg.port)     p.push(`PORT=${cfg.port || 50000}`);
      if (cfg.username) p.push(`UID=${cfg.username}`);
      if (cfg.password) p.push(`PWD=${cfg.password}`);
      p.push(`PROTOCOL=TCPIP`);
      return p.join(';') + ';';
    },
    setSchema: `SET SCHEMA ?`,
    listTablesSql(schema) {
      return {
        sql: `SELECT TABSCHEMA AS TABLE_SCHEMA, TABNAME AS TABLE_NAME,
                     TYPE AS TABLE_TYPE
              FROM SYSCAT.TABLES
              WHERE TABSCHEMA = ?
              ORDER BY TABNAME`,
        params: [schema.toUpperCase()],
      };
    },
    describeSQL(schema, table) {
      return {
        sql: `SELECT COLNAME AS COLUMN_NAME, TYPENAME AS DATA_TYPE,
                     LENGTH, SCALE AS NUMERIC_SCALE,
                     NULLS AS IS_NULLABLE, DEFAULT AS COLUMN_DEFAULT
              FROM SYSCAT.COLUMNS
              WHERE TABSCHEMA = ? AND TABNAME = ?
              ORDER BY COLNO`,
        params: [schema.toUpperCase(), table.toUpperCase()],
      };
    },
    primaryKeysSQL(schema, table) {
      return {
        sql: `SELECT kc.COLNAME AS COLUMN_NAME
              FROM SYSCAT.TABCONST tc
              JOIN SYSCAT.KEYCOLUSE kc
                ON tc.CONSTNAME = kc.CONSTNAME
               AND tc.TABSCHEMA = kc.TABSCHEMA
               AND tc.TABNAME   = kc.TABNAME
              WHERE tc.TYPE = 'P'
                AND tc.TABSCHEMA = ? AND tc.TABNAME = ?
              ORDER BY kc.COLSEQ`,
        params: [schema.toUpperCase(), table.toUpperCase()],
      };
    },
    paginateSQL(inner, offset, limit) {
      if (offset > 0) {
        return `SELECT * FROM (${inner}) AS SUBQ OFFSET ${offset} ROWS FETCH NEXT ${limit} ROWS ONLY`;
      }
      return `SELECT * FROM (${inner}) AS SUBQ FETCH FIRST ${limit} ROWS ONLY`;
    },
    quoteId: id => `"${id}"`,
  },

  // ── SQLite ─────────────────────────────────────────────────────────────────
  sqlite: {
    label: 'SQLite',
    buildConnStr(cfg) {
      if (cfg.connectionString) return cfg.connectionString;
      // For SQLite, 'host' is treated as the file path
      return `DRIVER={SQLite3 ODBC Driver};Database=${cfg.host || cfg.database};`;
    },
    setSchema: null,   // SQLite has no schema switching
    listTablesSql(_schema) {
      return {
        sql: `SELECT 'main' AS TABLE_SCHEMA, name AS TABLE_NAME, type AS TABLE_TYPE
              FROM sqlite_master
              WHERE type IN ('table','view')
              ORDER BY name`,
        params: [],
      };
    },
    describeSQL(_schema, table) {
      // PRAGMA doesn't support parameters — we sanitize manually
      return {
        sql: `PRAGMA table_info(${table.replace(/[^a-zA-Z0-9_]/g, '')})`,
        params: [],
        mapRow: row => ({
          COLUMN_NAME:    row.name,
          DATA_TYPE:      row.type,
          LENGTH:         null,
          NUMERIC_SCALE:  null,
          IS_NULLABLE:    row.notnull ? 'N' : 'Y',
          COLUMN_DEFAULT: row.dflt_value,
        }),
      };
    },
    primaryKeysSQL(_schema, table) {
      return {
        sql: `PRAGMA table_info(${table.replace(/[^a-zA-Z0-9_]/g, '')})`,
        params: [],
        mapRow: row => row.pk > 0 ? { COLUMN_NAME: row.name } : null,
      };
    },
    paginateSQL(inner, offset, limit) {
      return `SELECT * FROM (${inner}) AS _p LIMIT ${limit} OFFSET ${offset}`;
    },
    quoteId: id => `"${id}"`,
  },

};

// ─── public helpers ───────────────────────────────────────────────────────────

/** Return driver definition or throw. */
function getDriver(type) {
  const key = (type || 'ibmi').toLowerCase();
  const d   = DRIVERS[key];
  if (!d) {
    const available = Object.keys(DRIVERS).join(', ');
    throw new Error(`Unknown database type "${type}". Available: ${available}`);
  }
  return d;
}

/** List all supported types with labels. */
function listDrivers() {
  return Object.entries(DRIVERS).map(([type, d]) => ({ type, label: d.label }));
}

module.exports = { DRIVERS, getDriver, listDrivers };
