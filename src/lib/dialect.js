'use strict';

/**
 * Dialect — per-database SQL generation rules.
 *
 * A Dialect instance wraps all the SQL syntax differences between databases:
 *   - identifier quoting
 *   - value literals (timestamps, booleans, etc.)
 *   - upsert strategy (MERGE / INSERT OR REPLACE / ON CONFLICT / ON DUPLICATE KEY)
 *   - DDL type mapping (source type string → target DDL type string)
 *
 * Usage:
 *   const d = Dialect.for('postgresql');
 *   d.quoteId('order date')          → '"order date"'
 *   d.literal(new Date('2024-01-15')) → "'2024-01-15'::date"
 *   d.upsert(table, cols, keys, vals) → "INSERT INTO ... ON CONFLICT ..."
 *   d.mapType('CHARACTER', 5, 0)      → 'VARCHAR(5)'
 */

// ─── Literal helpers (shared building blocks) ────────────────────────────────

function _dateOnly(d) {
  return d.toISOString().slice(0, 10);
}
function _isoTs(d) {
  return d.toISOString().replace('T', ' ').replace('Z', '');
}
function _isDateOnly(d) {
  return d.getUTCHours() === 0 && d.getUTCMinutes() === 0 &&
         d.getUTCSeconds() === 0 && d.getUTCMilliseconds() === 0;
}
function _strEsc(s) {
  return `'${String(s).replace(/'/g, "''")}'`;
}

// ─── Dialect definitions ─────────────────────────────────────────────────────

const DIALECT_DEFS = {

  // ── IBM i / DB2 for i ────────────────────────────────────────────────────
  ibmi: {
    quoteId:  id => `"${id}"`,

    literal(val) {
      if (val === null || val === undefined) return 'NULL';
      if (typeof val === 'number')  return String(val);
      if (typeof val === 'boolean') return val ? '1' : '0';  // DB2 no native BOOLEAN in older releases
      if (val instanceof Date) {
        return _isDateOnly(val)
          ? _strEsc(_dateOnly(val))
          : `TIMESTAMP '${_isoTs(val)}'`;
      }
      return _strEsc(val);
    },

    // MERGE INTO T USING (VALUES (...)) AS S (...) ON (...) WHEN MATCHED … WHEN NOT MATCHED …
    upsertSyntax: 'merge-standard',

    upsert(table, cols, keys, vals, quoteId) {
      return _mergeSQLStandard(table, cols, keys, vals, quoteId || this.quoteId,
        v => this.literal(v));
    },

    mapType(srcType, len, scale) {
      return _mapTypeDB2(srcType, len, scale);
    },

    nullableSuffix: col => col.IS_NULLABLE === 'N' ? 'NOT NULL' : 'NULL',
  },

  // ── DB2 LUW ──────────────────────────────────────────────────────────────
  db2: {
    quoteId:  id => `"${id}"`,

    literal(val) {
      if (val === null || val === undefined) return 'NULL';
      if (typeof val === 'number')  return String(val);
      if (typeof val === 'boolean') return val ? 'TRUE' : 'FALSE';
      if (val instanceof Date) {
        return _isDateOnly(val)
          ? `DATE '${_dateOnly(val)}'`
          : `TIMESTAMP '${_isoTs(val)}'`;
      }
      return _strEsc(val);
    },

    upsertSyntax: 'merge-standard',
    upsert(table, cols, keys, vals, quoteId) {
      return _mergeSQLStandard(table, cols, keys, vals, quoteId || this.quoteId,
        v => this.literal(v));
    },

    mapType(srcType, len, scale) {
      return _mapTypeDB2(srcType, len, scale);
    },

    nullableSuffix: col => col.IS_NULLABLE === 'N' ? 'NOT NULL' : 'NULL',
  },

  // ── SQL Server ───────────────────────────────────────────────────────────
  sqlserver: {
    quoteId:  id => `[${id}]`,

    literal(val) {
      if (val === null || val === undefined) return 'NULL';
      if (typeof val === 'number')  return String(val);
      if (typeof val === 'boolean') return val ? '1' : '0';
      if (val instanceof Date) {
        return _isDateOnly(val)
          ? _strEsc(_dateOnly(val))
          : `CONVERT(datetime2, '${_isoTs(val)}', 121)`;
      }
      return _strEsc(val);
    },

    // SQL Server supports standard MERGE syntax
    upsertSyntax: 'merge-standard',
    upsert(table, cols, keys, vals, quoteId) {
      return _mergeSQLStandard(table, cols, keys, vals, quoteId || this.quoteId,
        v => this.literal(v));
    },

    mapType(srcType, len, scale) {
      const t = srcType.toUpperCase();
      const l = len || 0;
      const s = scale || 0;
      switch (t) {
        case 'CHARACTER':
        case 'CHAR':           return `NCHAR(${l || 1})`;
        case 'VARCHAR':
        case 'CHARACTER VARYING': return `NVARCHAR(${Math.min(l || 1, 4000)})`;
        case 'CLOB':           return 'NVARCHAR(MAX)';
        case 'SMALLINT':       return 'SMALLINT';
        case 'INTEGER':
        case 'INT':            return 'INT';
        case 'BIGINT':         return 'BIGINT';
        case 'DECIMAL':
        case 'NUMERIC':        return `DECIMAL(${l || 18},${s})`;
        case 'REAL':
        case 'FLOAT':          return 'REAL';
        case 'DOUBLE':
        case 'DOUBLE PRECISION': return 'FLOAT';
        case 'DATE':           return 'DATE';
        case 'TIME':           return 'TIME';
        case 'TIMESTAMP':      return 'DATETIME2';
        case 'BINARY':         return `BINARY(${l || 1})`;
        case 'VARBINARY':      return `VARBINARY(${Math.min(l || 1, 8000)})`;
        case 'BLOB':           return 'VARBINARY(MAX)';
        case 'BOOLEAN':        return 'BIT';
        default:               return t || 'NVARCHAR(256)';
      }
    },

    nullableSuffix: col => col.IS_NULLABLE === 'N' ? 'NOT NULL' : 'NULL',
  },

  // ── PostgreSQL ───────────────────────────────────────────────────────────
  postgresql: {
    quoteId:  id => `"${id}"`,

    literal(val) {
      if (val === null || val === undefined) return 'NULL';
      if (typeof val === 'number')  return String(val);
      if (typeof val === 'boolean') return val ? 'TRUE' : 'FALSE';
      if (val instanceof Date) {
        return _isDateOnly(val)
          ? `'${_dateOnly(val)}'::date`
          : `'${_isoTs(val)}'::timestamp`;
      }
      return _strEsc(val);
    },

    // PostgreSQL 9.5+: INSERT ... ON CONFLICT (keys) DO UPDATE SET ...
    upsertSyntax: 'on-conflict',
    upsert(table, cols, keys, vals, quoteId) {
      const qi      = quoteId || this.quoteId;
      const colList = cols.map(qi).join(', ');
      const valList = vals.map(v => this.literal(v)).join(', ');
      const keyList = keys.map(qi).join(', ');
      const keySet  = new Set(keys.map(k => k.toUpperCase()));
      const updates = cols
        .filter(c => !keySet.has(c.toUpperCase()))
        .map(c => `${qi(c)} = EXCLUDED.${qi(c)}`)
        .join(', ');

      let sql = `INSERT INTO ${table} (${colList})\nVALUES (${valList})\n` +
                `ON CONFLICT (${keyList})`;
      if (updates) {
        sql += `\nDO UPDATE SET ${updates}`;
      } else {
        sql += `\nDO NOTHING`;
      }
      return sql;
    },

    mapType(srcType, len, scale) {
      const t = srcType.toUpperCase();
      const l = len || 0;
      const s = scale || 0;
      switch (t) {
        case 'CHARACTER':
        case 'CHAR':           return `CHAR(${l || 1})`;
        case 'VARCHAR':
        case 'CHARACTER VARYING': return `VARCHAR(${l || 1})`;
        case 'CLOB':           return 'TEXT';
        case 'SMALLINT':       return 'SMALLINT';
        case 'INTEGER':
        case 'INT':            return 'INTEGER';
        case 'BIGINT':         return 'BIGINT';
        case 'DECIMAL':
        case 'NUMERIC':        return `NUMERIC(${l || 18},${s})`;
        case 'REAL':
        case 'FLOAT':          return 'REAL';
        case 'DOUBLE':
        case 'DOUBLE PRECISION': return 'DOUBLE PRECISION';
        case 'DATE':           return 'DATE';
        case 'TIME':           return 'TIME';
        case 'TIMESTAMP':      return 'TIMESTAMP';
        case 'BINARY':
        case 'VARBINARY':
        case 'BLOB':           return 'BYTEA';
        case 'BOOLEAN':        return 'BOOLEAN';
        default:               return t || 'TEXT';
      }
    },

    nullableSuffix: col => col.IS_NULLABLE === 'N' ? 'NOT NULL' : 'NULL',
  },

  // ── MySQL / MariaDB ──────────────────────────────────────────────────────
  mysql: {
    quoteId:  id => `\`${id}\``,

    literal(val) {
      if (val === null || val === undefined) return 'NULL';
      if (typeof val === 'number')  return String(val);
      if (typeof val === 'boolean') return val ? 'TRUE' : 'FALSE';
      if (val instanceof Date) {
        return _isDateOnly(val)
          ? _strEsc(_dateOnly(val))
          : _strEsc(_isoTs(val));   // MySQL accepts ISO string in quotes
      }
      return _strEsc(val);
    },

    // MySQL: INSERT ... ON DUPLICATE KEY UPDATE col=VALUES(col), ...
    upsertSyntax: 'on-duplicate-key',
    upsert(table, cols, keys, vals, quoteId) {
      const qi      = quoteId || this.quoteId;
      const colList = cols.map(qi).join(', ');
      const valList = vals.map(v => this.literal(v)).join(', ');
      const keySet  = new Set(keys.map(k => k.toUpperCase()));
      const updates = cols
        .filter(c => !keySet.has(c.toUpperCase()))
        .map(c => `${qi(c)} = VALUES(${qi(c)})`)
        .join(', ');

      let sql = `INSERT INTO ${table} (${colList})\nVALUES (${valList})`;
      if (updates) sql += `\nON DUPLICATE KEY UPDATE ${updates}`;
      return sql;
    },

    mapType(srcType, len, scale) {
      const t = srcType.toUpperCase();
      const l = len || 0;
      const s = scale || 0;
      switch (t) {
        case 'CHARACTER':
        case 'CHAR':           return `CHAR(${l || 1})`;
        case 'VARCHAR':
        case 'CHARACTER VARYING': return `VARCHAR(${Math.min(l || 1, 65535)})`;
        case 'CLOB':           return 'LONGTEXT';
        case 'SMALLINT':       return 'SMALLINT';
        case 'INTEGER':
        case 'INT':            return 'INT';
        case 'BIGINT':         return 'BIGINT';
        case 'DECIMAL':
        case 'NUMERIC':        return `DECIMAL(${l || 18},${s})`;
        case 'REAL':
        case 'FLOAT':          return 'FLOAT';
        case 'DOUBLE':
        case 'DOUBLE PRECISION': return 'DOUBLE';
        case 'DATE':           return 'DATE';
        case 'TIME':           return 'TIME';
        case 'TIMESTAMP':      return 'DATETIME';
        case 'BINARY':         return `BINARY(${l || 1})`;
        case 'VARBINARY':      return `VARBINARY(${Math.min(l || 1, 65535)})`;
        case 'BLOB':           return 'LONGBLOB';
        case 'BOOLEAN':        return 'TINYINT(1)';
        default:               return t || 'VARCHAR(255)';
      }
    },

    nullableSuffix: col => col.IS_NULLABLE === 'N' ? 'NOT NULL' : 'NULL',
  },

  // ── Oracle ───────────────────────────────────────────────────────────────
  oracle: {
    quoteId:  id => `"${id}"`,

    literal(val) {
      if (val === null || val === undefined) return 'NULL';
      if (typeof val === 'number')  return String(val);
      if (typeof val === 'boolean') return val ? '1' : '0';
      if (val instanceof Date) {
        return _isDateOnly(val)
          ? `DATE '${_dateOnly(val)}'`
          : `TIMESTAMP '${_isoTs(val)}'`;
      }
      return _strEsc(val);
    },

    // Oracle 11g+: MERGE INTO T USING (SELECT ... FROM DUAL) AS S ON (...) ...
    upsertSyntax: 'merge-oracle',
    upsert(table, cols, keys, vals, quoteId) {
      const qi      = quoteId || this.quoteId;
      const colList = cols.join(', ');
      const keySet  = new Set(keys.map(k => k.toUpperCase()));
      const nonKeyCols = cols.filter(c => !keySet.has(c.toUpperCase()));

      // Oracle: USING (SELECT lit AS col, ... FROM DUAL)
      const selectCols = cols
        .map((c, i) => `${this.literal(vals[i])} AS ${qi(c)}`)
        .join(', ');
      const onClause   = keys.map(k => `T.${qi(k)} = S.${qi(k)}`).join(' AND ');
      const updateSet  = nonKeyCols.map(c => `T.${qi(c)} = S.${qi(c)}`).join(', ');
      const insertCols = cols.map(qi).join(', ');
      const insertVals = cols.map(c => `S.${qi(c)}`).join(', ');

      let sql = `MERGE INTO ${table} T\n` +
                `USING (SELECT ${selectCols} FROM DUAL) S\n` +
                `ON (${onClause})\n`;
      if (updateSet) sql += `WHEN MATCHED THEN UPDATE SET ${updateSet}\n`;
      sql += `WHEN NOT MATCHED THEN INSERT (${insertCols}) VALUES (${insertVals})`;
      return sql;
    },

    mapType(srcType, len, scale) {
      const t = srcType.toUpperCase();
      const l = len || 0;
      const s = scale || 0;
      switch (t) {
        case 'CHARACTER':
        case 'CHAR':           return `CHAR(${l || 1})`;
        case 'VARCHAR':
        case 'CHARACTER VARYING': return `VARCHAR2(${Math.min(l || 1, 4000)})`;
        case 'CLOB':           return 'CLOB';
        case 'SMALLINT':
        case 'INTEGER':
        case 'INT':
        case 'BIGINT':         return 'NUMBER(18)';
        case 'DECIMAL':
        case 'NUMERIC':        return `NUMBER(${l || 18},${s})`;
        case 'REAL':
        case 'FLOAT':          return 'BINARY_FLOAT';
        case 'DOUBLE':
        case 'DOUBLE PRECISION': return 'BINARY_DOUBLE';
        case 'DATE':           return 'DATE';
        case 'TIME':           return 'VARCHAR2(8)';   // Oracle has no TIME type
        case 'TIMESTAMP':      return 'TIMESTAMP';
        case 'BINARY':
        case 'VARBINARY':      return `RAW(${Math.min(l || 1, 2000)})`;
        case 'BLOB':           return 'BLOB';
        case 'BOOLEAN':        return 'NUMBER(1)';
        default:               return t || 'VARCHAR2(256)';
      }
    },

    // Oracle uses Y/N for IS_NULLABLE in ALL_TAB_COLUMNS
    nullableSuffix: col => col.IS_NULLABLE === 'N' ? 'NOT NULL' : 'NULL',
  },

  // ── SQLite ───────────────────────────────────────────────────────────────
  sqlite: {
    quoteId:  id => `"${id}"`,

    literal(val) {
      if (val === null || val === undefined) return 'NULL';
      if (typeof val === 'number')  return String(val);
      if (typeof val === 'boolean') return val ? '1' : '0';
      if (val instanceof Date) {
        return _isDateOnly(val)
          ? _strEsc(_dateOnly(val))
          : _strEsc(_isoTs(val));
      }
      return _strEsc(val);
    },

    // SQLite: INSERT OR REPLACE INTO ...  (replaces entire row)
    // or INSERT INTO ... ON CONFLICT(keys) DO UPDATE SET ... (SQLite 3.24+)
    upsertSyntax: 'on-conflict',
    upsert(table, cols, keys, vals, quoteId) {
      const qi      = quoteId || this.quoteId;
      const colList = cols.map(qi).join(', ');
      const valList = vals.map(v => this.literal(v)).join(', ');
      const keyList = keys.map(qi).join(', ');
      const keySet  = new Set(keys.map(k => k.toUpperCase()));
      const updates = cols
        .filter(c => !keySet.has(c.toUpperCase()))
        .map(c => `${qi(c)} = excluded.${qi(c)}`)
        .join(', ');

      let sql = `INSERT INTO ${table} (${colList})\nVALUES (${valList})\n` +
                `ON CONFLICT (${keyList})`;
      sql += updates ? `\nDO UPDATE SET ${updates}` : `\nDO NOTHING`;
      return sql;
    },

    mapType(srcType, len, _scale) {
      const t = srcType.toUpperCase();
      // SQLite uses 5 storage classes — map to closest affinity
      if (['INTEGER','INT','SMALLINT','BIGINT'].includes(t))   return 'INTEGER';
      if (['REAL','FLOAT','DOUBLE','DOUBLE PRECISION'].includes(t)) return 'REAL';
      if (['DECIMAL','NUMERIC'].includes(t))                    return 'NUMERIC';
      if (['BLOB','BINARY','VARBINARY'].includes(t))            return 'BLOB';
      if (['BOOLEAN'].includes(t))                              return 'INTEGER';
      return 'TEXT';   // everything else → TEXT affinity
    },

    // SQLite: notnull comes from PRAGMA (mapped to IS_NULLABLE)
    nullableSuffix: col => col.IS_NULLABLE === 'N' ? 'NOT NULL' : '',
  },
};

// ─── Shared MERGE helper (IBM i / DB2 / SQL Server) ──────────────────────────

function _mergeSQLStandard(table, cols, keys, vals, quoteId, literalFn) {
  const keySet     = new Set(keys.map(k => k.toUpperCase()));
  const nonKeyCols = cols.filter(c => !keySet.has(c.toUpperCase()));
  const colList    = cols.join(', ');
  const valList    = vals.map(literalFn || (v => {           // literalize inline
    if (v === null || v === undefined) return 'NULL';
    if (typeof v === 'number' || typeof v === 'boolean') return String(v);
    if (v instanceof Date) {
      const iso = v.toISOString();
      return v.getUTCHours()===0 && v.getUTCMinutes()===0 && v.getUTCSeconds()===0
        ? `'${iso.slice(0,10)}'`
        : `TIMESTAMP '${iso.replace('T',' ').replace('Z','')}'`;
    }
    return `'${String(v).replace(/'/g, "''")}`;
  })).join(', ');   // literalized here
  const qi         = quoteId;

  const onClause  = keys.map(k => `T.${qi(k)} = S.${qi(k)}`).join(' AND ');
  const updateSet = nonKeyCols.map(c => `T.${qi(c)} = S.${qi(c)}`).join(', ');
  const iCols     = cols.map(qi).join(', ');
  const iVals     = cols.map(c => `S.${qi(c)}`).join(', ');

  let sql =
    `MERGE INTO ${table} AS T\n` +
    `USING (VALUES (${valList})) AS S (${colList})\n` +
    `ON (${onClause})\n`;
  if (updateSet) sql += `WHEN MATCHED THEN UPDATE SET ${updateSet}\n`;
  sql += `WHEN NOT MATCHED THEN INSERT (${iCols}) VALUES (${iVals})`;
  return sql;
}

// ─── DB2/IBM i type mapping (shared by ibmi + db2) ───────────────────────────

function _mapTypeDB2(srcType, len, scale) {
  const t = srcType.toUpperCase();
  const l = len   || 0;
  const s = scale || 0;
  switch (t) {
    case 'CHARACTER':
    case 'CHAR':           return `CHAR(${l || 1})`;
    case 'VARCHAR':
    case 'CHARACTER VARYING': return `VARCHAR(${l || 1})`;
    case 'NCHAR':          return `NCHAR(${l || 1})`;
    case 'NVARCHAR':       return `NVARCHAR(${l || 1})`;
    case 'CLOB':           return `CLOB(${l || 1048576})`;
    case 'SMALLINT':       return 'SMALLINT';
    case 'INTEGER':
    case 'INT':            return 'INTEGER';
    case 'BIGINT':         return 'BIGINT';
    case 'DECIMAL':
    case 'NUMERIC':        return `DECIMAL(${l || 15},${s})`;
    case 'REAL':
    case 'FLOAT':          return 'REAL';
    case 'DOUBLE':
    case 'DOUBLE PRECISION': return 'DOUBLE';
    case 'DATE':           return 'DATE';
    case 'TIME':           return 'TIME';
    case 'TIMESTAMP':      return 'TIMESTAMP';
    case 'BINARY':         return `BINARY(${l || 1})`;
    case 'VARBINARY':      return `VARBINARY(${l || 1})`;
    case 'BLOB':           return `BLOB(${l || 1048576})`;
    case 'BOOLEAN':        return 'BOOLEAN';
    default:               return t || 'VARCHAR(256)';
  }
}

// ─── Dialect class ────────────────────────────────────────────────────────────

class Dialect {
  constructor(type) {
    const key = (type || 'ibmi').toLowerCase();
    const def = DIALECT_DEFS[key];
    if (!def) {
      const available = Object.keys(DIALECT_DEFS).join(', ');
      throw new Error(`Unknown dialect "${type}". Available: ${available}`);
    }
    this._type = key;
    this._def  = def;
  }

  /** Quote an identifier (table name, column name). */
  quoteId(name) { return this._def.quoteId(name); }

  /** Convert a JS value to a SQL literal for this database. */
  literal(val)  { return this._def.literal.call(this._def, val); }

  /**
   * Build an upsert statement (INSERT or MERGE) for one row.
   * @param {string}   table  qualified table name
   * @param {string[]} cols   destination column names
   * @param {string[]} keys   key column names (for ON clause / CONFLICT target)
   * @param {any[]}    vals   JS values (in same order as cols)
   */
  upsert(table, cols, keys, vals) {
    // Pass raw JS values — each dialect literalizes internally as needed
    return this._def.upsert.call(this._def, table, cols, keys, vals, n => this.quoteId(n));
  }

  /**
   * Build a plain INSERT with ? placeholders.
   * Returns { sql, params } — params are the raw JS values for ODBC binding.
   */
  insert(table, cols, vals) {
    const quotedCols = cols.map(c => this.quoteId(c)).join(', ');
    const marks      = cols.map(() => '?').join(', ');
    return {
      sql:    `INSERT INTO ${table} (${quotedCols}) VALUES (${marks})`,
      params: vals,
    };
  }

  /**
   * Map a source type string to the target DDL type for this database.
   * @param {string} srcType   e.g. 'CHARACTER', 'DECIMAL', 'TIMESTAMP'
   * @param {number} len       column length / precision
   * @param {number} scale     numeric scale
   */
  mapType(srcType, len, scale) {
    return this._def.mapType(srcType, len, scale);
  }

  /**
   * Return the NULL / NOT NULL suffix for a column descriptor row.
   * Input row must have IS_NULLABLE field ('Y'/'N' or 'YES'/'NO').
   */
  nullableSuffix(col) {
    // Normalize YES/NO → Y/N
    const raw = String(col.IS_NULLABLE || 'Y').toUpperCase();
    const norm = { ...col, IS_NULLABLE: (raw === 'NO' || raw === 'N') ? 'N' : 'Y' };
    return this._def.nullableSuffix(norm);
  }

  get type()          { return this._type; }
  get upsertSyntax()  { return this._def.upsertSyntax; }

  /** Factory: get a Dialect instance for a DB type string. */
  static for(type) {
    return new Dialect(type || 'ibmi');
  }

  /** List all available dialect types. */
  static list() {
    return Object.keys(DIALECT_DEFS);
  }
}

module.exports = { Dialect };
