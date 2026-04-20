'use strict';

const fs   = require('fs');
const path = require('path');

// ─── public constants (error modes) ──────────────────────────────────────────

const ERROR_MODE = {
  ABORT:     'abort',     // rollback everything on first error
  SKIP:      'skip',      // log error, skip row, continue
  CONFIRM:   'confirm',   // ask user interactively before continuing
};

// ─── ImportResult ─────────────────────────────────────────────────────────────

class ImportResult {
  constructor() {
    this.total      = 0;
    this.inserted   = 0;
    this.skipped    = 0;
    this.errors     = [];   // [{ row, sql, error }]
    this.dryRun     = false;
    this.elapsed    = 0;
  }

  summary() {
    const status = this.errors.length === 0 ? '✓' : '⚠';
    const dryTag = this.dryRun ? ' [DRY RUN]' : '';
    return (
      `${status} Import complete${dryTag}  ` +
      `total=${this.total}  inserted=${this.inserted}  ` +
      `skipped=${this.skipped}  errors=${this.errors.length}  ` +
      `elapsed=${this.elapsed}ms`
    );
  }
}

// ─── Importer class ───────────────────────────────────────────────────────────

/**
 * opts:
 *   table       {string}    target table  e.g. "MYLIB.ORDERS"  (required for CSV/JSON)
 *   columns     {string[]}  ordered column names (CSV only, overrides header row)
 *   mapping     {object}    { csvHeader: dbColumn, ... }  rename/filter columns
 *   batchSize   {number}    rows per ODBC execute call (default 100)
 *   errorMode   {string}    'abort' | 'skip' | 'confirm'   (default 'abort')
 *   dryRun      {boolean}   parse + validate without sending to DB (default false)
 *   delimiter   {string}    CSV delimiter (default ',')
 *   encoding    {string}    file encoding (default 'utf8')
 *   onProgress  {function}  (current, total, lastSql) callback
 *   onConfirm   {function}  async (rowIndex, error, sql) → boolean  (confirm mode)
 *   logger      {object}    { info, warn, error } — defaults to console
 */
class Importer {
  constructor(conn, opts = {}) {
    this.conn      = conn;
    this.table     = opts.table     || null;
    this.columns   = opts.columns   || null;   // forced column list (CSV)
    this.mapping   = opts.mapping   || {};      // { srcCol: destCol }
    this.batchSize = opts.batchSize || 100;
    this.errorMode = opts.errorMode || ERROR_MODE.ABORT;
    this.dryRun    = opts.dryRun    || false;
    this.delimiter = opts.delimiter || ',';
    this.encoding  = opts.encoding  || 'utf8';
    this.onProgress = opts.onProgress || null;
    this.onConfirm  = opts.onConfirm  || null;
    this.log = opts.logger || {
      info:  (...a) => console.log(...a),
      warn:  (...a) => console.warn(...a),
      error: (...a) => console.error(...a),
    };
  }

  // ── public entry points ────────────────────────────────────────────────────

  /**
   * Import from file. Format auto-detected from extension.
   * .csv / .tsv  → importCSV
   * .json        → importJSON
   * .sql / .insert.sql / .merge.sql → importSQL
   */
  async importFile(filePath) {
    if (!fs.existsSync(filePath)) {
      throw new Error(`File not found: ${filePath}`);
    }

    const ext  = path.extname(filePath).toLowerCase();
    const base = path.basename(filePath).toLowerCase();

    if (ext === '.csv' || ext === '.tsv') {
      if (ext === '.tsv') this.delimiter = '\t';
      return this.importCSV(filePath);
    }
    if (ext === '.json') {
      return this.importJSON(filePath);
    }
    if (ext === '.sql' || base.endsWith('.insert.sql') || base.endsWith('.merge.sql')) {
      return this.importSQL(filePath);
    }

    throw new Error(
      `Unsupported file format: ${ext}. Supported: .csv, .tsv, .json, .sql, .insert.sql, .merge.sql`
    );
  }

  // ── CSV import ─────────────────────────────────────────────────────────────

  async importCSV(filePath) {
    if (!this.table) throw new Error('opts.table is required for CSV import.');

    const raw   = fs.readFileSync(filePath, this.encoding);
    const lines = _splitLines(raw);
    if (lines.length === 0) return this._emptyResult();

    // Parse header
    const rawHeader = _parseCSVLine(lines[0], this.delimiter);
    const srcCols   = this.columns || rawHeader;

    // Apply mapping: srcCol → destCol  (unmapped cols kept as-is unless not in mapping at all)
    const destCols = srcCols.map(c => this.mapping[c] || c);

    // Filter out cols mapped to null/false (explicit exclusion)
    const colPairs = srcCols
      .map((src, i) => ({ src, dest: this.mapping[src] !== undefined ? this.mapping[src] : src, idx: i }))
      .filter(p => p.dest);   // mapping to '' or null excludes the column

    const dataLines = lines.slice(1).filter(l => l.trim() !== '');
    const total     = dataLines.length;
    const result    = new ImportResult();
    result.total    = total;
    result.dryRun   = this.dryRun;

    const start = Date.now();

    // Build parametric INSERT: INSERT INTO T (c1,c2,...) VALUES (?,?,...)
    const insertSQL = _buildInsertSQL(
      this.table,
      colPairs.map(p => p.dest)
    );

    const batches = _chunkArray(dataLines, this.batchSize);

    for (const batch of batches) {
      for (const line of batch) {
        result.total; // already set above
        const cells  = _parseCSVLine(line, this.delimiter);
        const values = colPairs.map(p => {
          const v = cells[p.idx];
          return (v === '' || v === undefined) ? null : v;
        });

        const ok = await this._executeRow(insertSQL, values, result, line);
        if (!ok && this.errorMode === ERROR_MODE.ABORT) break;
        if (ok) result.inserted++;
      }

      this._emitProgress(result.inserted + result.skipped, total, insertSQL);
      if (result.errors.length > 0 && this.errorMode === ERROR_MODE.ABORT) break;
    }

    result.elapsed = Date.now() - start;
    return result;
  }

  // ── JSON import ────────────────────────────────────────────────────────────

  async importJSON(filePath) {
    const raw  = fs.readFileSync(filePath, this.encoding);
    let parsed;
    try { parsed = JSON.parse(raw); } catch (e) {
      throw new Error(`Invalid JSON: ${e.message}`);
    }

    // Accept both { rows: [...] }  and  plain array
    const rows = Array.isArray(parsed) ? parsed
      : Array.isArray(parsed.rows)     ? parsed.rows
      : null;

    if (!rows) throw new Error('JSON must be an array or an object with a "rows" array.');
    if (rows.length === 0) return this._emptyResult();

    // Determine table: opts.table or from JSON metadata
    const table = this.table || parsed.table || null;
    if (!table) throw new Error('opts.table is required (or include "table" key in JSON).');

    // Determine columns from first row, apply mapping
    const srcCols  = Object.keys(rows[0]);
    const colPairs = srcCols
      .map(src => ({ src, dest: this.mapping[src] !== undefined ? this.mapping[src] : src }))
      .filter(p => p.dest);

    const insertSQL = _buildInsertSQL(table, colPairs.map(p => p.dest));
    const total     = rows.length;
    const result    = new ImportResult();
    result.total    = total;
    result.dryRun   = this.dryRun;
    const start     = Date.now();

    const batches = _chunkArray(rows, this.batchSize);

    for (const batch of batches) {
      for (const row of batch) {
        const values = colPairs.map(p => {
          const v = row[p.src];
          return (v === undefined || v === null) ? null : v;
        });

        const ok = await this._executeRow(insertSQL, values, result, JSON.stringify(row));
        if (!ok && this.errorMode === ERROR_MODE.ABORT) break;
        if (ok) result.inserted++;
      }

      this._emitProgress(result.inserted + result.skipped, total, insertSQL);
      if (result.errors.length > 0 && this.errorMode === ERROR_MODE.ABORT) break;
    }

    result.elapsed = Date.now() - start;
    return result;
  }

  // ── SQL import ─────────────────────────────────────────────────────────────

  /**
   * Execute a .sql file statement by statement.
   * Handles both INSERT and MERGE syntax.
   * Statements are split on ';' (respecting quoted strings).
   */
  async importSQL(filePath) {
    const raw        = fs.readFileSync(filePath, this.encoding);
    const statements = _splitSQLStatements(raw);

    if (statements.length === 0) return this._emptyResult();

    const total  = statements.length;
    const result = new ImportResult();
    result.total = total;
    result.dryRun = this.dryRun;
    const start  = Date.now();

    const batches = _chunkArray(statements, this.batchSize);

    for (const batch of batches) {
      for (const sql of batch) {
        const trimmed = sql.trim();
        if (!trimmed) { result.skipped++; continue; }

        const ok = await this._executeRow(trimmed, [], result, trimmed);
        if (!ok && this.errorMode === ERROR_MODE.ABORT) break;
        if (ok) result.inserted++;
      }

      this._emitProgress(result.inserted + result.skipped, total, '');
      if (result.errors.length > 0 && this.errorMode === ERROR_MODE.ABORT) break;
    }

    result.elapsed = Date.now() - start;
    return result;
  }

  // ── internals ──────────────────────────────────────────────────────────────

  /**
   * Execute one statement (or skip / confirm depending on errorMode).
   * Returns true if the row was successfully processed.
   */
  async _executeRow(sql, params, result, rawRow) {
    if (this.dryRun) {
      // In dry-run just validate params count matches placeholders
      const placeholders = (sql.match(/\?/g) || []).length;
      if (params.length !== placeholders) {
        const err = `Parameter count mismatch: expected ${placeholders}, got ${params.length}`;
        result.errors.push({ row: rawRow, sql, error: err });
        result.skipped++;
        return false;
      }
      return true;
    }

    try {
      await this.conn.execute(sql, params);
      return true;
    } catch (err) {
      const odbcDetail = err.odbcErrors
        ? ' ' + err.odbcErrors.map(e => `[${e.state}] ${e.message}`).join(' ')
        : '';
      const errMsg = (err.message || String(err)) + odbcDetail;

      if (this.errorMode === ERROR_MODE.ABORT) {
        result.errors.push({ row: rawRow, sql, error: errMsg });
        return false;
      }

      if (this.errorMode === ERROR_MODE.SKIP) {
        result.errors.push({ row: rawRow, sql, error: errMsg });
        result.skipped++;
        this.log.warn(`  ⚠ Row skipped — ${errMsg}`);
        return false;
      }

      if (this.errorMode === ERROR_MODE.CONFIRM) {
        result.errors.push({ row: rawRow, sql, error: errMsg });
        if (this.onConfirm) {
          const proceed = await this.onConfirm(result.inserted + result.skipped + 1, errMsg, sql);
          if (proceed) {
            result.skipped++;
            return false;   // skip this row, continue
          } else {
            return false;   // caller will abort
          }
        }
        result.skipped++;
        return false;
      }

      return false;
    }
  }

  _emitProgress(done, total, lastSql) {
    if (this.onProgress) {
      this.onProgress(done, total, lastSql);
    }
  }

  _emptyResult() {
    const r = new ImportResult();
    r.total   = 0;
    r.dryRun  = this.dryRun;
    r.elapsed = 0;
    return r;
  }
}

// ─── private helpers ──────────────────────────────────────────────────────────

function _buildInsertSQL(table, destCols) {
  const cols  = destCols.join(', ');
  const marks = destCols.map(() => '?').join(', ');
  return `INSERT INTO ${table} (${cols}) VALUES (${marks})`;
}

function _chunkArray(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

function _splitLines(text) {
  return text.split(/\r?\n/);
}

/**
 * Parse a single CSV line, respecting double-quoted fields.
 */
function _parseCSVLine(line, delimiter = ',') {
  const result = [];
  let cur      = '';
  let inQuote  = false;

  for (let i = 0; i < line.length; i++) {
    const ch   = line[i];
    const next = line[i + 1];

    if (inQuote) {
      if (ch === '"' && next === '"') { cur += '"'; i++; }
      else if (ch === '"')            { inQuote = false; }
      else                            { cur += ch; }
    } else {
      if (ch === '"')           { inQuote = true; }
      else if (ch === delimiter){ result.push(cur); cur = ''; }
      else                      { cur += ch; }
    }
  }
  result.push(cur);
  return result;
}

/**
 * Split SQL text into individual statements on ';',
 * ignoring semicolons inside single-quoted strings.
 * Skips comment lines (-- ...).
 */
function _splitSQLStatements(text) {
  const stmts  = [];
  let   cur    = '';
  let   inStr  = false;

  for (let i = 0; i < text.length; i++) {
    const ch   = text[i];
    const next = text[i + 1];

    if (inStr) {
      cur += ch;
      if (ch === "'" && next === "'") { cur += next; i++; }
      else if (ch === "'")            { inStr = false; }
    } else {
      if (ch === "'") {
        inStr = true;
        cur   += ch;
      } else if (ch === '-' && next === '-') {
        // skip to end of line
        while (i < text.length && text[i] !== '\n') i++;
      } else if (ch === ';') {
        const s = cur.trim();
        if (s) stmts.push(s);
        cur = '';
      } else {
        cur += ch;
      }
    }
  }

  const last = cur.trim();
  if (last) stmts.push(last);

  return stmts;
}

module.exports = { Importer, ImportResult, ERROR_MODE };
