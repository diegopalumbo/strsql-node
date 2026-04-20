'use strict';

require('dotenv').config();

const readline = require('readline');
const chalk    = require('chalk');
const path     = require('path');

const { IBMiConnection }  = require('../lib/connection');
const { ProfileManager }  = require('../lib/profiles');
const { HistoryManager }  = require('../lib/history');
const { formatTable, formatExecResult, exportToFile, toInsert, toMerge } = require('../lib/formatter');
const { Importer, ERROR_MODE } = require('../lib/importer');
const { Pipe, generateDDL }    = require('../lib/pipe');
const { listDrivers }          = require('../lib/drivers');
const { ProgressBar }          = require('./progress');

// ─── helpers ─────────────────────────────────────────────────────────────────

function _splitArgs(line) {
  const tokens = [];
  let cur = '';
  let quote = null;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (quote) {
      if (ch === quote) { quote = null; }
      else { cur += ch; }
    } else if (ch === '"' || ch === "'") {
      quote = ch;
    } else if (/\s/.test(ch)) {
      if (cur) { tokens.push(cur.replace(/;+$/, '')); cur = ''; }
    } else {
      cur += ch;
    }
  }
  if (cur) tokens.push(cur.replace(/;+$/, ''));
  return tokens;
}

// ─── constants ───────────────────────────────────────────────────────────────

const PROMPT_IDLE    = chalk.green('SQL> ');
const PROMPT_CONT    = chalk.yellow('  -> ');
const VERSION        = require('../../package.json').version;

const BANNER = chalk.bold.green(`
 ╔═══════════════════════════════════════════╗
 ║   strsql-node  v${VERSION.padEnd(26)}║
 ║   Interactive SQL via ODBC and more...    ║
 ╚═══════════════════════════════════════════╝
`) + chalk.dim(' Type \\help for commands, \\quit to exit.\n');

// ─── REPL class ──────────────────────────────────────────────────────────────

class STRSQLSession {
  constructor(opts = {}) {
    this.opts = opts;
    this.conn = null;
    this.history = new HistoryManager();
    this.profiles = new ProfileManager();
    this.buffer = [];       // multi-line SQL buffer
    this.lastResult = null; // for post-query export
    this.rl = null;
  }

  // ── bootstrap ──────────────────────────────────────────────────────────────

  async start(profileName) {
    console.log(BANNER);

    // Resolve connection config
    let config;
    try {
      config = this.profiles.resolve(profileName || this.opts.profile);
    } catch (e) {
      config = this.profiles.resolve(null); // fall back to ENV only
    }

    if (!config.host) {
      console.log(chalk.yellow('No connection profile or STRSQL_HOST env set.'));
      console.log(chalk.dim(' Use \\connect <host> [user] [password] or set up a profile first.\n'));
    } else {
      await this._connect(config);
    }

    this._startREPL();
  }

  async _connect(config) {
    if (!config.host) {
      console.error(chalk.red('Connection failed: host is missing. Re-save the profile with --host.'));
      return;
    }
    const label = config.host + (config.username ? `@${config.username}` : '');
    process.stdout.write(chalk.dim(`Connecting to ${label}…`));
    try {
      this.conn = new IBMiConnection(config);
      await this.conn.connect();
      console.log(chalk.green(' ✓'));
      if (config.defaultSchema) {
        console.log(chalk.dim(` Default schema: ${config.defaultSchema}`));
      }
    } catch (err) {
      console.log(chalk.red(' ✗'));
      const odbcDetail = err.odbcErrors
        ? '\n  ' + err.odbcErrors.map(e => `[${e.state}] ${e.message}`).join('\n  ')
        : '';
      console.error(chalk.red(`Connection failed: ${err.message}${odbcDetail}`));
      this.conn = null;
    }
  }

  // ── readline REPL ──────────────────────────────────────────────────────────

  _startREPL() {
    this.rl = readline.createInterface({
      input:  process.stdin,
      output: process.stdout,
      prompt: PROMPT_IDLE,
      history: this.history.forReadline(),
      historySize: 500,
      completer: this._completer.bind(this),
    });

    this.rl.prompt();

    this.rl.on('line', async (line) => {
      await this._handleLine(line);
    });

    this.rl.on('close', () => {
      console.log(chalk.dim('\nGoodbye.'));
      process.exit(0);
    });

    process.on('SIGINT', () => {
      if (this.buffer.length > 0) {
        this.buffer = [];
        console.log(chalk.dim('\n(statement cancelled)'));
        this.rl.setPrompt(PROMPT_IDLE);
        this.rl.prompt();
      } else {
        this.rl.close();
      }
    });
  }

  async _handleLine(raw) {
    const line = raw.trimEnd();

    // ── backslash meta-commands ──────────────────────────────────────────────
    if (this.buffer.length === 0 && line.startsWith('\\')) {
      await this._metaCommand(line.slice(1).trim());
      this.rl.setPrompt(PROMPT_IDLE);
      this.rl.prompt();
      return;
    }

    // ── SQL accumulation ─────────────────────────────────────────────────────
    if (line.trim() === '') {
      this.rl.prompt();
      return;
    }

    this.buffer.push(line);

    // Execute when line ends with ';'  OR is a single-word command (GO / RUN)
    const joined = this.buffer.join(' ').trim();
    const upperJoined = joined.toUpperCase();

    if (line.trimEnd().endsWith(';') || upperJoined === 'GO' || upperJoined === 'RUN') {
      const sql = joined.replace(/;$/, '').replace(/^(GO|RUN)$/i, '').trim();
      this.buffer = [];
      this.rl.setPrompt(PROMPT_IDLE);
      if (sql) await this._executeSQL(sql);
    } else {
      this.rl.setPrompt(PROMPT_CONT);
    }

    this.rl.prompt();
  }

  // ── execute SQL ────────────────────────────────────────────────────────────

  async _executeSQL(sql) {
    if (!this.conn || !this.conn.isConnected()) {
      console.error(chalk.red('Not connected. Use \\connect <host> [user] [password]'));
      return;
    }

    this.history.add(sql);

    const upper = sql.trim().toUpperCase();
    const isSelect =
      upper.startsWith('SELECT') ||
      upper.startsWith('WITH') ||
      upper.startsWith('VALUES');

    try {
      if (isSelect) {
        const result = await this.conn.query(sql);
        this.lastResult = result;
        console.log(formatTable(result, { maxCellWidth: this.opts.maxCellWidth || 40 }));
      } else {
        const result = await this.conn.execute(sql);
        this.lastResult = result;
        console.log(formatExecResult(result));
      }
    } catch (err) {
      console.error(chalk.red(`SQL Error: ${err.message}`));
      if (err.odbcErrors) {
        for (const e of err.odbcErrors) {
          console.error(chalk.dim(`  [${e.state}] ${e.message}`));
        }
      }
    }
  }

  // ── meta-commands ──────────────────────────────────────────────────────────

  async _metaCommand(cmd) {
    const [verb, ...args] = _splitArgs(cmd);

    switch (verb.toLowerCase()) {

      case 'help':
      case 'h':
        this._printHelp();
        break;

      case 'quit':
      case 'exit':
      case 'q':
        this.rl.close();
        break;

      case 'connect': {
        // \connect <host> [user] [password] [schema] [--type sqlserver|postgresql|mysql|oracle|db2|sqlite|ibmi]
        // Or: \connect --type postgresql --host h --user u --password p --database db --schema s
        let connCfg = {};
        const positional = [];
        for (let i = 0; i < args.length; i++) {
          switch (args[i]) {
            case '--type':     connCfg.type          = args[++i]; break;
            case '--host':     connCfg.host          = args[++i]; break;
            case '--user':     connCfg.username      = args[++i]; break;
            case '--password': connCfg.password      = args[++i]; break;
            case '--schema':   connCfg.defaultSchema = args[++i]; break;
            case '--database': connCfg.database      = args[++i]; break;
            case '--port':     connCfg.port          = parseInt(args[++i],10); break;
            default:           if (!args[i].startsWith('--')) positional.push(args[i]);
          }
        }
        // Positional fallback for backward compat: host user password schema
        if (positional[0] && !connCfg.host)          connCfg.host          = positional[0];
        if (positional[1] && !connCfg.username)       connCfg.username      = positional[1];
        if (positional[2] && !connCfg.password)       connCfg.password      = positional[2];
        if (positional[3] && !connCfg.defaultSchema)  connCfg.defaultSchema = positional[3];

        if (!connCfg.host && !connCfg.database) {
          console.error(chalk.red('Usage: \\connect <host> [user] [pwd] [schema] [--type TYPE]'));
          break;
        }
        if (this.conn) await this.conn.disconnect().catch(() => {});
        await this._connect(connCfg);
        break;
      }

      case 'profile': {
        // \profile <name>
        const [pname] = args;
        if (!pname) { console.error(chalk.red('Usage: \\profile <name>')); break; }
        if (this.conn) await this.conn.disconnect().catch(() => {});
        const cfg = this.profiles.resolve(pname);
        await this._connect(cfg);
        break;
      }

      case 'profiles': {
        const list = this.profiles.list();
        if (list.length === 0) { console.log(chalk.dim('No profiles saved.')); break; }
        console.log(chalk.bold('\nSaved profiles:'));
        for (const p of list) {
          const hostOrDb = p.database || p.host || '';
          console.log(
            `  ${chalk.cyan(p.name.padEnd(18))} ${chalk.yellow((p.type||'ibmi').padEnd(12))} ${hostOrDb}` +
            (p.username      ? chalk.dim(`  user=${p.username}`)  : '') +
            (p.defaultSchema ? chalk.dim(`  schema=${p.defaultSchema}`) : '')
          );
        }
        console.log();
        break;
      }

      case 'saveprofile': {
        // \saveprofile <n> --type TYPE --host H [--user U] [--password P]
        //                  [--schema S] [--database DB] [--port N]
        //                  [--instance I]  (SQL Server named instance)
        //                  [--service S]   (Oracle service name)
        //                  [--ssl MODE]    (PostgreSQL ssl mode)
        const [pname, ...pfArgs] = args;
        if (!pname) {
          console.error(chalk.red(
            'Usage: \\saveprofile <n> --type TYPE --host H [--user U] [--password P]\n' +
            '                        [--schema S] [--database DB] [--port N]'
          ));
          break;
        }
        const pfCfg = {};
        for (let i = 0; i < pfArgs.length; i++) {
          switch (pfArgs[i]) {
            case '--type':     pfCfg.type          = pfArgs[++i]; break;
            case '--host':     pfCfg.host          = pfArgs[++i]; break;
            case '--user':     pfCfg.username      = pfArgs[++i]; break;
            case '--password': pfCfg.password      = pfArgs[++i]; break;
            case '--schema':   pfCfg.defaultSchema = pfArgs[++i]; break;
            case '--database': pfCfg.database      = pfArgs[++i]; break;
            case '--port':     pfCfg.port          = parseInt(pfArgs[++i], 10); break;
            case '--instance': pfCfg.instanceName  = pfArgs[++i]; break;
            case '--service':  pfCfg.serviceName   = pfArgs[++i]; break;
            case '--ssl':      pfCfg.sslMode       = pfArgs[++i]; break;
            case '--naming':   pfCfg.namingMode    = pfArgs[++i]; break;
          }
        }
        if (!pfCfg.type) pfCfg.type = 'ibmi';
        this.profiles.set(pname, pfCfg);
        const { getDriver: _gd } = require('../lib/drivers');
        const _lbl = _gd(pfCfg.type).label;
        console.log(chalk.green(`Profile "${pname}" saved.`) + chalk.dim(` [${_lbl}]`));
        break;
      }

      case 'delprofile': {
        const [pname] = args;
        if (!pname) { console.error(chalk.red('Usage: \\delprofile <name>')); break; }
        this.profiles.remove(pname);
        console.log(chalk.green(`Profile "${pname}" deleted.`));
        break;
      }

      case 'disconnect': {
        if (this.conn) {
          await this.conn.disconnect();
          console.log(chalk.dim('Disconnected.'));
        }
        break;
      }

      case 'schema': {
        // \schema [name]  — show or set default schema
        const [schema] = args;
        if (!schema) {
          console.log(chalk.dim(`Current schema: ${this.conn?.config?.defaultSchema || '(none)'}`));
        } else {
          const setSchemaSQL = this.conn.driver?.setSchema
            ? this.conn.driver.setSchema.replace('?', schema)
            : `SET SCHEMA ${schema}`;
          await this._executeSQL(setSchemaSQL);
          if (this.conn) this.conn.config.defaultSchema = schema;
        }
        break;
      }

      case 'tables': {
        // \tables [schema]
        if (!this.conn?.isConnected()) { console.error(chalk.red('Not connected.')); break; }
        const schema = args[0] || this.conn.config.defaultSchema;
        if (!schema) { console.error(chalk.red('Specify a schema: \\tables <schema>')); break; }
        const result = await this.conn.listTables(schema);
        console.log(formatTable(result));
        break;
      }

      case 'describe':
      case 'desc': {
        // \describe [schema.]TABLE
        if (!this.conn?.isConnected()) { console.error(chalk.red('Not connected.')); break; }
        const [table, schema] = args;
        if (!table) { console.error(chalk.red('Usage: \\describe [schema.]TABLE')); break; }
        const result = await this.conn.describeTable(table, schema);
        console.log(formatTable(result));
        break;
      }

      case 'export': {
        // \export <file> [--table SCHEMA.TABLE] [--keys COL1,COL2] [--batch N]
        //
        // Format is auto-detected from extension:
        //   .csv            → CSV
        //   .json           → JSON
        //   .sql            → INSERT (default)
        //   .insert.sql     → INSERT  (explicit)
        //   .merge.sql      → MERGE
        //
        // For MERGE, --keys is required.
        // For multi-row INSERT, --batch N bundles N rows per statement.

        if (args.length === 0) {
          console.error(chalk.red(
            'Usage: \\export <file> [--table SCHEMA.TABLE] [--keys COL1,COL2] [--batch N]'
          ));
          break;
        }

        if (!this.lastResult) {
          console.error(chalk.yellow('No result to export yet. Run a SELECT first.'));
          break;
        }

        // Parse args: first positional = filePath, rest = flags
        let filePath = null;
        const exportOpts = {};

        for (let i = 0; i < args.length; i++) {
          if (args[i] === '--table' && args[i + 1]) {
            exportOpts.table = args[++i];
          } else if (args[i] === '--keys' && args[i + 1]) {
            exportOpts.keys = args[++i].split(',').map(k => k.trim());
          } else if (args[i] === '--batch' && args[i + 1]) {
            exportOpts.batch = parseInt(args[++i], 10);
          } else if (!filePath && !args[i].startsWith('--')) {
            filePath = args[i];
          }
        }

        if (!filePath) {
          console.error(chalk.red('Specify an output file path.'));
          break;
        }

        // Detect mode from double extension (.insert.sql / .merge.sql)
        const base = path.basename(filePath).toLowerCase();
        if (base.endsWith('.merge.sql'))  exportOpts.sqlMode = 'merge';
        if (base.endsWith('.insert.sql')) exportOpts.sqlMode = 'insert';

        // Default table name to filename stem (e.g. movimenti.sql → movimenti)
        if (!exportOpts.table) {
          exportOpts.table = path.basename(filePath).replace(/(\.(insert|merge))?\.sql$/i, '').replace(/\.(csv|json)$/i, '') || 'TARGET_TABLE';
        }

        // Warn if MERGE but no keys
        if (exportOpts.sqlMode === 'merge' && (!exportOpts.keys || exportOpts.keys.length === 0)) {
          console.error(chalk.red(
            'MERGE export requires --keys. E.g.: \\export out.merge.sql --table MYLIB.ORDERS --keys ORDNUM'
          ));
          break;
        }

        try {
          const bytes = exportToFile(this.lastResult, path.resolve(filePath), exportOpts);
          const fmt = exportOpts.sqlMode || path.extname(filePath).replace('.', '').toUpperCase();
          console.log(chalk.green(`✓ Exported ${bytes} bytes [${fmt}] → ${filePath}`));
          if (exportOpts.sqlMode === 'merge' || base.endsWith('.merge.sql')) {
            console.log(chalk.dim(`  keys: ${exportOpts.keys.join(', ')}`));
          }
          if (exportOpts.table) {
            console.log(chalk.dim(`  table: ${exportOpts.table}`));
          }
        } catch (err) {
          console.error(chalk.red(err.message));
        }
        break;
      }

      case 'import': {
        // \import <file> --table SCHEMA.TABLE [--mode abort|skip|confirm]
        //                [--batch N] [--dry-run]
        //                [--map srcCol=destCol,src2=dest2]
        //                [--delimiter ,]
        //
        // Format auto-detected from extension:
        //   .csv / .tsv        → CSV import
        //   .json              → JSON import
        //   .sql / *.insert.sql / *.merge.sql → SQL statement import

        if (args.length === 0) {
          console.error(chalk.red(
            'Usage: \\import <file> [--table SCHEMA.TABLE] [--mode abort|skip|confirm]\n' +
            '                      [--batch N] [--dry-run] [--map col1=COL1,col2=COL2]\n' +
            '                      [--delimiter ,]'
          ));
          break;
        }

        if (!this.conn?.isConnected()) {
          console.error(chalk.red('Not connected. Use \\connect or \\profile first.'));
          break;
        }

        // ── Parse args ────────────────────────────────────────────────────────
        let filePath = null;
        const importOpts = {
          errorMode: ERROR_MODE.ABORT,
          batchSize: 100,
          dryRun:    false,
          mapping:   {},
          delimiter: ',',
        };

        for (let i = 0; i < args.length; i++) {
          switch (args[i]) {
            case '--table':     importOpts.table     = args[++i]; break;
            case '--mode':      importOpts.errorMode = args[++i]; break;
            case '--batch':     importOpts.batchSize = parseInt(args[++i], 10) || 100; break;
            case '--dry-run':   importOpts.dryRun    = true; break;
            case '--delimiter': importOpts.delimiter = args[++i]; break;
            case '--map': {
              // --map OLDCOL=NEWCOL,OLDCOL2=NEWCOL2
              const pairs = (args[++i] || '').split(',');
              for (const pair of pairs) {
                const [src, dest] = pair.split('=');
                if (src) importOpts.mapping[src.trim()] = (dest || src).trim();
              }
              break;
            }
            default:
              if (!args[i].startsWith('--')) filePath = args[i];
          }
        }

        if (!filePath) {
          console.error(chalk.red('Specify a file to import.'));
          break;
        }

        const absPath = path.resolve(filePath);
        const bar     = new ProgressBar('Importing');

        importOpts.onProgress = (done, total) => bar.tick(done, total);

        // Confirm mode: pause REPL and ask interactively
        if (importOpts.errorMode === ERROR_MODE.CONFIRM) {
          importOpts.onConfirm = async (rowIdx, errMsg, sql) => {
            this.rl.pause();
            return new Promise(resolve => {
              process.stdout.write(
                chalk.yellow(`\n  ⚠ Row ${rowIdx} error: ${errMsg}\n`) +
                chalk.dim(`  SQL: ${sql.slice(0, 120)}\n`) +
                chalk.bold('  Skip this row and continue? [y/N] ')
              );
              process.stdin.once('data', data => {
                const answer = data.toString().trim().toLowerCase();
                this.rl.resume();
                resolve(answer === 'y' || answer === 'yes');
              });
            });
          };
        }

        const importer = new Importer(this.conn, importOpts);

        if (importOpts.dryRun) {
          console.log(chalk.dim(`  Dry run — no data will be written.`));
        }

        try {
          const result = await importer.importFile(absPath);
          bar.finish(result);

          if (result.errors.length > 0) {
            console.log(chalk.yellow(`\n  First ${Math.min(5, result.errors.length)} error(s):`));
            result.errors.slice(0, 5).forEach((e, i) => {
              console.log(chalk.dim(`  ${i + 1}. ${e.error}`));
              console.log(chalk.dim(`     Row: ${String(e.row).slice(0, 80)}`));
            });
            if (result.errors.length > 5) {
              console.log(chalk.dim(`  ... and ${result.errors.length - 5} more.`));
            }
          }
        } catch (err) {
          console.error(chalk.red(`Import error: ${err.message}`));
        }
        break;
      }

      case 'pipe': {
        // \pipe <src-table> --target-profile <p> [options]
        //
        // Transfer rows from current connection (source) to a target DB2.
        //
        // Required:
        //   --target-profile <n>       use saved profile for target connection
        //   OR
        //   --target-host <h> [--target-user u] [--target-password p] [--target-schema s]
        //
        // Table:
        //   First positional arg = source table  (e.g. SRCLIB.ORDERS)
        //   --target-table TGTLIB.ORDERS          (default: same as source)
        //   --sql "SELECT ..."                    override source SELECT
        //   --where "ACTIVE=1"                    append WHERE to source
        //
        // Transfer:
        //   --mode insert|merge      (default: insert)
        //   --keys  COL1,COL2        required for merge
        //   --batch N                rows per page (default: 500)
        //   --map   srcCol=destCol,… rename/exclude columns
        //   --truncate               DELETE FROM target before pipe
        //   --ddl                    CREATE TABLE on target from source schema
        //   --drop-if-exists         DROP TABLE before --ddl
        //   --mode-on-error skip     skip rows with errors (default: abort)
        //   --dry-run                fetch source, skip writes

        if (!this.conn?.isConnected()) {
          console.error(chalk.red('Source not connected. Use \\connect or \\profile first.'));
          break;
        }

        if (args.length === 0) {
          console.error(chalk.red(
            'Usage: \\pipe <src-table> --target-profile <n> [--target-table T]\n' +
            '       [--mode insert|merge] [--keys C1,C2] [--batch N] [--truncate]\n' +
            '       [--ddl] [--drop-if-exists] [--map c=C,...] [--dry-run]\n' +
            '       [--where "..."] [--sql "SELECT ..."] [--mode-on-error skip]'
          ));
          break;
        }

        // ── parse args ───────────────────────────────────────────────────────
        let sourceTable = null;
        const pipeOpts  = {
          mode: 'insert', batchSize: 500,
          keys: [], mapping: {},
          truncate: false, generateDDL: false,
          dropIfExists: false, dryRun: false,
          errorMode: ERROR_MODE.ABORT,
        };
        const tgtCfg = {};

        for (let i = 0; i < args.length; i++) {
          switch (args[i]) {
            case '--target-profile':  tgtCfg.profile       = args[++i]; break;
            case '--target-host':     tgtCfg.host          = args[++i]; break;
            case '--target-user':     tgtCfg.username      = args[++i]; break;
            case '--target-password': tgtCfg.password      = args[++i]; break;
            case '--target-schema':   tgtCfg.defaultSchema = args[++i]; break;
            case '--target-table':    pipeOpts.targetTable = args[++i]; break;
            case '--sql':             pipeOpts.sourceSQL   = args[++i]; break;
            case '--where':           pipeOpts.where       = args[++i]; break;
            case '--mode':            pipeOpts.mode        = args[++i]; break;
            case '--keys':            pipeOpts.keys        = args[++i].split(',').map(k=>k.trim()); break;
            case '--batch':           pipeOpts.batchSize   = parseInt(args[++i],10)||500; break;
            case '--truncate':        pipeOpts.truncate    = true; break;
            case '--ddl':             pipeOpts.generateDDL = true; break;
            case '--drop-if-exists':  pipeOpts.dropIfExists= true; break;
            case '--dry-run':         pipeOpts.dryRun      = true; break;
            case '--mode-on-error':   pipeOpts.errorMode   = args[++i]; break;
            case '--map': {
              (args[++i]||'').split(',').forEach(pair => {
                const [s,d] = pair.split('=');
                if (s) pipeOpts.mapping[s.trim()] = (d||s).trim();
              });
              break;
            }
            default:
              if (!args[i].startsWith('--')) sourceTable = args[i];
          }
        }

        if (!sourceTable && !pipeOpts.sourceSQL) {
          console.error(chalk.red('Specify a source table or --sql "SELECT ..."'));
          break;
        }

        // ── resolve target connection ─────────────────────────────────────────
        let targetConn = null;
        try {
          let targetConfig;
          if (tgtCfg.profile) {
            targetConfig = this.profiles.resolve(tgtCfg.profile);
          } else if (tgtCfg.host) {
            targetConfig = tgtCfg;
          } else {
            console.error(chalk.red(
              'Specify target: --target-profile <n>  or  --target-host <h>'
            ));
            break;
          }

          process.stdout.write(chalk.dim(`  Connecting to target ${targetConfig.host}…`));
          targetConn = new IBMiConnection(targetConfig);
          await targetConn.connect();
          console.log(chalk.green(' ✓'));
        } catch (err) {
          console.error(chalk.red(`  Target connection failed: ${err.message}`));
          break;
        }

        // ── run pipe ─────────────────────────────────────────────────────────
        pipeOpts.sourceTable = sourceTable;
        if (!pipeOpts.targetTable) pipeOpts.targetTable = sourceTable;

        const pipebar = new ProgressBar('Pipe');
        pipeOpts.onProgress = (written, total) => pipebar.tick(written, total);

        if (pipeOpts.dryRun) console.log(chalk.dim('  Dry run — nothing written to target.'));

        try {
          const pipe   = new Pipe(this.conn, targetConn, pipeOpts);
          const result = await pipe.run();
          pipebar.finish({
            inserted: result.totalWritten, skipped: result.totalSkipped,
            errors:   result.errors,       elapsed: result.elapsed,
            dryRun:   result.dryRun,
          });

          if (result.ddlExecuted) console.log(chalk.dim('  DDL executed on target.'));
          if (result.truncated)   console.log(chalk.dim('  Target table truncated before import.'));
          if (result.errors.length > 0) {
            console.log(chalk.yellow(`\n  First ${Math.min(5, result.errors.length)} error(s):`));
            result.errors.slice(0,5).forEach((e,i) =>
              console.log(chalk.dim(`  ${i+1}. [page ${e.page}] ${e.error}`))
            );
          }
        } catch (err) {
          const odbcDetail = err.odbcErrors
            ? '\n  ' + err.odbcErrors.map(e => `[${e.state}] ${e.message}`).join('\n  ')
            : '';
          console.error(chalk.red(`Pipe error: ${err.message}${odbcDetail}`));
        } finally {
          if (targetConn) await targetConn.disconnect().catch(() => {});
        }
        break;
      }

      case 'ddl': {
        // \ddl [schema.]TABLE [--target-table T] [--exec] [--drop-if-exists]
        // Generate DDL from QSYS2.SYSCOLUMNS; optionally execute on current conn.
        if (!this.conn?.isConnected()) { console.error(chalk.red('Not connected.')); break; }

        const [srcTable, ...ddlRest] = args;
        if (!srcTable) {
          console.error(chalk.red('Usage: \\ddl [schema.]TABLE [--target-table T] [--exec] [--drop-if-exists]'));
          break;
        }

        let ddlTarget    = srcTable;
        let execDDL      = false;
        let dropIfExists = false;
        for (let i = 0; i < ddlRest.length; i++) {
          if (ddlRest[i] === '--target-table')  ddlTarget    = ddlRest[++i];
          if (ddlRest[i] === '--exec')          execDDL      = true;
          if (ddlRest[i] === '--drop-if-exists') dropIfExists = true;
        }

        const [sch, tbl] = srcTable.includes('.')
          ? srcTable.split('.')
          : [this.conn.config.defaultSchema, srcTable];

        try {
          const desc = await this.conn.describeTable(tbl, sch);
          if (desc.rows.length === 0) {
            console.error(chalk.red(`No columns found for ${srcTable}`)); break;
          }
          const ddl = generateDDL(ddlTarget, desc);
          console.log(chalk.dim('\n' + ddl + '\n'));

          if (execDDL) {
            if (dropIfExists) {
              try { await this.conn.execute(`DROP TABLE ${ddlTarget}`); } catch {}
            }
            await this.conn.execute(ddl.replace(/;\s*$/, ''));
            console.log(chalk.green(`✓ Table ${ddlTarget} created.`));
          }
        } catch (err) {
          const odbcDetail = err.odbcErrors
            ? '\n  ' + err.odbcErrors.map(e => `[${e.state}] ${e.message}`).join('\n  ')
            : '';
          console.error(chalk.red(err.message + odbcDetail));
        }
        break;
      }


      case 'drivers': {
        const { listDrivers: _ld } = require('../lib/drivers');
        console.log(chalk.bold('\nSupported database types:\n'));
        for (const d of _ld()) {
          console.log(`  ${chalk.cyan(d.type.padEnd(14))} ${d.label}`);
        }
        console.log(chalk.dim('\n  Use --type <type> in \\saveprofile or \\connect.\n'));
        break;
      }

      case 'history': {
        const all = this.history.all();
        if (all.length === 0) { console.log(chalk.dim('No history.')); break; }
        const slice = all.slice(-20).reverse();
        slice.forEach((h, i) => console.log(chalk.dim(`${String(i + 1).padStart(3)}`) + '  ' + h));
        break;
      }

      case 'hsearch': {
        // \hsearch <keyword>
        const kw = args.join(' ');
        const found = this.history.search(kw);
        if (found.length === 0) { console.log(chalk.dim(`No history matching "${kw}".`)); break; }
        found.slice(-20).forEach((h, i) => console.log(chalk.dim(`${String(i + 1).padStart(3)}`) + '  ' + h));
        break;
      }

      case 'clear':
        console.clear();
        break;

      case 'status': {
        if (this.conn?.isConnected()) {
          const cfg = this.conn.config;
          const typeLabel = this.conn.dbLabel || cfg.type || 'ibmi';
          const hostOrDb  = cfg.database || cfg.host || '?';
          console.log(chalk.green('● Connected') +
            chalk.dim(`  [${typeLabel}]  host=${hostOrDb}  user=${cfg.username || '?'}  schema=${cfg.defaultSchema || '?'}`));
        } else {
          console.log(chalk.red('● Not connected'));
        }
        break;
      }

      default:
        console.error(chalk.red(`Unknown command: \\${verb}. Type \\help for help.`));
    }
  }

  // ── completer ─────────────────────────────────────────────────────────────

  _completer(line) {
    const SQL_KEYWORDS = [
      'SELECT', 'FROM', 'WHERE', 'INSERT', 'INTO', 'VALUES',
      'UPDATE', 'SET', 'DELETE', 'JOIN', 'LEFT', 'INNER', 'OUTER',
      'GROUP BY', 'ORDER BY', 'HAVING', 'LIMIT', 'FETCH FIRST',
      'CREATE', 'DROP', 'ALTER', 'TABLE', 'VIEW', 'INDEX',
      'CALL', 'WITH', 'DISTINCT', 'AS', 'ON', 'AND', 'OR', 'NOT',
      'NULL', 'IS NULL', 'IS NOT NULL', 'IN', 'BETWEEN', 'LIKE',
    ];

    const META_CMDS = [
      '\\help', '\\quit', '\\connect', '\\disconnect',
      '\\profile', '\\profiles', '\\saveprofile', '\\delprofile',
      '\\schema', '\\tables', '\\describe',
      '\\export', '\\import', '\\pipe', '\\ddl', '\\drivers', '\\history', '\\hsearch', '\\status', '\\clear',
    ];

    const all = [...SQL_KEYWORDS, ...META_CMDS];
    const upper = line.toUpperCase();
    const hits = all.filter(k => k.startsWith(upper));
    return [hits.length ? hits : all, line];
  }

  // ── help ──────────────────────────────────────────────────────────────────

  _printHelp() {
    const s = chalk.cyan;
    const d = chalk.dim;
    console.log(`
${chalk.bold('Connection')}
  ${s('\\connect')} <host> [user] [pwd] [schema]   Connect to an IBM i system
  ${s('\\disconnect')}                              Close current connection
  ${s('\\profile')} <name>                          Connect using saved profile
  ${s('\\status')}                                  Show connection status

${chalk.bold('Profiles')}
  ${s('\\profiles')}                                List all saved profiles
  ${s('\\saveprofile')} <n> <host> [u] [p] [s]     Save a named profile
  ${s('\\delprofile')} <name>                       Delete a profile

${chalk.bold('Schema & objects')}
  ${s('\\schema')} [name]                           Show/set default schema
  ${s('\\tables')} [schema]                         List tables in a schema
  ${s('\\describe')} [schema.]TABLE                 Describe table columns

${chalk.bold('SQL execution')}
  ${d('Enter SQL and end with ; or type GO/RUN on its own line')}
  ${d('Multi-line input supported — press Enter to continue')}

${chalk.bold('Export')}
  ${s('\\export')} <file.csv>                        Export last result as CSV
  ${s('\\export')} <file.json>                       Export last result as JSON
  ${s('\\export')} <file.sql>                        Export as SQL INSERTs
  ${s('\\export')} <file.insert.sql> ${d('[--table T] [--batch N]')}
  ${s('\\export')} <file.merge.sql>  ${d('--keys COL1,COL2 [--table T]')}
  ${d('--table SCHEMA.TABLE  override target table name')}
  ${d('--keys  COL1,COL2     join key columns (MERGE only)')}
  ${d('--batch N             rows per INSERT statement (default 1)')}

${chalk.bold('Import')}
  ${s('\\import')} <file.csv>   ${d('--table SCHEMA.TABLE')}
  ${s('\\import')} <file.json>  ${d('[--table T]  (or "table" key inside JSON)')}
  ${s('\\import')} <file.sql>   ${d('(INSERT/MERGE statements)')}
  ${d('--table  SCHEMA.TABLE          target table (required for CSV/JSON)')}
  ${d('--mode   abort|skip|confirm    error handling (default: abort)')}
  ${d('--batch  N                     rows per commit (default: 100)')}
  ${d('--dry-run                      parse + validate without writing')}
  ${d('--map    srcCol=DESTCOL,...    rename/filter columns (CSV/JSON)')}
  ${d('--delimiter CHAR               CSV delimiter (default: ,)')}

${chalk.bold('DB2 → DB2 Pipe')}
  ${s('\\pipe')} <src-table> ${d('--target-profile <n>  [--target-table T]')}
  ${s('\\pipe')} ${d('--sql "SELECT ..." --target-host h --target-user u --target-password p')}
  ${d('--mode         insert|merge       transfer mode (default: insert)')}
  ${d('--keys         COL1,COL2          join keys for MERGE')}
  ${d('--batch        N                  rows per page (default: 500)')}
  ${d('--truncate                        DELETE FROM target before pipe')}
  ${d('--ddl                             CREATE TABLE on target from source schema')}
  ${d('--drop-if-exists                  DROP TABLE before --ddl')}
  ${d('--map          srcCol=DESTCOL,…   rename/exclude columns')}
  ${d('--where        "condition"        filter rows on source')}
  ${d('--mode-on-error skip              skip bad rows instead of aborting')}
  ${d('--dry-run                         fetch source only, no writes')}

${chalk.bold('DDL')}
  ${s('\\ddl')} <table> ${d('[--target-table T] [--exec] [--drop-if-exists]')}
  ${d('Generate CREATE TABLE DDL from QSYS2.SYSCOLUMNS; --exec runs it on current conn.')}

${chalk.bold('History')}
  ${s('\\history')}                                 Show last 20 commands
  ${s('\\hsearch')} <keyword>                       Search command history
  ${d('Use ↑ ↓ arrow keys to navigate history')}

${chalk.bold('Other')}
  ${s('\\clear')}                                   Clear screen
  ${s('\\quit')} or ${s('\\exit')}                             Exit
`);
  }
}

module.exports = { STRSQLSession };
