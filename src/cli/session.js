'use strict';

require('dotenv').config();

const readline = require('readline');
const chalk    = require('chalk');
const path     = require('path');
const fs       = require('fs');

const { IBMiConnection, parseLibraryList } = require('../lib/connection');
const { ProfileManager }  = require('../lib/profiles');
const { HistoryManager }  = require('../lib/history');
const { formatTable, formatExecResult, exportToFile, toInsert, toMerge } = require('../lib/formatter');
const { Importer, ERROR_MODE } = require('../lib/importer');
const { Pipe, generateDDL }    = require('../lib/pipe');
const { listDrivers }          = require('../lib/drivers');
const { ProgressBar }          = require('./progress');
const { printWithPager, prefersAsciiOutput, shouldUsePager } = require('../lib/pager');

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
    this.completionCache = this._newCompletionCache();
  }

  async _printResult(text, opts = {}) {
    if (!this.rl) {
      await printWithPager(text, opts);
      return;
    }

    this.rl.pause();
    try {
      await printWithPager(text, opts);
    } finally {
      this.rl.resume();
    }
  }

  _tableOpts(usePager) {
    const degrade = prefersAsciiOutput();
    const explicitWidth = Number.isFinite(this.opts.maxCellWidth) && this.opts.maxCellWidth > 0
      ? this.opts.maxCellWidth
      : null;

    const autoWidth = usePager ? Number.MAX_SAFE_INTEGER : 40;

    return {
      maxCellWidth: explicitWidth || autoWidth,
      asciiBorders: degrade,
      plain: degrade,
    };
  }

  _formatTableForDisplay(result) {
    const compact = formatTable(result, this._tableOpts(false));
    if (!shouldUsePager(compact)) {
      return { text: compact, forcePager: false };
    }

    return {
      text: formatTable(result, this._tableOpts(true)),
      forcePager: true,
    };
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
      this._resetCompletionCache();
      console.log(chalk.green(' ✓'));
      if (config.defaultSchema) {
        console.log(chalk.dim(` Default schema: ${config.defaultSchema}`));
      }
      if (config.libraryList) {
        const libs = parseLibraryList(this.conn.config.libraryList);
        if (libs.length > 0) {
          console.log(chalk.dim(` Library list: ${libs.join(', ')}`));
        }
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
      completer: (line, cb) => this._completer(line, cb),
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
        // ── CHANGED: pipe table output through less ──────────────────────────
        const formatted = this._formatTableForDisplay(result);
        await this._printResult(formatted.text, { force: formatted.forcePager });
        // ────────────────────────────────────────────────────────────────────
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
            case '--library-list': case '--libl': connCfg.libraryList = args[++i]; break;
            default:           if (!args[i].startsWith('--')) positional.push(args[i]);
          }
        }
        if (positional[0] && !connCfg.host)          connCfg.host          = positional[0];
        if (positional[1] && !connCfg.username)       connCfg.username      = positional[1];
        if (positional[2] && !connCfg.password)       connCfg.password      = positional[2];
        if (positional[3] && !connCfg.defaultSchema)  connCfg.defaultSchema = positional[3];

        if (!connCfg.host && !connCfg.database) {
          console.error(chalk.red('Usage: \\connect <host> [user] [pwd] [schema] [--type TYPE]'));
          break;
        }
        if (this.conn) await this.conn.disconnect().catch(() => {});
        this._resetCompletionCache();
        await this._connect(connCfg);
        break;
      }

      case 'profile': {
        const [pname] = args;
        if (!pname) { console.error(chalk.red('Usage: \\profile <name>')); break; }
        if (this.conn) await this.conn.disconnect().catch(() => {});
        this._resetCompletionCache();
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
            case '--library-list': case '--libl': pfCfg.libraryList = pfArgs[++i]; break;
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
          this._resetCompletionCache();
          console.log(chalk.dim('Disconnected.'));
        }
        break;
      }

      case 'schema': {
        const [schema] = args;
        if (!schema) {
          console.log(chalk.dim(`Current schema: ${this.conn?.config?.defaultSchema || '(none)'}`));
        } else {
          const setSchemaSQL = this.conn.driver?.setSchema
            ? this.conn.driver.setSchema.replace('?', schema)
            : `SET SCHEMA ${schema}`;
          await this._executeSQL(setSchemaSQL);
          if (this.conn) this.conn.config.defaultSchema = schema;
          this._resetCompletionCache();
        }
        break;
      }

      case 'libl': {
        if (!this.conn?.isConnected()) { console.error(chalk.red('Not connected.')); break; }
        if (args.length === 0) {
          const current = this.conn.config?.libraryList;
          if (current) {
            const libs = parseLibraryList(current);
            const curLib = this.conn.config.defaultSchema
              ? `  current=${this.conn.config.defaultSchema}`
              : '';
            console.log(chalk.dim(`Library list: ${libs.join(', ')}${curLib}`));
          } else {
            console.log(chalk.dim('No library list set.'));
          }
        } else {
          const libStr = args.join(',');
          try {
            await this.conn.setLibraryList(libStr);
            this._resetCompletionCache();
            const libs = Array.isArray(this.conn.config.libraryList)
              ? this.conn.config.libraryList
              : parseLibraryList(this.conn.config.libraryList);
            const current = this.conn.config.defaultSchema
              ? chalk.dim(`  current=${this.conn.config.defaultSchema}`)
              : '';
            console.log(chalk.green(`Library list: ${libs.join(', ') || '*NONE'}`) + current);
          } catch (err) {
            const odbcDetail = err.odbcErrors
              ? '\n  ' + err.odbcErrors.map(e => `[${e.state}] ${e.message}`).join('\n  ')
              : '';
            console.error(chalk.red(`Failed to set library list: ${err.message}${odbcDetail}`));
          }
        }
        break;
      }

      case 'tables': {
        if (!this.conn?.isConnected()) { console.error(chalk.red('Not connected.')); break; }
        const schema = args[0] || this.conn.config.defaultSchema;
        if (!schema) { console.error(chalk.red('Specify a schema: \\tables <schema>')); break; }
        const result = await this.conn.listTables(schema);
        // ── CHANGED: pipe \tables output through less too ────────────────────
        const formatted = this._formatTableForDisplay(result);
        await this._printResult(formatted.text, { force: formatted.forcePager });
        break;
      }

      case 'describe':
      case 'desc': {
        if (!this.conn?.isConnected()) { console.error(chalk.red('Not connected.')); break; }
        const [table, schema] = args;
        if (!table) { console.error(chalk.red('Usage: \\describe [schema.]TABLE')); break; }
        const [result, pkSet] = await Promise.all([
          this.conn.describeTable(table, schema),
          this.conn.primaryKeys(table, schema),
        ]);
        result.rows = result.rows.map(row => ({
          ...row,
          PK: pkSet.has((row.COLUMN_NAME || '').toUpperCase()) ? '🔑' : '',
        }));
        result.columns = [{ name: 'PK' }, ...result.columns];
        // describe output is rarely long enough to need paging, but keep consistent
        const formatted = this._formatTableForDisplay(result);
        await this._printResult(formatted.text, { force: formatted.forcePager });
        break;
      }

      case 'export': {
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

        const base = path.basename(filePath).toLowerCase();
        if (base.endsWith('.merge.sql'))  exportOpts.sqlMode = 'merge';
        if (base.endsWith('.insert.sql')) exportOpts.sqlMode = 'insert';

        if (!exportOpts.table) {
          exportOpts.table = path.basename(filePath).replace(/(\.(insert|merge))?\.sql$/i, '').replace(/\.(csv|json)$/i, '') || 'TARGET_TABLE';
        }

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
        const kw = args.join(' ');
        const found = this.history.search(kw);
        if (found.length === 0) { console.log(chalk.dim(`No history matching "${kw}".`)); break; }
        found.slice(-20).forEach((h, i) => console.log(chalk.dim(`${String(i + 1).padStart(3)}`) + '  ' + h));
        break;
      }

      case 'clear':
        console.clear();
        break;

      case 'run': {
        if (args.length === 0) {
          console.error(chalk.red('Usage: \\run <file.sql> [--stop-on-error]'));
          break;
        }
        if (!this.conn?.isConnected()) {
          console.error(chalk.red('Not connected. Use \\connect or \\profile first.'));
          break;
        }

        let filePath = null;
        let stopOnError = false;
        for (const a of args) {
          if (a === '--stop-on-error') stopOnError = true;
          else if (!a.startsWith('--')) filePath = a;
        }
        if (!filePath) {
          console.error(chalk.red('Specify a SQL file path.'));
          break;
        }

        const absPath = path.resolve(filePath);
        if (!fs.existsSync(absPath)) {
          console.error(chalk.red(`File not found: ${absPath}`));
          break;
        }

        const content = fs.readFileSync(absPath, 'utf8');
        const statements = content
          .split(/;\s*(?:\r?\n|$)/)
          .map(s => s.replace(/--.*$/gm, '').trim())
          .filter(s => s.length > 0 && !/^\s*$/.test(s));

        if (statements.length === 0) {
          console.log(chalk.dim('No SQL statements found in file.'));
          break;
        }

        console.log(chalk.dim(`\n  Executing ${statements.length} statement(s) from ${path.basename(absPath)}…\n`));

        let executed = 0;
        let errors   = 0;
        const startTime = Date.now();

        for (let i = 0; i < statements.length; i++) {
          const sql = statements[i];
          const label = `[${i + 1}/${statements.length}]`;

          try {
            const upper = sql.trim().toUpperCase();
            const isSelect =
              upper.startsWith('SELECT') ||
              upper.startsWith('WITH') ||
              upper.startsWith('VALUES');

            if (isSelect) {
              const result = await this.conn.query(sql);
              this.lastResult = result;
              console.log(chalk.dim(`${label} SELECT → ${result.rowCount} row(s)`));
              // ── CHANGED: pipe \run SELECT results through less too ──────────
              const formatted = this._formatTableForDisplay(result);
              await this._printResult(formatted.text, { force: formatted.forcePager });
              // ────────────────────────────────────────────────────────────────
            } else {
              const result = await this.conn.execute(sql);
              this.lastResult = result;
              console.log(chalk.dim(`${label}`) + ' ' + formatExecResult(result));
            }
            executed++;
          } catch (err) {
            errors++;
            console.error(chalk.red(`${label} Error: ${err.message}`));
            if (err.odbcErrors) {
              for (const e of err.odbcErrors) {
                console.error(chalk.dim(`  [${e.state}] ${e.message}`));
              }
            }
            console.error(chalk.dim(`  SQL: ${sql.slice(0, 120)}${sql.length > 120 ? '…' : ''}`));
            if (stopOnError) {
              console.error(chalk.yellow('  Stopped on error (--stop-on-error).'));
              break;
            }
          }
        }

        const elapsed = Date.now() - startTime;
        console.log(chalk.dim(`\n  Done: ${executed} executed, ${errors} error(s), ${elapsed} ms\n`));
        break;
      }

      case 'status': {
        if (this.conn?.isConnected()) {
          const cfg = this.conn.config;
          const typeLabel = this.conn.dbLabel || cfg.type || 'ibmi';
          const hostOrDb  = cfg.database || cfg.host || '?';
          let statusLine = chalk.green('● Connected') +
            chalk.dim(`  [${typeLabel}]  host=${hostOrDb}  user=${cfg.username || '?'}  schema=${cfg.defaultSchema || '?'}`);
          if (cfg.libraryList) {
            const libs = parseLibraryList(cfg.libraryList);
            statusLine += chalk.dim(`  libl=${libs.join(',')}`);
          }
          console.log(statusLine);
        } else {
          console.log(chalk.red('● Not connected'));
        }
        break;
      }

      // ── NEW: \nopager — toggle pager off for the session ──────────────────
      case 'nopager': {
        process.env.STRSQL_NO_PAGER = '1';
        console.log(chalk.dim('Pager disabled for this session. Use \\pager to re-enable.'));
        break;
      }

      // ── NEW: \pager — toggle pager back on ───────────────────────────────
      case 'pager': {
        delete process.env.STRSQL_NO_PAGER;
        const { detectPager } = require('../lib/pager');
        const p = detectPager();
        if (p) {
          console.log(chalk.dim(`Pager enabled (${p}).`));
        } else {
          console.log(chalk.yellow('No pager found on this system (less/more/most).'));
        }
        break;
      }

      case 'pagerstatus': {
        const { pagerDebugInfo } = require('../lib/pager');
        const info = pagerDebugInfo();
        console.log(chalk.bold('\nPager status:'));
        console.log(`  pager:        ${info.pager || '(none)'}`);
        console.log(`  pagerName:    ${info.pagerName || '(none)'}`);
        console.log(`  args:         ${info.args.length ? info.args.join(' ') : '(none)'}`);
        console.log(`  interactive:  ${info.interactive ? 'yes' : 'no'}`);
        console.log(`  ascii mode:   ${info.ascii ? 'yes' : 'no'}`);
        console.log(`  env PAGER:    ${info.envPager || '(unset)'}`);
        console.log(`  env LESS:     ${info.envLess || '(unset)'}`);
        console.log();
        break;
      }

      default:
        console.error(chalk.red(`Unknown command: \\${verb}. Type \\help for help.`));
    }
  }

  // ── completer ─────────────────────────────────────────────────────────────

  _newCompletionCache() {
    return {
      tablesBySchema: new Map(),
      columnsByTable: new Map(),
    };
  }

  _resetCompletionCache() {
    this.completionCache = this._newCompletionCache();
  }

  _completionSchemas() {
    const cfg = this.conn?.config || {};
    const schemas = [];
    if (cfg.defaultSchema) schemas.push(cfg.defaultSchema);
    if (cfg.libraryList) {
      const libs = parseLibraryList(cfg.libraryList);
      schemas.push(...libs);
    }
    if (schemas.length > 0) {
      const seen = new Set();
      return schemas.filter(schema => {
        const key = String(schema || '').toUpperCase();
        if (!key || seen.has(key)) return false;
        seen.add(key);
        return true;
      });
    }
    return this.conn?.dbType === 'sqlite' ? ['main'] : [];
  }

  _completionToken(line) {
    const match = line.match(/(?:^|[\s(),=<>+\-*/])([\\A-Za-z0-9_.$"]*)$/);
    return match ? match[1] : '';
  }

  _stripIdentifier(id) {
    return String(id || '').replace(/^["'`\[]|["'`\]]$/g, '');
  }

  _completionFilter(values, token) {
    const needle = token.toUpperCase();
    const seen = new Set();
    return values
      .filter(Boolean)
      .filter(v => String(v).toUpperCase().startsWith(needle))
      .filter(v => {
        const key = String(v).toUpperCase();
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
      })
      .sort((a, b) => String(a).localeCompare(String(b)));
  }

  async _tableCompletions() {
    if (!this.conn?.isConnected()) return [];

    const schemas = this._completionSchemas();
    const candidates = [];

    for (const schema of schemas) {
      const schemaKey = String(schema || '').toUpperCase();
      if (!this.completionCache.tablesBySchema.has(schemaKey)) {
        const result = await this.conn.listTables(schema);
        const tables = result.rows.map(r => ({
          schema: r.TABLE_SCHEMA || schema,
          name: r.TABLE_NAME,
        })).filter(t => t.name);
        this.completionCache.tablesBySchema.set(schemaKey, tables);
      }

      for (const table of this.completionCache.tablesBySchema.get(schemaKey)) {
        candidates.push(table.name);
        if (table.schema) candidates.push(`${table.schema}.${table.name}`);
      }
    }

    return candidates;
  }

  _tablesInSQL(line) {
    const sql = line.replace(/;+\s*$/, '');
    const tables = [];
    const aliasByName = new Map();
    const re = /\b(?:FROM|JOIN|UPDATE|INTO)\s+([A-Za-z0-9_.$"]+)(?:\s+(?:AS\s+)?([A-Za-z0-9_]+))?/gi;
    const stop = new Set(['WHERE', 'JOIN', 'LEFT', 'RIGHT', 'FULL', 'INNER', 'OUTER', 'ON',
      'SET', 'VALUES', 'GROUP', 'ORDER', 'HAVING', 'LIMIT', 'FETCH']);

    let match;
    while ((match = re.exec(sql)) !== null) {
      const rawTable = this._stripIdentifier(match[1]);
      const alias = match[2] && !stop.has(match[2].toUpperCase()) ? match[2] : null;
      if (!rawTable) continue;
      tables.push({ table: rawTable, alias });
      aliasByName.set(rawTable.toUpperCase(), rawTable);
      if (alias) aliasByName.set(alias.toUpperCase(), rawTable);
    }

    return { tables, aliasByName };
  }

  async _columnsForTable(tableRef) {
    if (!this.conn?.isConnected()) return [];
    const clean = this._stripIdentifier(tableRef);
    const [schema, table] = clean.includes('.')
      ? clean.split('.')
      : [this.conn.config.defaultSchema, clean];
    const key = `${schema || ''}.${table}`.toUpperCase();

    if (!this.completionCache.columnsByTable.has(key)) {
      const result = await this.conn.describeTable(table, schema);
      const columns = result.rows.map(r => r.COLUMN_NAME).filter(Boolean);
      this.completionCache.columnsByTable.set(key, columns);
    }

    return this.completionCache.columnsByTable.get(key);
  }

  async _columnCompletions(line, token) {
    const { tables, aliasByName } = this._tablesInSQL(line);
    if (tables.length === 0) return [];

    const dot = token.lastIndexOf('.');
    if (dot >= 0) {
      const qualifier = token.slice(0, dot);
      const columnPrefix = token.slice(dot + 1);
      const table = aliasByName.get(qualifier.toUpperCase());
      if (!table) return [];
      const columns = await this._columnsForTable(table);
      return this._completionFilter(columns, columnPrefix).map(c => `${qualifier}.${c}`);
    }

    const candidates = [];
    for (const entry of tables) {
      const columns = await this._columnsForTable(entry.table);
      candidates.push(...columns);
    }
    return this._completionFilter(candidates, token);
  }

  _wantsTableCompletion(line, token) {
    const beforeToken = line.slice(0, line.length - token.length);
    const parts = beforeToken.trim().split(/\s+/);
    const prev = (parts[parts.length - 1] || '').toUpperCase();
    const tableWords = new Set(['FROM', 'JOIN', 'INTO', 'UPDATE', 'TABLE', 'DESCRIBE', 'DESC']);
    return tableWords.has(prev) ||
      /^\\(?:describe|desc|ddl|pipe)\s+/i.test(line) ||
      /--(?:target-table|source-table|table)\s+$/i.test(beforeToken);
  }

  async _completionHits(line, token = this._completionToken(line)) {
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
      '\\schema', '\\libl', '\\tables', '\\describe',
      '\\export', '\\import', '\\pipe', '\\ddl', '\\run',
      '\\drivers', '\\history', '\\hsearch', '\\status', '\\clear',
      '\\pager', '\\nopager', '\\pagerstatus',
    ];

    if (line.trimStart().startsWith('\\') && !line.trimStart().includes(' ')) {
      return this._completionFilter(META_CMDS, token);
    }

    if (this._wantsTableCompletion(line, token)) {
      return this._completionFilter(await this._tableCompletions(), token);
    }

    const columns = await this._columnCompletions(line, token);
    if (columns.length > 0) return columns;

    return this._completionFilter([...SQL_KEYWORDS, ...META_CMDS], token);
  }

  async _completer(line, cb) {
    const token = this._completionToken(line);
    const contextLine = this.buffer.length > 0
      ? `${this.buffer.join(' ')} ${line}`
      : line;
    try {
      const hits = await this._completionHits(contextLine, token);
      cb(null, [hits, token]);
    } catch {
      cb(null, [[], token]);
    }
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
  ${s('\\libl')} LIB1,LIB2,...                      Set IBM i library list
  ${s('\\libl')}                                    Show current IBM i library list
  ${s('\\tables')} [schema]                         List tables in a schema
  ${s('\\describe')} [schema.]TABLE                 Describe table columns

${chalk.bold('SQL execution')}
  ${d('Enter SQL and end with ; or type GO/RUN on its own line')}
  ${d('Multi-line input supported — press Enter to continue')}
  ${s('\\run')} <file.sql> ${d('[--stop-on-error]')}     Execute SQL from a file

${chalk.bold('Export')}
  ${s('\\export')} <file.csv>                        Export last result as CSV
  ${s('\\export')} <file.json>                       Export last result as JSON
  ${s('\\export')} <file.sql>                        Export as SQL INSERTs
  ${s('\\export')} <file.insert.sql> ${d('[--table T] [--batch N]')}
  ${s('\\export')} <file.merge.sql>  ${d('--keys COL1,COL2 [--table T]')}

${chalk.bold('Import')}
  ${s('\\import')} <file.csv>   ${d('--table SCHEMA.TABLE')}
  ${s('\\import')} <file.json>  ${d('[--table T]')}
  ${s('\\import')} <file.sql>

${chalk.bold('DB2 → DB2 Pipe')}
  ${s('\\pipe')} <src-table> ${d('--target-profile <n>  [--target-table T]')}

${chalk.bold('DDL')}
  ${s('\\ddl')} <table> ${d('[--target-table T] [--exec] [--drop-if-exists]')}

${chalk.bold('History')}
  ${s('\\history')}                                 Show last 20 commands
  ${s('\\hsearch')} <keyword>                       Search command history
  ${d('Use ↑ ↓ arrow keys to navigate history')}

${chalk.bold('Pager')}
  ${d('Results are automatically shown via less (scroll with ← → ↑ ↓, q to exit)')}
  ${s('\\nopager')}                                 Disable pager for this session
  ${s('\\pager')}                                   Re-enable pager
  ${s('\\pagerstatus')}                             Show pager diagnostics
  ${d('Set STRSQL_NO_PAGER=1 to disable permanently')}

${chalk.bold('Other')}
  ${s('\\drivers')}                                 List supported DB types
  ${s('\\clear')}                                   Clear screen
  ${s('\\quit')} or ${s('\\exit')}                             Exit
`);
  }
}

module.exports = { STRSQLSession };
