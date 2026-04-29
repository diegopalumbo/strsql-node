#!/usr/bin/env node
'use strict';

require('dotenv').config();

const { Command } = require('commander');
const chalk = require('chalk');

const { STRSQLSession }  = require('../src/cli/session');
const { ProfileManager } = require('../src/lib/profiles');
const pkg = require('../package.json');

const program = new Command();

program
  .name('strsql')
  .description('IBM i STRSQL emulator via ODBC')
  .version(pkg.version);

// ─── strsql  (interactive session) ──────────────────────────────────────────
program
  .command('session', { isDefault: true })
  .description('Start an interactive SQL session (default)')
  .option('-p, --profile <name>',  'Named connection profile to use')
  .option('-H, --host <host>',     'IBM i hostname (overrides profile)')
  .option('-u, --user <user>',     'Username')
  .option('--password <password>', 'Password (prefer STRSQL_PASSWORD env var)')
  .option('-s, --schema <schema>', 'Default schema/library')
  .option('-l, --library-list <libs>', 'IBM i library list (comma-separated)')
  .option('--max-cell-width <n>',  'Max column width in table output (default: auto)')
  .action(async (opts) => {
    // CLI flags override ENV and profile
    if (opts.host)        process.env.STRSQL_HOST         = opts.host;
    if (opts.user)        process.env.STRSQL_USER         = opts.user;
    if (opts.password)    process.env.STRSQL_PASSWORD     = opts.password;
    if (opts.schema)      process.env.STRSQL_SCHEMA       = opts.schema;
    if (opts.libraryList) process.env.STRSQL_LIBRARY_LIST = opts.libraryList;

    const session = new STRSQLSession({
      profile:      opts.profile,
      maxCellWidth: opts.maxCellWidth ? parseInt(opts.maxCellWidth, 10) : undefined,
    });

    await session.start(opts.profile);
  });

// ─── strsql run <sql>  (non-interactive single query) ────────────────────────
program
  .command('run <sql>')
  .description('Execute a single SQL statement and exit')
  .option('-p, --profile <name>',   'Named connection profile')
  .option('-H, --host <host>',      'IBM i hostname')
  .option('-u, --user <user>',      'Username')
  .option('--password <password>',  'Password')
  .option('-s, --schema <schema>',  'Default schema')
  .option('-l, --library-list <libs>', 'IBM i library list (comma-separated)')
  .option('-f, --format <fmt>',     'Output format: table|csv|json|insert|merge', 'table')
  .option('-o, --out <file>',       'Export result to file (.csv/.json/.sql/.insert.sql/.merge.sql)')
  .option('--table <table>',        'Target table name for SQL export (e.g. MYLIB.ORDERS)')
  .option('--keys <keys>',          'Key columns for MERGE, comma-separated (e.g. ORDNUM,CUSNUM)')
  .option('--batch <n>',            'Rows per INSERT statement (default 1)', '1')
  .action(async (sql, opts) => {
    if (opts.host)        process.env.STRSQL_HOST         = opts.host;
    if (opts.user)        process.env.STRSQL_USER         = opts.user;
    if (opts.password)    process.env.STRSQL_PASSWORD     = opts.password;
    if (opts.schema)      process.env.STRSQL_SCHEMA       = opts.schema;
    if (opts.libraryList) process.env.STRSQL_LIBRARY_LIST = opts.libraryList;

    const { IBMiConnection }  = require('../src/lib/connection');
    const { ProfileManager }  = require('../src/lib/profiles');
    const { formatTable, formatExecResult, toCSV, toJSON, toInsert, toMerge, exportToFile } = require('../src/lib/formatter');

    const profiles = new ProfileManager();
    const config = profiles.resolve(opts.profile);

    if (!config.host) {
      console.error(chalk.red('No host specified. Use --host or set STRSQL_HOST.'));
      process.exit(1);
    }

    const sqlOpts = {
      table: opts.table,
      keys:  opts.keys ? opts.keys.split(',').map(k => k.trim()) : [],
      batch: parseInt(opts.batch, 10) || 1,
    };

    const conn = new IBMiConnection(config);
    try {
      await conn.connect();
      const upper = sql.trim().toUpperCase();
      const isSelect = upper.startsWith('SELECT') || upper.startsWith('WITH') || upper.startsWith('VALUES');

      if (isSelect) {
        const result = await conn.query(sql);
        if (opts.out) {
          exportToFile(result, opts.out, sqlOpts);
          console.log(chalk.green(`Exported → ${opts.out}`));
        } else if (opts.format === 'json') {
          console.log(toJSON(result));
        } else if (opts.format === 'csv') {
          process.stdout.write(toCSV(result) + '\n');
        } else if (opts.format === 'insert') {
          process.stdout.write(toInsert(result, sqlOpts) + '\n');
        } else if (opts.format === 'merge') {
          if (!sqlOpts.keys || sqlOpts.keys.length === 0) {
            console.error(chalk.red('--keys is required for merge format.'));
            process.exit(1);
          }
          process.stdout.write(toMerge(result, sqlOpts) + '\n');
        } else {
          console.log(formatTable(result));
        }
      } else {
        const result = await conn.execute(sql);
        console.log(formatExecResult(result));
      }
    } catch (err) {
      console.error(chalk.red(`Error: ${err.message}`));
      process.exit(1);
    } finally {
      await conn.disconnect().catch(() => {});
    }
  });

// ─── strsql import <file>  (non-interactive import) ──────────────────────────
program
  .command('import <file>')
  .description('Import a file into IBM i (CSV, JSON, SQL)')
  .option('-p, --profile <n>',       'Named connection profile')
  .option('-H, --host <host>',       'IBM i hostname')
  .option('-u, --user <user>',       'Username')
  .option('--password <password>',   'Password')
  .option('-s, --schema <schema>',   'Default schema')
  .option('-l, --library-list <libs>', 'IBM i library list (comma-separated)')
  .option('-t, --table <table>',     'Target table e.g. MYLIB.ORDERS  (required for CSV/JSON)')
  .option('-m, --mode <mode>',       'Error mode: abort|skip  (default: abort)', 'abort')
  .option('-b, --batch <n>',         'Rows per commit (default: 100)', '100')
  .option('--dry-run',               'Parse and validate without writing to DB')
  .option('--map <mapping>',         'Column mapping: srcCol=DEST,src2=DEST2')
  .option('--delimiter <char>',      'CSV delimiter (default: ,)', ',')
  .action(async (file, opts) => {
    if (opts.host)        process.env.STRSQL_HOST         = opts.host;
    if (opts.user)        process.env.STRSQL_USER         = opts.user;
    if (opts.password)    process.env.STRSQL_PASSWORD     = opts.password;
    if (opts.schema)      process.env.STRSQL_SCHEMA       = opts.schema;
    if (opts.libraryList) process.env.STRSQL_LIBRARY_LIST = opts.libraryList;

    const { IBMiConnection } = require('../src/lib/connection');
    const { ProfileManager } = require('../src/lib/profiles');
    const { Importer, ERROR_MODE } = require('../src/lib/importer');
    const { ProgressBar }    = require('../src/cli/progress');

    const profiles = new ProfileManager();
    const config   = profiles.resolve(opts.profile);

    if (!config.host) {
      console.error(chalk.red('No host specified. Use --host or set STRSQL_HOST.'));
      process.exit(1);
    }

    // Parse column mapping  "A=B,C=D"
    const mapping = {};
    if (opts.map) {
      opts.map.split(',').forEach(pair => {
        const [src, dest] = pair.split('=');
        if (src) mapping[src.trim()] = (dest || src).trim();
      });
    }

    const bar = new ProgressBar('Importing');

    const importOpts = {
      table:     opts.table,
      errorMode: opts.mode === 'skip' ? ERROR_MODE.SKIP : ERROR_MODE.ABORT,
      batchSize: parseInt(opts.batch, 10) || 100,
      dryRun:    !!opts.dryRun,
      mapping,
      delimiter: opts.delimiter,
      onProgress: (done, total) => bar.tick(done, total),
    };

    const conn = new IBMiConnection(config);
    try {
      await conn.connect();
      if (importOpts.dryRun) console.log(chalk.dim('  Dry run — no data will be written.'));

      const importer = new Importer(conn, importOpts);
      const result   = await importer.importFile(require('path').resolve(file));
      bar.finish(result);

      if (result.errors.length > 0) {
        console.log(chalk.yellow(`\nFirst ${Math.min(5, result.errors.length)} error(s):`));
        result.errors.slice(0, 5).forEach((e, i) => {
          console.error(chalk.dim(`  ${i + 1}. ${e.error}`));
        });
        if (!importOpts.dryRun) process.exit(1);
      }
    } catch (err) {
      console.error(chalk.red(`Import failed: ${err.message}`));
      process.exit(1);
    } finally {
      await conn.disconnect().catch(() => {});
    }
  });

// ─── strsql pipe  (DB2 → DB2 transfer) ───────────────────────────────────────
program
  .command('pipe')
  .description('Transfer rows from source IBM i to target IBM i (DB2 → DB2)')
  // ── source ──
  .option('-p, --profile <n>',             'Source connection profile')
  .option('-H, --host <host>',             'Source IBM i hostname')
  .option('-u, --user <user>',             'Source username')
  .option('--password <password>',         'Source password')
  .option('-s, --schema <schema>',         'Source default schema')
  .option('-l, --library-list <libs>',     'Source IBM i library list (comma-separated)')
  .option('--source-table <table>',        'Source table  e.g. SRCLIB.ORDERS')
  .option('--sql <select>',                'Override: full SELECT on source')
  .option('--where <condition>',           'WHERE clause appended to source SELECT')
  // ── target ──
  .option('--target-profile <n>',          'Target connection profile')
  .option('--target-host <host>',          'Target IBM i hostname')
  .option('--target-user <user>',          'Target username')
  .option('--target-password <password>',  'Target password')
  .option('--target-schema <schema>',      'Target default schema')
  .option('--target-library-list <libs>',  'Target IBM i library list (comma-separated)')
  .option('--target-table <table>',        'Target table (default: same as source)')
  // ── transfer ──
  .option('--mode <mode>',                 'Transfer mode: insert|merge  (default: insert)', 'insert')
  .option('--keys <keys>',                 'Key columns for MERGE  e.g. ORDNUM,CUSNUM')
  .option('-b, --batch <n>',              'Rows per page/commit  (default: 500)', '500')
  .option('--map <mapping>',               'Column mapping  srcCol=DESTCOL,…')
  .option('--truncate',                    'DELETE FROM target before transfer')
  .option('--ddl',                         'CREATE TABLE on target from source schema')
  .option('--drop-if-exists',              'DROP TABLE before --ddl')
  .option('--mode-on-error <mode>',        'Error handling: abort|skip  (default: abort)', 'abort')
  .option('--dry-run',                     'Fetch source rows, skip writes to target')
  .action(async (opts) => {
    // source env
    if (opts.host)        process.env.STRSQL_HOST         = opts.host;
    if (opts.user)        process.env.STRSQL_USER         = opts.user;
    if (opts.password)    process.env.STRSQL_PASSWORD     = opts.password;
    if (opts.schema)      process.env.STRSQL_SCHEMA       = opts.schema;
    if (opts.libraryList) process.env.STRSQL_LIBRARY_LIST = opts.libraryList;

    const { IBMiConnection } = require('../src/lib/connection');
    const { ProfileManager } = require('../src/lib/profiles');
    const { Pipe }           = require('../src/lib/pipe');
    const { ERROR_MODE }     = require('../src/lib/importer');
    const { ProgressBar }    = require('../src/cli/progress');

    const profiles   = new ProfileManager();
    const srcConfig  = profiles.resolve(opts.profile);

    // Validate source
    if (!srcConfig.host) {
      console.error(chalk.red('No source host. Use --host or --profile.'));
      process.exit(1);
    }

    if (!opts.sourceTable && !opts.sql) {
      console.error(chalk.red('Specify --source-table <table> or --sql "SELECT ..."'));
      process.exit(1);
    }

    // Resolve target config
    let tgtConfig;
    if (opts.targetProfile) {
      tgtConfig = profiles.resolve(opts.targetProfile);
    } else if (opts.targetHost) {
      tgtConfig = {
        host:          opts.targetHost,
        username:      opts.targetUser,
        password:      opts.targetPassword,
        defaultSchema: opts.targetSchema,
        libraryList:   opts.targetLibraryList,
      };
    } else {
      console.error(chalk.red('Specify target: --target-profile <n> or --target-host <h>'));
      process.exit(1);
    }

    // Column mapping
    const mapping = {};
    if (opts.map) {
      opts.map.split(',').forEach(pair => {
        const [s, d] = pair.split('=');
        if (s) mapping[s.trim()] = (d || s).trim();
      });
    }

    const srcConn = new IBMiConnection(srcConfig);
    const tgtConn = new IBMiConnection(tgtConfig);
    const bar     = new ProgressBar('Pipe');

    const pipeOpts = {
      sourceTable:  opts.sourceTable,
      targetTable:  opts.targetTable || opts.sourceTable,
      sourceSQL:    opts.sql,
      where:        opts.where,
      mode:         opts.mode,
      keys:         opts.keys ? opts.keys.split(',').map(k => k.trim()) : [],
      batchSize:    parseInt(opts.batch, 10) || 500,
      mapping,
      truncate:     !!opts.truncate,
      generateDDL:  !!opts.ddl,
      dropIfExists: !!opts.dropIfExists,
      dryRun:       !!opts.dryRun,
      errorMode:    opts.modeOnError === 'skip' ? ERROR_MODE.SKIP : ERROR_MODE.ABORT,
      onProgress:   (written, total) => bar.tick(written, total),
    };

    try {
      process.stdout.write(chalk.dim(`Connecting source ${srcConfig.host}…`));
      await srcConn.connect();
      console.log(chalk.green(' ✓'));

      process.stdout.write(chalk.dim(`Connecting target ${tgtConfig.host}…`));
      await tgtConn.connect();
      console.log(chalk.green(' ✓'));

      if (pipeOpts.dryRun) console.log(chalk.dim('Dry run — nothing written to target.'));

      const pipe   = new Pipe(srcConn, tgtConn, pipeOpts);
      const result = await pipe.run();

      bar.finish({
        inserted: result.totalWritten,
        skipped:  result.totalSkipped,
        errors:   result.errors,
        elapsed:  result.elapsed,
        dryRun:   result.dryRun,
      });

      if (result.ddlExecuted) console.log(chalk.dim('DDL executed on target.'));
      if (result.truncated)   console.log(chalk.dim('Target table truncated before transfer.'));

      if (result.errors.length > 0) {
        console.log(chalk.yellow(`\nFirst ${Math.min(5, result.errors.length)} error(s):`));
        result.errors.slice(0, 5).forEach((e, i) =>
          console.error(chalk.dim(`  ${i + 1}. [page ${e.page}] ${e.error}`))
        );
        process.exit(1);
      }
    } catch (err) {
      console.error(chalk.red(`Pipe failed: ${err.message}`));
      process.exit(1);
    } finally {
      await srcConn.disconnect().catch(() => {});
      await tgtConn.disconnect().catch(() => {});
    }
  });



const profilesCmd = program
  .command('profiles')
  .description('Manage named connection profiles');

profilesCmd
  .command('list')
  .description('List all saved profiles')
  .action(() => {
    const pm = new ProfileManager();
    const list = pm.list();
    if (list.length === 0) {
      console.log(chalk.dim('No profiles saved.'));
      return;
    }
    console.log(chalk.bold('\nSaved profiles:\n'));
    for (const p of list) {
      const hostOrDb = p.database || p.host || '';
      console.log(
        `  ${chalk.cyan(p.name.padEnd(18))} ${chalk.yellow((p.type || 'ibmi').padEnd(12))} ${hostOrDb}` +
        (p.username      ? chalk.dim(`  user=${p.username}`)      : '') +
        (p.defaultSchema ? chalk.dim(`  schema=${p.defaultSchema}`) : '')
      );
    }
    console.log();
  });

profilesCmd
  .command('add <name>')
  .description('Add or update a profile')
  .option('--type <type>', 'Database type (e.g. ibmi, mssql, mysql)', 'ibmi')
  .requiredOption('-H, --host <host>', 'IBM i hostname')
  .option('-u, --user <user>', 'Username')
  .option('--password <password>', 'Password (stored in plain text)')
  .option('-s, --schema <schema>', 'Default schema/library')
  .option('-l, --library-list <libs>', 'IBM i library list (comma-separated)')
  .option('--naming <mode>', 'Naming mode: sql or system', 'sql')
  .action((name, opts) => {
    const pm = new ProfileManager();
    pm.set(name, {
      type: opts.type,
      host: opts.host,
      username: opts.user,
      password: opts.password,
      defaultSchema: opts.schema,
      libraryList: opts.libraryList,
      namingMode: opts.naming,
    });
    console.log(chalk.green(`Profile "${name}" saved.`));
  });

profilesCmd
  .command('remove <name>')
  .description('Delete a profile')
  .action((name) => {
    const pm = new ProfileManager();
    pm.remove(name);
    console.log(chalk.green(`Profile "${name}" deleted.`));
  });


// ─── strsql drivers  (list supported DB types) ───────────────────────────────
program
  .command('drivers')
  .description('List all supported ODBC database types')
  .action(() => {
    const { listDrivers } = require('../src/lib/drivers');
    console.log(chalk.bold('\nSupported database types:\n'));
    for (const d of listDrivers()) {
      console.log(`  ${chalk.cyan(d.type.padEnd(14))} ${d.label}`);
    }
    console.log(chalk.dim('\n  Use --type <type> when adding a profile.\n'));
  });

program.parse(process.argv);
