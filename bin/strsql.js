#!/usr/bin/env node
'use strict';

require('dotenv').config();

const { Command } = require('commander');
const chalk = require('chalk');

const { STRSQLSession }  = require('../src/cli/session');
const { ProfileManager } = require('../src/lib/profiles');
const pkg = require('../package.json');

const program = new Command();

function applyConnectionEnv(opts) {
  if (opts.host)        process.env.STRSQL_HOST         = opts.host;
  if (opts.user)        process.env.STRSQL_USER         = opts.user;
  if (opts.password)    process.env.STRSQL_PASSWORD     = opts.password;
  if (opts.schema)      process.env.STRSQL_SCHEMA       = opts.schema;
  if (opts.libraryList) process.env.STRSQL_LIBRARY_LIST = opts.libraryList;
  if (opts.adapter)     process.env.STRSQL_ADAPTER      = opts.adapter;
}

async function runSingleStatement(statement, opts) {
  applyConnectionEnv(opts);

  const { IBMiConnection } = require('../src/lib/connection');
  const { ProfileManager } = require('../src/lib/profiles');
  const { formatTable, formatExecResult, toCSV, toJSON, toInsert, toMerge, exportToFile } = require('../src/lib/formatter');

  const profiles = new ProfileManager();
  const config = profiles.resolve(opts.profile);

  if (!config.host) {
    console.error(chalk.red('No host specified. Use --host or set STRSQL_HOST.'));
    process.exit(1);
  }

  const sqlOpts = {
    table: opts.table,
    keys: opts.keys ? opts.keys.split(',').map(k => k.trim()) : [],
    batch: parseInt(opts.batch, 10) || 1,
  };

  const conn = new IBMiConnection(config);
  try {
    await conn.connect();
    const upper = statement.trim().toUpperCase();
    const isSelect = upper.startsWith('SELECT') || upper.startsWith('WITH') || upper.startsWith('VALUES');

    if (isSelect) {
      const result = await conn.query(statement);
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
      const result = await conn.execute(statement);
      console.log(formatExecResult(result));
    }
  } catch (err) {
    console.error(chalk.red(`Error: ${err.message}`));
    process.exit(1);
  } finally {
    await conn.disconnect().catch(() => {});
  }
}

program
  .name('strsql')
  .description('IBM i STRSQL emulator via ODBC')
  .version(pkg.version);

// ─── strsql  (interactive session) ──────────────────────────────────────────
program
  .command('session', { isDefault: true })
  .description('Start an interactive SQL session (default)')
  .helpOption('--help', 'display help for command')
  .option('-p, --profile <name>',  'Named connection profile to use')
  .option('-h, --host <host>',     'IBM i hostname (overrides profile)')
  .option('-u, --user <user>',     'Username')
  .option('--password <password>', 'Password (prefer STRSQL_PASSWORD env var)')
  .option('-s, --schema <schema>', 'Default schema/library')
  .option('-l, --library-list <libs>', 'IBM i library list (comma-separated)')
  .option('--adapter <adapter>',    'Connection adapter: odbc|idb (IBM i only)')
  .option('-q, --query <sql>',      'Execute a single SQL statement and exit')
  .option('-f, --format <fmt>',     'Output format for -q: table|csv|json|insert|merge', 'table')
  .option('-o, --out <file>',       'Export result for -q (.csv/.json/.sql/.insert.sql/.merge.sql)')
  .option('--table <table>',        'Target table for SQL export with -q')
  .option('--keys <keys>',          'Key columns for MERGE with -q, comma-separated')
  .option('--batch <n>',            'Rows per INSERT statement with -q (default 1)', '1')
  .option('--max-cell-width <n>',  'Max column width in table output (default: auto)')
  .action(async (opts) => {
    if (opts.query && opts.query.trim()) {
      await runSingleStatement(opts.query, opts);
      return;
    }

    // CLI flags override ENV and profile
    applyConnectionEnv(opts);

    const session = new STRSQLSession({
      profile:      opts.profile,
      maxCellWidth: opts.maxCellWidth ? parseInt(opts.maxCellWidth, 10) : undefined,
    });

    await session.start(opts.profile);
  });

// ─── strsql run [sql]  (non-interactive single query) ────────────────────────
program
  .command('run [sql]')
  .description('Execute a single SQL statement and exit')
  .helpOption('--help', 'display help for command')
  .option('-q, --query <sql>',      'SQL query/statement to execute')
  .option('-p, --profile <name>',   'Named connection profile')
  .option('-h, --host <host>',      'IBM i hostname')
  .option('-u, --user <user>',      'Username')
  .option('--password <password>',  'Password')
  .option('-s, --schema <schema>',  'Default schema')
  .option('-l, --library-list <libs>', 'IBM i library list (comma-separated)')
  .option('--adapter <adapter>',    'Connection adapter: odbc|idb (IBM i only)')
  .option('-f, --format <fmt>',     'Output format: table|csv|json|insert|merge', 'table')
  .option('-o, --out <file>',       'Export result to file (.csv/.json/.sql/.insert.sql/.merge.sql)')
  .option('--table <table>',        'Target table name for SQL export (e.g. MYLIB.ORDERS)')
  .option('--keys <keys>',          'Key columns for MERGE, comma-separated (e.g. ORDNUM,CUSNUM)')
  .option('--batch <n>',            'Rows per INSERT statement (default 1)', '1')
  .action(async (sql, opts) => {
    const statement = opts.query || sql;
    if (!statement || !statement.trim()) {
      console.error(chalk.red('No SQL specified. Use: strsql run -q "SELECT ..."'));
      process.exit(1);
    }
    await runSingleStatement(statement, opts);
  });

// ─── strsql import <file>  (non-interactive import) ──────────────────────────
program
  .command('import <file>')
  .description('Import a file into IBM i (CSV, JSON, SQL)')
  .helpOption('--help', 'display help for command')
  .option('-p, --profile <n>',       'Named connection profile')
  .option('-h, --host <host>',       'IBM i hostname')
  .option('-u, --user <user>',       'Username')
  .option('--password <password>',   'Password')
  .option('-s, --schema <schema>',   'Default schema')
  .option('-l, --library-list <libs>', 'IBM i library list (comma-separated)')
  .option('--adapter <adapter>',     'Connection adapter: odbc|idb (IBM i only)')
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
    if (opts.adapter)     process.env.STRSQL_ADAPTER      = opts.adapter;
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
  .helpOption('--help', 'display help for command')
  // ── source ──
  .option('-p, --profile <n>',             'Source connection profile')
  .option('-h, --host <host>',             'Source IBM i hostname')
  .option('-u, --user <user>',             'Source username')
  .option('--password <password>',         'Source password')
  .option('-s, --schema <schema>',         'Source default schema')
  .option('-l, --library-list <libs>',     'Source IBM i library list (comma-separated)')
  .option('--adapter <adapter>',           'Source adapter: odbc|idb (IBM i only)')
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
  .option('--target-adapter <adapter>',    'Target adapter: odbc|idb (IBM i only)')
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
    if (opts.adapter) srcConfig.adapter = opts.adapter;

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
      if (opts.targetAdapter) tgtConfig.adapter = opts.targetAdapter;
    } else if (opts.targetHost) {
      tgtConfig = {
        host:          opts.targetHost,
        username:      opts.targetUser,
        password:      opts.targetPassword,
        defaultSchema: opts.targetSchema,
        libraryList:   opts.targetLibraryList,
        adapter:       opts.targetAdapter || opts.adapter,
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
        `  ${chalk.cyan(p.name.padEnd(18))} ${chalk.yellow((p.type || 'ibmi').padEnd(12))} ${chalk.magenta((p.adapter || 'odbc').padEnd(6))} ${hostOrDb}` +
        (p.username      ? chalk.dim(`  user=${p.username}`)      : '') +
        (p.defaultSchema ? chalk.dim(`  schema=${p.defaultSchema}`) : '')
      );
    }
    console.log();
  });

profilesCmd
  .command('add <name>')
  .description('Add or update a profile')
  .helpOption('--help', 'display help for command')
  .option('--type <type>', 'Database type (e.g. ibmi, mssql, mysql)', 'ibmi')
  .option('--adapter <adapter>', 'Connection adapter: odbc|idb (IBM i only)', 'odbc')
  .requiredOption('-h, --host <host>', 'IBM i hostname')
  .option('-u, --user <user>', 'Username')
  .option('-p, --password <password>', 'Password (stored in plain text)')
  .option('-s, --schema <schema>', 'Default schema/library')
  .option('-l, --library-list <libs>', 'IBM i library list (comma-separated)')
  .option('--naming <mode>', 'Naming mode: sql or system', 'sql')
  .option('--migration-table <name>', 'Default migration tracking table (e.g. MYLIB.MIGRATION_LOG)')
  .option('--seed-table <name>',      'Default seed tracking table (e.g. MYLIB.SEED_LOG)')
  .action((name, opts) => {
    const pm = new ProfileManager();
    pm.set(name, {
      type: opts.type,
      adapter: opts.adapter,
      host: opts.host,
      username: opts.user,
      password: opts.password,
      defaultSchema: opts.schema,
      libraryList: opts.libraryList,
      namingMode: opts.naming,
      ...(opts.migrationTable && { migrationTable: opts.migrationTable }),
      ...(opts.seedTable      && { seedTable:      opts.seedTable }),
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
    console.log(chalk.dim('\n  Use --type <type> when adding a profile.'));
    console.log(chalk.dim('  IBM i supports --adapter odbc (default) or --adapter idb on IBM i/PASE.\n'));
  });

// ─── helpers for migration/seed commands ─────────────────────────────────────

/**
 * Parse "SCHEMA.TABLE" or plain "TABLE" into its parts.
 * Returns { schema, table, qualified }.
 */
function parseTableRef(tableRef, defaultSchema) {
  if (tableRef.includes('.')) {
    const dot    = tableRef.indexOf('.');
    const schema = tableRef.slice(0, dot);
    const table  = tableRef.slice(dot + 1);
    return { schema, table, qualified: tableRef };
  }
  const schema = defaultSchema || '';
  return { schema, table: tableRef, qualified: schema ? `${schema}.${tableRef}` : tableRef };
}

// ─── strsql migrations run|create ────────────────────────────────────────────
const migrationsCmd = program
  .command('migrations')
  .description('Manage database migrations');

migrationsCmd
  .command('run <path> [action]')
  .description('Run migrations against the connected DB (default action: up)')
  .helpOption('--help', 'display help for command')
  .option('-p, --profile <name>',          'Named connection profile')
  .option('-h, --host <host>',             'Hostname')
  .option('-u, --user <user>',             'Username')
  .option('--password <password>',         'Password')
  .option('-s, --schema <schema>',         'Default schema (also used to qualify the tracking table)')
  .option('-l, --library-list <libs>',     'IBM i library list (comma-separated)')
  .option('--adapter <adapter>',           'Connection adapter: odbc|idb (IBM i only)')
  .option('--migration-table <name>',      'Tracking table name (overrides profile setting, default: MIGRATION_LOG)')
  .action(async (migrationsPath, action, opts) => {
    const resolvedAction = action || 'up';
    if (!['up', 'down'].includes(resolvedAction)) {
      console.error(chalk.red(`Invalid action "${resolvedAction}". Use "up" or "down".`));
      process.exit(1);
    }
    applyConnectionEnv(opts);
    const profiles = new ProfileManager();
    const config   = profiles.resolve(opts.profile);
    if (!config.host) {
      console.error(chalk.red('No host specified. Use --host or --profile.'));
      process.exit(1);
    }
    const { runMigrations } = require('../src/lib/migrations/migrationRunner');
    try {
      await runMigrations(resolvedAction, {
        config,
        migrationsPath: require('path').resolve(migrationsPath),
        migrationTable: opts.migrationTable || config.migrationTable || 'MIGRATION_LOG',
        schema:         config.defaultSchema || '',
      });
    } catch (err) {
      console.error(chalk.red(`Migrations failed: ${err.message}`));
      process.exit(1);
    }
  });

migrationsCmd
  .command('create <path> <name>')
  .description('Create a new migration file pair (.up.sql / .down.sql)')
  .helpOption('--help', 'display help for command')
  .option('--from-table <TABLE>',          'Populate .up.sql with CREATE TABLE DDL from DB')
  .option('-p, --profile <name>',          'Named connection profile (required with --from-table)')
  .option('-h, --host <host>',             'Hostname')
  .option('-u, --user <user>',             'Username')
  .option('--password <password>',         'Password')
  .option('-s, --schema <schema>',         'Default schema')
  .option('-l, --library-list <libs>',     'IBM i library list (comma-separated)')
  .option('--adapter <adapter>',           'Connection adapter: odbc|idb (IBM i only)')
  .action(async (migrationsPath, name, opts) => {
    const { createMigration } = require('../src/lib/migrations/create');

    let upContent, downContent;

    if (opts.fromTable) {
      applyConnectionEnv(opts);
      const profiles = new ProfileManager();
      const config   = profiles.resolve(opts.profile);
      if (!config.host) {
        console.error(chalk.red('No host specified for --from-table. Use --host or --profile.'));
        process.exit(1);
      }
      const { IBMiConnection } = require('../src/lib/connection');
      const { generateDDL }    = require('../src/lib/pipe');
      const conn = new IBMiConnection(config);
      try {
        await conn.connect();
        const { schema, table, qualified } = parseTableRef(opts.fromTable, config.defaultSchema);
        const descResult = await conn.describeTable(table, schema);
        if (descResult.rows.length === 0) {
          console.error(chalk.red(`No columns found for table ${qualified}.`));
          process.exit(1);
        }
        upContent   = generateDDL(qualified, descResult, conn.dbType) + '\n';
        downContent = `DROP TABLE ${qualified};\n`;
      } finally {
        await conn.disconnect().catch(() => {});
      }
    }

    const { upFile, downFile } = createMigration(name, migrationsPath, { upContent, downContent });
    console.log(chalk.green(`Created migration files:\n  ${upFile}\n  ${downFile}`));
  });

// ─── strsql seeds run|create ──────────────────────────────────────────────────
const seedsCmd = program
  .command('seeds')
  .description('Manage database seeds');

seedsCmd
  .command('run <path> [action]')
  .description('Run seeds against the connected DB (default action: up)')
  .helpOption('--help', 'display help for command')
  .option('-p, --profile <name>',          'Named connection profile')
  .option('-h, --host <host>',             'Hostname')
  .option('-u, --user <user>',             'Username')
  .option('--password <password>',         'Password')
  .option('-s, --schema <schema>',         'Default schema (also used to qualify the tracking table)')
  .option('-l, --library-list <libs>',     'IBM i library list (comma-separated)')
  .option('--adapter <adapter>',           'Connection adapter: odbc|idb (IBM i only)')
  .option('--seed-table <name>',           'Tracking table name (overrides profile setting, default: SEED_LOG)')
  .action(async (seedsPath, action, opts) => {
    const resolvedAction = action || 'up';
    if (!['up', 'down'].includes(resolvedAction)) {
      console.error(chalk.red(`Invalid action "${resolvedAction}". Use "up" or "down".`));
      process.exit(1);
    }
    applyConnectionEnv(opts);
    const profiles = new ProfileManager();
    const config   = profiles.resolve(opts.profile);
    if (!config.host) {
      console.error(chalk.red('No host specified. Use --host or --profile.'));
      process.exit(1);
    }
    const { runSeeds } = require('../src/lib/migrations/seedRunner');
    try {
      await runSeeds(resolvedAction, {
        config,
        seedsPath: require('path').resolve(seedsPath),
        seedTable: opts.seedTable || config.seedTable || 'SEED_LOG',
        schema:    config.defaultSchema || '',
      });
    } catch (err) {
      console.error(chalk.red(`Seeds failed: ${err.message}`));
      process.exit(1);
    }
  });

seedsCmd
  .command('create <path> <name>')
  .description('Create a new seed file pair (.up.sql / .down.sql)')
  .helpOption('--help', 'display help for command')
  .option('-q, --query <sql>',             'SQL query whose results become the seed data')
  .option('--from-table <TABLE>',          'Use SELECT * FROM <TABLE> as the seed query')
  .option('--table <table>',               'Target table for INSERT/UPSERT statements (required with -q)')
  .option('--format <fmt>',               'Output format: insert|upsert (default: insert)', 'insert')
  .option('--keys <cols>',                 'Key columns for upsert, comma-separated (required with --format upsert and -q)')
  .option('-b, --batch <n>',              'Rows per INSERT statement (default: 100)', '100')
  .option('-p, --profile <name>',          'Named connection profile')
  .option('-h, --host <host>',             'Hostname')
  .option('-u, --user <user>',             'Username')
  .option('--password <password>',         'Password')
  .option('-s, --schema <schema>',         'Default schema')
  .option('-l, --library-list <libs>',     'IBM i library list (comma-separated)')
  .option('--adapter <adapter>',           'Connection adapter: odbc|idb (IBM i only)')
  .action(async (seedsPath, name, opts) => {
    const { createSeed } = require('../src/lib/migrations/create');

    let upContent, downContent;

    if (opts.query || opts.fromTable) {
      if (opts.format === 'upsert' && !opts.fromTable && !opts.keys) {
        console.error(chalk.red('--keys is required with --format upsert and -q.'));
        process.exit(1);
      }
      if (opts.query && !opts.fromTable && !opts.table) {
        console.error(chalk.red('--table is required with -q to know the target table for INSERT statements.'));
        process.exit(1);
      }

      applyConnectionEnv(opts);
      const profiles = new ProfileManager();
      const config   = profiles.resolve(opts.profile);
      if (!config.host) {
        console.error(chalk.red('No host specified. Use --host or --profile.'));
        process.exit(1);
      }

      const { IBMiConnection } = require('../src/lib/connection');
      const { toInsert, toMerge } = require('../src/lib/formatter');

      let sql, targetTable, keys = [];

      if (opts.fromTable) {
        const { schema, table, qualified } = parseTableRef(opts.fromTable, config.defaultSchema);
        sql         = `SELECT * FROM ${qualified}`;
        targetTable = opts.table || qualified;
        opts._fromSchema = schema;
        opts._fromTable  = table;
      } else {
        sql         = opts.query;
        targetTable = opts.table;
      }

      const conn = new IBMiConnection(config);
      try {
        await conn.connect();

        if (opts.format === 'upsert') {
          if (opts.keys) {
            keys = opts.keys.split(',').map(k => k.trim());
          } else {
            const pkSet = await conn.primaryKeys(opts._fromTable, opts._fromSchema);
            keys = [...pkSet];
            if (keys.length === 0) {
              console.error(chalk.red(`No primary key found for ${opts.fromTable}. Specify --keys manually.`));
              process.exit(1);
            }
          }
        }

        const result = await conn.query(sql);
        const batch  = parseInt(opts.batch, 10) || 100;

        upContent   = opts.format === 'upsert'
          ? toMerge(result, { table: targetTable, keys, batch, dialect: conn.dbType })
          : toInsert(result, { table: targetTable, batch });
        downContent = `DELETE FROM ${targetTable};\n`;
      } finally {
        await conn.disconnect().catch(() => {});
      }
    }

    const { upFile, downFile } = createSeed(name, seedsPath, { upContent, downContent });
    console.log(chalk.green(`Created seed files:\n  ${upFile}\n  ${downFile}`));
  });

program.parse(process.argv);
