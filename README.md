# strsql-node

> Interactive SQL session emulator for Node.js via ODBC — IBM i/AS400 and beyond

`strsql-node` emulates an interactive `STRSQL` session and provides a full toolkit for querying, exporting, importing, and transferring data across databases via ODBC. It ships as both a **CLI tool** and a **programmatic library**.

> **⚠ Alpha software** — This package is in early alpha. Only the **IBM i** and **MySQL** ODBC drivers have been tested so far. Other supported databases (SQL Server, PostgreSQL, Oracle, DB2 LUW, SQLite) are implemented but untested — use them at your own risk and please report any issues.

## Key features

| Feature | Description |
|---|---|
| **Interactive REPL** | Full `STRSQL`-style session with multi-line SQL, `\commands`, inline history navigation, and persistent history |
| **Multi-database support** | IBM i/AS400, SQL Server, PostgreSQL, MySQL/MariaDB, Oracle, DB2 LUW, SQLite — each with native catalog, pagination, and dialect support |
| **Single-query CLI** | Run any SQL non-interactively and get output as table, CSV, JSON, INSERT, or MERGE/upsert statements |
| **DB-to-DB Pipe** | Stream rows directly between two database connections (same or different engines) with DDL auto-generation, merge/upsert, column mapping, and no intermediate files |
| **Export** | Dump query results to `.csv`, `.json`, `.sql` (INSERT), `.insert.sql`, or `.merge.sql` — from CLI or interactive session |
| **Import** | Load `.csv`, `.json`, or `.sql` files into any supported database, with column mapping, batch size control, dry-run, and skip-on-error modes |
| **Dialect engine** | Handles identifier quoting, value literals, upsert strategy, pagination syntax, and DDL type mapping per database |
| **Connection profiles** | Named profiles stored in `~/.strsql-node/profiles.json` for quick multi-environment switching |
| **Programmatic API** | `ODBCConnection`, `Importer`, `Pipe`, `Dialect`, and formatter functions fully usable as a Node.js library |

---

## Table of contents

- [Supported databases](#supported-databases)
- [Requirements](#requirements)
- [Installation](#installation)
- [Environment variables](#environment-variables)
- [Connection profiles](#connection-profiles)
- [Quick start — CLI](#quick-start--cli)
- [Interactive session commands](#interactive-session-commands)
  - [Connection](#connection)
  - [Schema & objects](#schema--objects)
  - [SQL execution](#sql-execution)
  - [Export](#export)
  - [Import](#import)
  - [DB2 → DB2 Pipe](#db2--db2-pipe)
  - [DDL generation](#ddl-generation)
  - [History](#history)
- [CLI subcommands](#cli-subcommands)
- [Programmatic API](#programmatic-api)
  - [ODBCConnection](#odbcconnection)
  - [Importer](#importer)
  - [Pipe](#pipe)
  - [Dialect](#dialect)
  - [Formatters](#formatters)
- [Dialect reference](#dialect-reference)
- [File layout](#file-layout)
- [Security notes](#security-notes)
- [Publishing to npm](#publishing-to-npm)
- [License](#license)

---

## Supported databases

| `--type` | Database | ODBC Driver |
|---|---|---|
| `ibmi` *(default)* | IBM i / AS400 / DB2 for i | IBM i Access ODBC Driver |
| `sqlserver` | Microsoft SQL Server | ODBC Driver 17 or 18 for SQL Server |
| `postgresql` | PostgreSQL | PostgreSQL Unicode (psqlODBC) |
| `mysql` | MySQL / MariaDB | MySQL Connector/ODBC 8.x or 9.x |
| `oracle` | Oracle | Oracle ODBC Driver 21 |
| `db2` | DB2 LUW (Linux/Windows/AIX) | IBM DB2 ODBC DRIVER |
| `sqlite` | SQLite | SQLite3 ODBC Driver |

Each driver knows the correct ODBC connection string format, catalog queries, pagination syntax, literal formatting, upsert strategy, and DDL type mapping for its target database.

> **Driver name mismatch?** If the installed ODBC driver has a different version name than the default (e.g. `MySQL ODBC 9.6 Unicode Driver` instead of `8.0`), add `--driver-name` when saving the profile or set `"driverName"` in `~/.strsql-node/profiles.json`.

---

## Requirements

| Requirement | Notes |
|---|---|
| Node.js ≥ 16 | https://nodejs.org/en |
| `odbc` npm package | Requires native build (`node-gyp`) |
| ODBC driver for your DB | See table above |
| unixODBC (Linux / PASE) | `sudo apt-get install unixodbc unixodbc-dev` |

```bash
# Linux / IBM i PASE
sudo apt-get install unixodbc unixodbc-dev build-essential

# macOS
brew install unixodbc
```

For Windows see doc here: https://ibmi-oss-docs.readthedocs.io/en/latest/odbc/installation.html#windows


---

## Installation

```bash
npm install -g strsql-node
```

Or run directly without installing:

```bash
node bin/strsql.js [command] [options]
```

## Publishing to npm

Use the package script to publish the current version to npm:

```bash
npm run publish:npm
```

Typical release flow:

```bash
# 1) run tests
npm test

# 2) bump version
npm version patch

# 3) publish
npm run publish:npm
```

Note: make sure you are logged in with `npm login` and have publish rights on `strsql-node`.

---

## Environment variables

| Variable | Description |
|---|---|
| `STRSQL_HOST` | Hostname / IP (or file path for SQLite) |
| `STRSQL_USER` | Username |
| `STRSQL_PASSWORD` | Password |
| `STRSQL_SCHEMA` | Default schema / library |
| `STRSQL_LIBRARY_LIST` | IBM i library list (comma-separated, e.g. `MYLIB,QGPL,QUSRSYS`) |

Copy `.env.example` → `.env` in the working directory. CLI flags always win over environment variables, which win over saved profiles.

---

## Connection profiles

Profiles are stored in `~/.strsql-node/profiles.json`. Each profile carries the connection type and all parameters needed to connect to a specific database.

```bash
# List all profiles
strsql profiles list

# Add profiles for different databases
strsql profiles add ibmi-prod  --type ibmi        --host 10.0.0.1  -u PRODUSER --password s3c -s PRODLIB -l PRODLIB,QGPL,QUSRSYS
strsql profiles add pg-sales   --type postgresql  --host pg.local  -u admin    --password s3c --database sales --schema public --ssl require
strsql profiles add ss-dev     --type sqlserver   --host ss.local  -u sa       --password s3c --database DevDB --instance DEV
strsql profiles add ora-uat    --type oracle      --host ora.local -u SYS      --password s3c --service ORCL
strsql profiles add mysql-log  --type mysql       --host my.local  -u root     --password s3c --database logs
strsql profiles add db2-dw     --type db2         --host db2.local -u db2user  --password s3c --database DW   --port 50000
strsql profiles add sqlite-dev --type sqlite      --host /data/dev.db

# Remove a profile
strsql profiles remove pg-sales

# List supported DB types
strsql drivers
```

---

## Quick start — CLI

### Interactive session

```bash
strsql                                    # uses STRSQL_* env vars or .env
strsql --host 10.0.0.1 -u MYUSER --password secret
strsql --profile ibmi-prod                # IBM i saved profile (library list from profile)
strsql --profile ibmi-prod -l MYLIB,TESTLIB  # override library list
strsql --profile pg-sales                 # PostgreSQL saved profile
strsql --type sqlserver --host ss.local -u sa --password s3c --database MyDB
```

### Single query (non-interactive)

```bash
# Table output (default)
strsql run "SELECT * FROM MYLIB.ORDERS FETCH FIRST 10 ROWS ONLY" --profile ibmi-prod

# Different formats
strsql run "SELECT * FROM MYLIB.ORDERS" --format csv
strsql run "SELECT * FROM MYLIB.ORDERS" --format json  --out orders.json
strsql run "SELECT * FROM MYLIB.ORDERS" --format insert --table MYLIB.ORDERS_COPY
strsql run "SELECT * FROM MYLIB.ORDERS" --format merge  --table MYLIB.ORDERS_COPY --keys ORDNUM

# With explicit type (no profile)
strsql run "SELECT * FROM orders" --type postgresql --host pg.local -u admin --database sales
```

### Import a file

```bash
strsql import orders.csv   --profile ibmi-prod --table MYLIB.ORDERS
strsql import orders.json  --profile pg-sales
strsql import orders.sql   --profile ibmi-prod --mode skip
strsql import orders.csv   --profile ibmi-prod --table MYLIB.ORDERS \
       --map "order_no=ORDNUM,customer=CUSNAM" --batch 500 --dry-run
```

### DB2-to-DB2 pipe (direct transfer)

```bash
# IBM i → IBM i
strsql pipe --source-table SRCLIB.ORDERS --target-profile ibmi-prod2 --target-table TGTLIB.ORDERS

# IBM i → PostgreSQL (cross-database)
strsql pipe --source-table SRCLIB.ORDERS \
       --target-profile pg-sales --target-table public.orders \
       --mode merge --keys ORDNUM --batch 1000 --ddl

# Custom SELECT → target
strsql pipe --sql "SELECT * FROM SRCLIB.ORDERS WHERE YEAR=2024" \
       --target-host pg.local --target-user admin --target-password s3c \
       --target-table public.orders_2024 --ddl --drop-if-exists
```

---

## Interactive session commands

Start with `strsql` or `strsql --profile <n>`. Type `\help` inside the session for the full command list.

### Connection

| Command | Description |
|---|---|
| `\connect <host> [user] [pwd] [schema]` | Connect (IBM i, backward compat) |
| `\connect --type TYPE --host H --user U --password P [--database DB] [--port N]` | Connect to any supported DB |
| `\connect --host H --user U --password P --library-list LIB1,LIB2` | Connect with IBM i library list |
| `\disconnect` | Close current connection |
| `\profile <n>` | Switch to a saved profile |
| `\status` | Show connection status and DB type |
| `\drivers` | List all supported database types |

### Profiles

| Command | Description |
|---|---|
| `\profiles` | List all saved profiles (shows type column) |
| `\saveprofile <n> --type TYPE --host H [--user U] [--password P] [--schema S] [--database DB] [--port N]` | Save a named profile |
| `\delprofile <n>` | Delete a profile |

Additional options for `\saveprofile`:
- `--instance` — SQL Server named instance
- `--service` — Oracle service name
- `--ssl` — PostgreSQL SSL mode (`disable` / `require` / `verify-ca` / `verify-full`)
- `--naming sql|system` — IBM i naming mode
- `--library-list LIB1,LIB2,...` — IBM i library list (alias: `--libl`)

### Schema & objects

| Command | Description |
|---|---|
| `\schema [name]` | Show or set default schema |
| `\libl [LIB1,LIB2,...]` | Show or set IBM i library list |
| `\tables [schema]` | List tables (uses native catalog per DB) |
| `\describe [schema.]TABLE` | Describe table columns |

The `\libl` command calls `CHGLIBL` on the IBM i job via `QSYS2.QCMDEXC`. Without arguments it displays the current library list; with arguments it sets a new one.

### SQL execution

End a statement with `;` or type `GO` / `RUN` on its own line. Multi-line input is supported.

```sql
SQL> SELECT ORDNUM, CUSNAM, ORDDAT
  -> FROM MYLIB.ORDERS
  -> WHERE ORDDAT >= '2024-01-01'
  -> ORDER BY ORDDAT DESC
  -> FETCH FIRST 20 ROWS ONLY;
```

#### Execute SQL from a file

Use `\run` to execute all SQL statements from a file on disk:

```
SQL> \run /path/to/queries.sql
SQL> \run updates.sql --stop-on-error
```

The file is split on `;` delimiters, line comments (`--`) are stripped, and each statement is executed sequentially. By default execution continues on error; use `--stop-on-error` to halt at the first failure. A summary with counts and elapsed time is printed at the end.

### Export

`\export` writes the **last SELECT result** to a file. Run a `SELECT` first, then call `\export`.

```sql
SQL> SELECT * FROM movimenti;
... rows ...
SQL> \export movimenti.sql
SQL> \export movimenti.csv
SQL> \export movimenti.json
```

| Command | Description |
|---|---|
| `\export file.csv` | Export last result as CSV |
| `\export file.json` | Export last result as JSON |
| `\export file.sql` | Export as SQL INSERTs — table name defaults to the filename stem |
| `\export file.insert.sql [--table T] [--batch N]` | INSERT statements (explicit) |
| `\export file.merge.sql --keys COL1,COL2 [--table T]` | Upsert statements |

Options:
- `--table SCHEMA.TABLE` — override target table name (default: filename without extension, e.g. `movimenti.sql` → `movimenti`)
- `--keys COL1,COL2` — join key columns (required for `.merge.sql`)
- `--batch N` — rows per INSERT statement (default: 1)

### Import

| Command | Description |
|---|---|
| `\import file.csv --table SCHEMA.TABLE` | Import CSV |
| `\import file.json [--table T]` | Import JSON array or `{ table, rows }` object |
| `\import file.sql` | Execute SQL statements from file |

Options:
- `--table` — target table (required for CSV; optional for JSON if `table` key present)
- `--mode abort|skip|confirm` — error handling (default: `abort`)
- `--batch N` — rows per commit (default: 100)
- `--dry-run` — parse and validate without writing to DB
- `--map srcCol=DESTCOL,...` — rename or exclude columns (empty dest = exclude)
- `--delimiter CHAR` — CSV field delimiter (default: `,`)

### DB2 → DB2 Pipe

Transfer rows directly between two database connections — no intermediate file.

```
\pipe SRCLIB.ORDERS --target-profile ibmi-prod2
\pipe SRCLIB.ORDERS --target-profile pg-sales --target-table public.orders --mode merge --keys ORDNUM
\pipe SRCLIB.ORDERS --target-host pg.local --target-user admin --target-password s3c --target-table public.orders
\pipe --sql "SELECT * FROM SRCLIB.ORDERS WHERE ACTIVE=1" --target-profile pg-sales --target-table public.orders
```

Options:
- `--target-profile <n>` — use saved profile for target *(or use `--target-host` etc.)*
- `--target-host / --target-user / --target-password / --target-schema`
- `--target-table T` — target table (default: same as source)
- `--mode insert|merge` — transfer mode (default: `insert`)
- `--keys COL1,COL2` — required for merge mode
- `--batch N` — rows per page/commit (default: 500)
- `--truncate` — DELETE FROM target before transfer
- `--ddl` — CREATE TABLE on target from source schema (dialect-aware)
- `--drop-if-exists` — DROP TABLE before `--ddl`
- `--map srcCol=DESTCOL,...` — rename or exclude columns
- `--where "condition"` — filter rows on source (quote the value: `--where 'YEAR = 2024'`)
- `--mode-on-error skip` — skip bad rows instead of aborting
- `--dry-run` — fetch source rows, skip writes to target

### DDL generation

```
\ddl SRCLIB.ORDERS                          -- print DDL (ibmi dialect)
\ddl SRCLIB.ORDERS --target-table public.orders --exec   -- run on current connection
\ddl SRCLIB.ORDERS --exec --drop-if-exists
```

### History

| Command | Description |
|---|---|
| `\history` | Show last 20 commands |
| `\hsearch <keyword>` | Search history |
| `↑ ↓` arrow keys | Navigate history inline |

History is persisted to `~/.strsql-node/history.json`.

---

## CLI subcommands

```
strsql [session]        Interactive REPL (default)
strsql run <sql>        Execute a single SQL statement
strsql import <file>    Import a file into a database
strsql pipe             Transfer rows between two databases
strsql profiles list    List saved profiles
strsql profiles add     Add/update a profile
strsql profiles remove  Delete a profile
strsql drivers          List supported database types
```

### `strsql run` options

| Option | Description |
|---|---|
| `-p, --profile <n>` | Source connection profile |
| `-H, --host / -u, --user / --password / -s, --schema` | Inline connection params |
| `-l, --library-list <libs>` | IBM i library list (comma-separated) |
| `--type <type>` | DB type (default: `ibmi`) |
| `-f, --format <fmt>` | `table` \| `csv` \| `json` \| `insert` \| `merge` |
| `-o, --out <file>` | Export to file (`.csv` / `.json` / `.sql` / `.insert.sql` / `.merge.sql`) |
| `--table <T>` | Target table for SQL export |
| `--keys <cols>` | Key columns for merge format |
| `--batch <N>` | Rows per INSERT (default: 1) |
| `--dialect <type>` | SQL dialect for export (default: `ibmi`) |

### `strsql import` options

| Option | Description |
|---|---|
| `-p, --profile <n>` | Connection profile |
| `-l, --library-list <libs>` | IBM i library list (comma-separated) |
| `-t, --table <T>` | Target table (required for CSV/JSON) |
| `-m, --mode <mode>` | `abort` \| `skip` (default: `abort`) |
| `-b, --batch <N>` | Rows per commit (default: 100) |
| `--dry-run` | Validate without writing |
| `--map <mapping>` | `srcCol=DEST,src2=DEST2` |
| `--delimiter <char>` | CSV delimiter (default: `,`) |

### `strsql pipe` options

| Option | Description |
|---|---|
| `-p, --profile <n>` | Source profile |
| `--source-table <T>` | Source table |
| `-l, --library-list <libs>` | Source IBM i library list (comma-separated) |
| `--sql <SELECT>` | Override source SELECT |
| `--where <condition>` | WHERE clause on source |
| `--target-profile <n>` | Target profile |
| `--target-host / --target-user / --target-password / --target-schema` | Inline target params |
| `--target-library-list <libs>` | Target IBM i library list (comma-separated) |
| `--target-table <T>` | Target table (default: same as source) |
| `--mode insert\|merge` | Transfer mode |
| `--keys <cols>` | Key columns for merge |
| `-b, --batch <N>` | Rows per page (default: 500) |
| `--map <mapping>` | Column mapping |
| `--truncate` | DELETE FROM target first |
| `--ddl` | CREATE TABLE on target |
| `--drop-if-exists` | DROP before CREATE |
| `--mode-on-error skip` | Skip errors instead of abort |
| `--dry-run` | Fetch only, no writes |

---

## Programmatic API

### ODBCConnection

```js
const { ODBCConnection } = require('strsql-node');

// IBM i (backward-compatible: IBMiConnection still works)
const conn = new ODBCConnection({
  type: 'ibmi',               // optional, default
  host: '10.0.0.1',
  username: 'MYUSER',
  password: 'secret',
  defaultSchema: 'MYLIB',
  namingMode: 'sql',          // 'sql' | 'system'
  libraryList: 'MYLIB,QGPL,QUSRSYS',  // IBM i library list (string or array)
});

// PostgreSQL
const pg = new ODBCConnection({
  type: 'postgresql',
  host: 'pg.local',
  port: 5432,
  username: 'admin',
  password: 'secret',
  database: 'sales',
  defaultSchema: 'public',
  sslMode: 'require',
});

// SQL Server
const ss = new ODBCConnection({
  type: 'sqlserver',
  host: 'ss.local',
  username: 'sa',
  password: 'secret',
  database: 'MyDB',
  instanceName: 'DEV',       // optional named instance
});

// Oracle
const ora = new ODBCConnection({
  type: 'oracle',
  host: 'ora.local',
  port: 1521,
  username: 'SYS',
  password: 'secret',
  serviceName: 'ORCL',
});

// Raw connection string (any driver)
const raw = new ODBCConnection({
  connectionString: 'DRIVER={My Driver};SERVER=...;',
});

await conn.connect();

const result  = await conn.query('SELECT * FROM MYLIB.ORDERS FETCH FIRST 5 ROWS ONLY');
const written = await conn.execute('UPDATE MYLIB.ORDERS SET ACTIVE=1 WHERE ID=?', [42]);
const tables  = await conn.listTables('MYLIB');
const cols    = await conn.describeTable('ORDERS', 'MYLIB');

console.log(conn.dbType);   // 'ibmi'
console.log(conn.dbLabel);  // 'IBM i (AS/400)'

await conn.disconnect();
```

#### `ODBCConnection` API

| Method | Returns | Description |
|---|---|---|
| `connect()` | `Promise<void>` | Open ODBC connection |
| `disconnect()` | `Promise<void>` | Close connection |
| `query(sql, [params])` | `Promise<Result>` | Execute SELECT |
| `execute(sql, [params])` | `Promise<ExecResult>` | Execute DML/DDL |
| `listTables(schema)` | `Promise<Result>` | List tables (native catalog per DB) |
| `describeTable(table, [schema])` | `Promise<Result>` | Describe columns (native catalog per DB) |
| `paginateSQL(sql, offset, limit)` | `string` | Wrap SQL with DB-specific pagination |
| `setLibraryList(libs)` | `Promise<void>` | Set IBM i library list at runtime (string or array) |
| `quoteIdentifier(name)` | `string` | Quote identifier for this DB |
| `isConnected()` | `boolean` | Connection status |
| `dbType` | `string` | DB type key e.g. `'ibmi'` |
| `dbLabel` | `string` | Human label e.g. `'IBM i (AS/400)'` |

#### `Result` object

```js
{
  columns: [{ name, dataType, columnSize, nullable }],
  rows:     [{ COL1: val, COL2: val, ... }],
  rowCount: Number,
  elapsed:  Number,    // ms
  statement: String,
}
```

### Importer

```js
const { Importer, ERROR_MODE } = require('strsql-node');

const importer = new Importer(conn, {
  table:      'MYLIB.ORDERS',
  errorMode:  ERROR_MODE.SKIP,   // 'abort' | 'skip' | 'confirm'
  batchSize:  200,
  dryRun:     false,
  mapping:    { order_no: 'ORDNUM', customer: 'CUSNAM', note: null }, // null = exclude
  delimiter:  ',',
  onProgress: (done, total) => process.stdout.write(`\r${done}/${total}`),
});

// Auto-detects format from extension: .csv .tsv .json .sql .insert.sql .merge.sql
const result = await importer.importFile('/path/to/orders.csv');
console.log(result.inserted, result.skipped, result.errors);
```

### Pipe

```js
const { Pipe } = require('strsql-node');

const pipe = new Pipe(srcConn, tgtConn, {
  sourceTable:  'SRCLIB.ORDERS',
  targetTable:  'public.orders',
  mode:         'merge',          // 'insert' | 'merge'
  keys:         ['ORDNUM'],
  batchSize:    1000,
  truncate:     false,
  generateDDL:  true,             // CREATE TABLE on target from source schema
  dropIfExists: false,
  tgtDialect:   'postgresql',     // explicit target dialect (auto-detected if omitted)
  mapping:      { NOTE: null },   // exclude NOTE column
  where:        'ACTIVE = 1',
  dryRun:       false,
  errorMode:    'skip',
  onProgress:   (written, total, page) => {},
});

const result = await pipe.run();
console.log(result.totalFetched, result.totalWritten, result.pages, result.elapsed);
```

### Dialect

The `Dialect` class handles all SQL syntax differences between databases. It is used internally by `toInsert`, `toMerge`, `generateDDL`, and `Pipe`, but is also available for programmatic use.

```js
const { Dialect } = require('strsql-node');

const d = Dialect.for('postgresql');

// Identifier quoting
d.quoteId('order date')           // → "order date"
d.quoteId('AMOUNT')               // → "AMOUNT"

// Value literals
d.literal(null)                   // → NULL
d.literal(true)                   // → TRUE
d.literal(new Date('2024-01-15')) // → '2024-01-15'::date
d.literal(new Date('2024-01-15T14:30:00Z')) // → '2024-01-15 14:30:00.000'::timestamp
d.literal("O'Brien")              // → 'O''Brien'

// INSERT with ? placeholders (ODBC binding)
const { sql, params } = d.insert('public.orders', ['ORDNUM','CUSNAM'], ['00001','ACME']);
// sql    → INSERT INTO public.orders ("ORDNUM", "CUSNAM") VALUES (?, ?)
// params → ['00001', 'ACME']

// Upsert (inline literals, dialect-specific syntax)
d.upsert('public.orders', ['ORDNUM','CUSNAM','AMOUNT'], ['ORDNUM'], ['00001','ACME',100])
// → INSERT INTO public.orders ("ORDNUM","CUSNAM","AMOUNT")
//   VALUES ('00001','ACME',100)
//   ON CONFLICT ("ORDNUM") DO UPDATE SET "CUSNAM"=EXCLUDED."CUSNAM", "AMOUNT"=EXCLUDED."AMOUNT"

// DDL type mapping
d.mapType('CHARACTER', 5, 0)   // → CHAR(5)
d.mapType('BOOLEAN', 0, 0)     // → BOOLEAN
d.mapType('CLOB', 0, 0)        // → TEXT

// Available types
Dialect.list()   // → ['ibmi','db2','sqlserver','postgresql','mysql','oracle','sqlite']
```

### Formatters

```js
const { toInsert, toMerge, toCSV, toJSON, exportToFile, generateDDL } = require('strsql-node');

// Export to string
toCSV(result)
toJSON(result)
toInsert(result, { table: 'MYLIB.ORDERS', dialect: 'ibmi',  batch: 100 })
toInsert(result, { table: 'public.orders', dialect: 'postgresql' })
toMerge(result,  { table: 'MYLIB.ORDERS', keys: ['ORDNUM'], dialect: 'ibmi' })
toMerge(result,  { table: 'public.orders', keys: ['ORDNUM'], dialect: 'postgresql' })

// Export to file (format from extension)
exportToFile(result, 'orders.csv')
exportToFile(result, 'orders.json')
exportToFile(result, 'orders.sql',        { table: 'MYLIB.ORDERS', dialect: 'ibmi' })
exportToFile(result, 'orders.insert.sql', { table: 'MYLIB.ORDERS', dialect: 'sqlserver', batch: 50 })
exportToFile(result, 'orders.merge.sql',  { table: 'MYLIB.ORDERS', keys: ['ORDNUM'], dialect: 'postgresql' })

// DDL from describeTable() result
generateDDL('public.orders', conn.describeTable('ORDERS','MYLIB'), 'postgresql')
```

---

## Dialect reference

### Upsert strategy per database

| DB type | Strategy | SQL generated |
|---|---|---|
| `ibmi` | `merge-standard` | `MERGE INTO T USING (VALUES ...) AS S ON (...) WHEN MATCHED ... WHEN NOT MATCHED ...` |
| `db2` | `merge-standard` | Same as IBM i |
| `sqlserver` | `merge-standard` | Same as IBM i with `[bracket]` quoting |
| `postgresql` | `on-conflict` | `INSERT ... ON CONFLICT (keys) DO UPDATE SET ...` |
| `mysql` | `on-duplicate-key` | `INSERT ... ON DUPLICATE KEY UPDATE col=VALUES(col)` |
| `oracle` | `merge-oracle` | `MERGE INTO T USING (SELECT ... FROM DUAL) S ON (...)` |
| `sqlite` | `on-conflict` | `INSERT ... ON CONFLICT (keys) DO UPDATE SET ...` |

### Timestamp literals per database

| DB type | Date only | Date + time |
|---|---|---|
| `ibmi` | `'2024-01-15'` | `TIMESTAMP '2024-01-15 14:30:00.000'` |
| `sqlserver` | `'2024-01-15'` | `CONVERT(datetime2, '2024-01-15 14:30:00.000', 121)` |
| `postgresql` | `'2024-01-15'::date` | `'2024-01-15 14:30:00.000'::timestamp` |
| `mysql` | `'2024-01-15'` | `'2024-01-15 14:30:00.000'` |
| `oracle` | `DATE '2024-01-15'` | `TIMESTAMP '2024-01-15 14:30:00.000'` |
| `sqlite` | `'2024-01-15'` | `'2024-01-15 14:30:00.000'` |

### Key DDL type mappings

| Source type | ibmi | sqlserver | postgresql | mysql | oracle | sqlite |
|---|---|---|---|---|---|---|
| `CHAR(n)` | `CHAR(n)` | `NCHAR(n)` | `CHAR(n)` | `CHAR(n)` | `CHAR(n)` | `TEXT` |
| `VARCHAR(n)` | `VARCHAR(n)` | `NVARCHAR(n)` | `VARCHAR(n)` | `VARCHAR(n)` | `VARCHAR2(n)` | `TEXT` |
| `CLOB` | `CLOB` | `NVARCHAR(MAX)` | `TEXT` | `LONGTEXT` | `CLOB` | `TEXT` |
| `DECIMAL(p,s)` | `DECIMAL(p,s)` | `DECIMAL(p,s)` | `NUMERIC(p,s)` | `DECIMAL(p,s)` | `NUMBER(p,s)` | `NUMERIC` |
| `BOOLEAN` | `BOOLEAN` | `BIT` | `BOOLEAN` | `TINYINT(1)` | `NUMBER(1)` | `INTEGER` |
| `TIMESTAMP` | `TIMESTAMP` | `DATETIME2` | `TIMESTAMP` | `DATETIME` | `TIMESTAMP` | `TEXT` |
| `BLOB` | `BLOB` | `VARBINARY(MAX)` | `BYTEA` | `LONGBLOB` | `BLOB` | `BLOB` |
| `INTEGER` | `INTEGER` | `INT` | `INTEGER` | `INT` | `NUMBER(18)` | `INTEGER` |

---

## File layout

```
strsql-node/
├── bin/
│   └── strsql.js          CLI entry point (Commander subcommands)
├── src/
│   ├── lib/
│   │   ├── index.js        Public API exports
│   │   ├── connection.js   ODBCConnection (database-agnostic)
│   │   ├── drivers.js      ODBC driver registry (conn strings, catalog SQL, pagination)
│   │   ├── dialect.js      SQL generation rules per DB (literals, upsert, DDL types)
│   │   ├── profiles.js     Named connection profile manager (~/.strsql-node/)
│   │   ├── history.js      Command history with persistence
│   │   ├── formatter.js    Output formatters: table, CSV, JSON, INSERT, MERGE/upsert
│   │   ├── importer.js     File import engine (CSV, JSON, SQL)
│   │   └── pipe.js         DB-to-DB streaming pipe + DDL generator
│   └── cli/
│       ├── session.js      Interactive REPL (\commands, readline, progress)
│       └── progress.js     Terminal progress bar
├── test/
│   └── basic.js            Usage examples and smoke tests
├── .env.example
└── README.md
```

---

## Security notes

- Passwords saved in profiles are stored in **plain text** in `~/.strsql-node/profiles.json`. Use `STRSQL_PASSWORD` environment variable for production environments.
- The `odbc` package uses native bindings — keep it updated and verify driver provenance.
- SQL import (`\import file.sql`) executes statements as-is — only import files from trusted sources.

---

## License

MIT
