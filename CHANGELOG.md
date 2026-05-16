# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [1.0.27] - 2026-05-16

### Added
- `strsql doctor` — diagnoses the local ODBC environment: Node.js version, `odbc` npm module, ODBC manager (unixODBC / Windows built-in), list of installed ODBC drivers, and per-driver-type check against the expected driver names. Supports `--type <type>` to narrow the check to a specific database and `--json` for machine-readable output.
- `strsql setup` — interactive guided wizard that selects the database type, summarises the detected environment, prints the correct install commands for the detected package manager (Homebrew / apt / dnf / yum / winget), optionally runs them after explicit confirmation, and creates and tests a connection profile in one flow. Supports `--type`, `--profile`, and `--install` flags.
- `src/lib/driverRegistry.js` — central knowledge base covering all 7 supported database types (ibmi, sqlserver, postgresql, mysql, oracle, db2, sqlite): known ODBC driver name variants, per-platform install commands, official download URLs, and helpers for manager detection, driver listing, and package-manager detection.
- Light `postinstall` hint in `package.json` pointing new users to `strsql doctor` and `strsql setup`.

---

## [1.0.26] - 2026-05-16

### Added
- Added command-specific interactive help via `\help <command>` for connection, profile, import/export, pipe, DDL, run, shell, migrations, and seeds commands.
- Added focused help output for migration and seed subcommands such as `\help migrations run` and `\help seeds create`.

### Changed
- Invalid or incomplete interactive meta-command usage now routes to the relevant command help instead of printing one-off usage strings.
- The main `\help` output now points users to `\help pipe` for the full DB2-to-DB2 pipe option list.

---

## [1.0.25] - 2026-05-15

### Changed
- Replaced the optional native IBM i dependency from `idb-connector` to `idb-pconnector`.
- Updated the IBM i `idb` adapter implementation to use the promise-based `idb-pconnector` API (`Connection`, async statements, async close/disconnect).

### Fixed
- IBM i `idb` DML execution now commits through the promise connector API and retries eligible `INSERT`/`UPDATE`/`DELETE`/`MERGE` statements with `WITH NC` when Db2 for i reports non-journaled table commitment-control errors (`SQLSTATE=55019` / `SQLCODE=-7008`).

---

## [1.0.24] - 2026-05-15

> **Deprecated:** this release is deprecated. Use `1.0.25` or later.

### Fixed
- IBM i `idb` DML persistence now uses an explicit `COMMIT` after statements because `idb-connector` connections can run with auto-commit disabled by default.
- Session banner copy no longer says "via ODBC" when the active adapter can also be native IBM i `idb`.

---

## [1.0.23] - 2026-05-15

> **Deprecated:** this release is deprecated. Use `1.0.25` or later.

### Fixed
- IBM i `idb` auto-commit setup was moved to run after the native connection is opened, matching the connector's expected lifecycle.

---

## [1.0.22] - 2026-05-15

> **Deprecated:** this release is deprecated. Use `1.0.25` or later.

### Fixed
- IBM i `idb` DML execution now captures affected row counts and attempts auto-commit for `INSERT`/`UPDATE`/`DELETE`, fixing cases where `DELETE` statements were executed but not persisted.

---

## [1.0.21] - 2026-05-12

> **Deprecated:** this release is deprecated. Use `1.0.25` or later.

### Fixed
- Pager output on IBM i/PASE (`aix`) is now rendered without ANSI color sequences to prevent unreadable characters when `more` is used as fallback pager.
- Added `STRSQL_NO_COLOR=1` / `NO_COLOR` support to strip ANSI styles from output, and exposed the no-color state in `\pagerstatus`.
- On IBM i/PASE and when the detected pager is `more`, chalk colors are disabled automatically before pager rendering.

---

## [1.0.20] - 2026-05-10

> **Deprecated:** this release is deprecated. Use `1.0.25` or later.

### Changed
- Internal version bump only; no user-facing changes.

---

## [1.0.19] - 2026-05-10

> **Deprecated:** this release is deprecated. Use `1.0.25` or later.

### Changed
- Updated migration and seed documentation to use the grouped CLI command names: `strsql migrations run`, `strsql migrations create`, `strsql seeds run`, and `strsql seeds create`.
- Expanded the README examples for interactive `\migrations` and `\seeds` commands, including tracking table options, `--from-table`, `--format upsert`, and `--keys` guidance.

---

## [1.0.18] - 2026-05-09

### Added
- **Migrations & Seeds** — integrated the `ibmi-db-migrations` library directly into `strsql-node`. Four new CLI subcommands are now available:
  - **`strsql migrate <path> [up|down]`** — apply or roll back versioned SQL migrations. Migrations are stored as `.up.sql` / `.down.sql` file pairs; a `MIGRATION_LOG` table (configurable via `--migration-table`) tracks which files have been applied. On `down` the most recently applied migration is rolled back.
  - **`strsql seed <path> [up|down]`** — apply or roll back data seeds using the same file-pair convention. Applied seeds are tracked in a `SEED_LOG` table (configurable via `--seed-table`).
  - **`strsql migration:create <path> <name>`** — scaffold a timestamped migration file pair. Pass `--from-table SCHEMA.TABLE` to have the `.up.sql` pre-populated with the table's `CREATE TABLE` DDL (generated from the live database) and the `.down.sql` pre-populated with the corresponding `DROP TABLE`.
  - **`strsql seed:create <path> <name>`** — scaffold a timestamped seed file pair. Pass `--from-table TABLE` (or `-q <sql>` with `--table`) to pre-populate the `.up.sql` with `INSERT` or `MERGE/UPSERT` statements generated from live data; primary keys are auto-detected when `--format upsert` is used with `--from-table`. An interactive prompt offers to generate a `DELETE FROM` rollback in the `.down.sql`.
- **Programmatic API** — `runMigrations`, `runSeeds`, `createMigration`, and `createSeed` are now exported from `strsql-node`'s public API (`src/lib/index.js`).
- **`src/lib/migrations/` module** — new internal module containing `migrate.js` (low-level migration helpers), `migrationRunner.js`, `seedRunner.js`, and `create.js`.
- All migration/seed commands honour the full connection option set (`--profile`, `--host`, `--user`, `--password`, `--schema`, `--library-list`, `--adapter`) consistent with other `strsql` subcommands.
- **Interactive session — `\migrations` and `\seeds`** — the same migration and seed operations are now available inside an active `strsql` session without leaving the REPL:
  - **`\migrations run <path> [up|down] [--migration-table <name>]`** — apply or roll back migrations against the currently connected database.
  - **`\migrations create <path> <name> [--from-table [SCHEMA.]TABLE]`** — scaffold a timestamped migration file pair; `--from-table` generates DDL from the live connection.
  - **`\seeds run <path> [up|down] [--seed-table <name>]`** — apply or roll back seeds.
  - **`\seeds create <path> <name> [-q <sql>] [--from-table TABLE] [--table T] [--format insert|upsert] [--keys cols] [--batch N]`** — scaffold a seed file pair, optionally pre-populated from live data.
- **`\saveprofile` / profiles** — two new options: `--migration-table <name>` and `--seed-table <name>` can now be stored in a named profile and will be used as defaults by `\migrations run` and `\seeds run`.

---

## [1.0.17] - 2026-05-09

### Added
- **`\sysnames [on|off]`** — session toggle to display DDS system column names (≤10 chars) instead of SQL column names in SELECT results and `\describe` output. When enabled, `_applySystemNames()` queries `QSYS2.SYSCOLUMNS` for the first table found in the `FROM` clause and remaps headers accordingly. If the names are identical (already ≤10 chars) no change is applied. Silently ignored on non-IBM i databases.
- **`\describe --system-names`** — activates system name display for a single `\describe` call without toggling the session flag. Also inherits the session flag when `\sysnames on` is active.
- **`\status`** now shows `sysnames` (ON/OFF), `pager` (path or OFF/none), and `libl` (if set) as separate indented lines for easier reading.

---

## [1.0.15] - 2026-05-09

### Fixed
- **`\describe` — PK detection on IBM i**: the `🔑` indicator was never shown for any IBM i table. The previous implementation queried only `QSYS2.SYSKEYS WHERE INDEX_NAME = TABLE_NAME`, which applies solely to DDS physical files whose primary access path shares the file name. SQL tables with a `PRIMARY KEY` or `UNIQUE` constraint (the far more common case) were silently missed. Detection now uses three strategies in order:
  1. **ODBC `SQLPrimaryKeys` catalog function** (`conn.primaryKeys()`) — covers SQL `PRIMARY KEY` constraints natively via the IBM i Access ODBC driver
  2. **`QSYS2.SYSCSTCOL` + `QSYS2.SYSCST`** — SQL fallback covering both `PRIMARY KEY` and `UNIQUE` constraints, compatible with all IBM i OS versions; when both types exist the `PRIMARY KEY` constraint is preferred
  3. **`QSYS2.SYSKEYS`** — final fallback for DDS physical files whose key access path has `INDEX_NAME = TABLE_NAME`

---

## [1.0.14] - 2026-05-08

### Fixed
- **Pager — garbled output on IBM i PASE**: `less` on IBM i PASE does not interpret multi-byte UTF-8 sequences by default, causing box-drawing characters (╔, ║, ╠, …) to appear as raw byte sequences (`<E2><94><8C>`, …). Tables now automatically use ASCII borders (`+`, `-`, `|`) when running on PASE (`process.platform === 'aix'`). This can be overridden with `STRSQL_ASCII=0` if your terminal and `less` are configured for UTF-8.
- **Pager — `LESSCHARSET=utf-8`**: `less` is now spawned with `LESSCHARSET=utf-8` in its environment (unless already set) so that UTF-8 multi-byte characters are handled correctly on systems that support it.

---

## [1.0.13] - 2026-05-08

### Fixed
- **idb-connector — system naming mode**: `SQL_ATTR_DBC_SYS_NAMING` (`10004`) is now set to `SQL_TRUE` before connecting; unqualified table references (e.g. `SELECT * FROM PRODUCTSP`) are now resolved via the job's library list, the same behaviour as ODBC with `NAM=1`. Previously they were resolved only against the current schema, returning 0 rows when the table lived in a library list entry other than the current library.

---

## [1.0.12] - 2026-05-08

### Added
- **`\cmd <command>`** — run any shell command without leaving the session (`\cmd ls`, `\cmd cat file.sql`, etc.)
- **`\cmd cd <dir>`** — change the working directory of the strsql process; affects relative paths used by `\run`, `\import`, `\export`, and `\edit`; tilde (`~`) and relative paths are supported
- **`\edit [file.sql]`** (alias **`\e`**) — open the current SQL buffer (or last executed statement) in `$VISUAL` / `$EDITOR` (falls back to `vi`); the SQL is executed automatically when the editor is saved and closed; GUI editors are supported via their `--wait` flag (e.g. `VISUAL="code --wait"`)

### Fixed
- **TAB completion**: added `\cmd` to the list of meta-commands suggested on TAB; previously it was missing even though the command was fully implemented

---

## [1.0.11] — 2026-05-05

### Fixed
- Library list parsing: fixed an edge case where the library list string was not correctly split when it contained extra whitespace or was passed as an array internally

### Changed
- Updated README documentation for TAB completion

---

## [1.0.10] — 2026-05-05

### Changed
- Documentation improvements for TAB completion feature introduced in 1.0.9

---

## [1.0.9] — 2026-05-05

### Added
- **TAB completion** for table names and column names in the interactive session
  - Table completions are fetched from the catalog for the default schema and all libraries in the IBM i library list
  - Column completions are inferred from tables already present in the SQL text (e.g. `SELECT CUS<TAB> FROM CLIENTI`)
  - Qualifier-aware: `C.<TAB>` resolves columns for alias `C`
  - Completions are also triggered after keywords `FROM`, `JOIN`, `INTO`, `UPDATE`, `\describe`, `\ddl`, `\pipe`
  - Completion cache is refreshed on reconnect, disconnect, `\schema`, and `\libl`

---

## [1.0.8] — 2026-05-04

### Fixed
- `\describe` command: added fallback to `COLUMN_TEXT` when `COLUMN_HEADING` is not present in the catalog result (compatibility with certain IBM i ODBC driver versions)

---

## [1.0.7] — 2026-05-01

### Changed
- Internal publishing infrastructure updates

---

## [1.0.6] — 2026-05-01

### Changed
- Internal publishing infrastructure updates

---

## [1.0.5] — 2026-05-01

### Fixed
- npm trusted publishing metadata corrected

---

## [1.0.4] — 2026-05-01

### Changed
- Version bump following publishing pipeline fix

---

## [1.0.3] — 2026-04-29

### Added
- **Pager integration** — query results are now automatically piped through `less` (or `more` / `most` as fallbacks) when the output exceeds the terminal height, enabling both vertical and horizontal scrolling
- Pager is auto-detected at runtime; gracefully disabled in non-interactive environments (pipes, CI, etc.)
- `\nopager` — disable the pager for the current session
- `\pager` — re-enable the pager after `\nopager`
- `\pagerstatus` — show pager diagnostics (detected binary, arguments, interactive flag, env vars)
- Set `STRSQL_NO_PAGER=1` to disable the pager permanently via environment variable
- Column headings added to the `\describe` output

### Fixed
- Pager detection and activation in complex command scenarios and non-TTY environments

---

## [1.0.2] — 2026-04-20

### Added
- **`\run <file.sql>`** — execute all SQL statements from a file during an interactive session; statements are split on `;`, line comments (`--`) are stripped, and a summary with counts and elapsed time is printed at the end
- `--stop-on-error` flag for `\run` to halt execution at the first failing statement
- `libraryList` connection config parameter for IBM i: sets the user library list (`CHGLIBL`) immediately after connect
- **`setLibraryList(libs)`** method on `ODBCConnection` to change the IBM i library list at runtime (accepts string or array)
- **`primaryKeys(table, schema)`** method on `ODBCConnection`
- `\describe` now marks primary key columns with a 🔑 indicator
- `\libl` command to show or set the IBM i library list from within the session

### Fixed
- `\describe` and primary key retrieval for unqualified table names when an IBM i library list is active
- Trailing semicolon is now stripped before executing DDL statements to avoid syntax errors
- ODBC error details (`state` + `message` from `odbcErrors`) included in error output for easier diagnosis

---

## [1.0.1] — 2026-04-18

### Added
- Initial public release
- Interactive SQL session (`strsql` / `strsql session`) with multi-line input, `\commands`, persistent history, and arrow-key history navigation
- `strsql run <sql>` — non-interactive single-query mode with output formats: `table`, `csv`, `json`, `insert`, `merge`
- `strsql import <file>` — import `.csv`, `.json`, `.sql` files into any supported database
- `strsql pipe` — direct DB-to-DB row transfer with DDL generation, merge/upsert, column mapping, batch control, and dry-run mode
- `strsql profiles` — named connection profiles stored in `~/.strsql-node/profiles.json`
- Multi-database support via ODBC: **IBM i/AS400**, **SQL Server**, **PostgreSQL**, **MySQL/MariaDB**, **Oracle**, **DB2 LUW**, **SQLite**
- Dialect engine for per-database identifier quoting, value literals, upsert strategy, pagination syntax, and DDL type mapping
- `\connect`, `\disconnect`, `\profile`, `\status` — connection management
- `\schema`, `\libl`, `\tables`, `\describe` — schema and object inspection
- `\export` — export last SELECT result to `.csv`, `.json`, `.sql`, `.insert.sql`, `.merge.sql`
- `\import` — import files with column mapping, batch size, error mode (`abort` / `skip` / `confirm`), and dry-run
- `\pipe` — pipe data between databases from within the session
- `\ddl` — generate (and optionally execute) `CREATE TABLE` DDL from source schema
- `\history`, `\hsearch` — history browsing and search
- `\drivers` — list supported database types
- `\clear`, `\quit` / `\exit` — utility commands
- Programmatic API: `ODBCConnection`, `Importer`, `Pipe`, `Dialect`, formatters (`toInsert`, `toMerge`, `toCSV`, `toJSON`, `exportToFile`, `generateDDL`)

[Unreleased]: https://github.com/diegopalumbo/strsql-node/compare/v1.0.25...HEAD
[1.0.25]: https://github.com/diegopalumbo/strsql-node/compare/v1.0.24...v1.0.25
[1.0.24]: https://github.com/diegopalumbo/strsql-node/compare/v1.0.23...v1.0.24
[1.0.23]: https://github.com/diegopalumbo/strsql-node/compare/v1.0.22...v1.0.23
[1.0.22]: https://github.com/diegopalumbo/strsql-node/compare/v1.0.21...v1.0.22
[1.0.21]: https://github.com/diegopalumbo/strsql-node/compare/v1.0.20...v1.0.21
[1.0.20]: https://github.com/diegopalumbo/strsql-node/compare/v1.0.19...v1.0.20
[1.0.19]: https://github.com/diegopalumbo/strsql-node/compare/v1.0.18...v1.0.19
[1.0.18]: https://github.com/diegopalumbo/strsql-node/compare/v1.0.17...v1.0.18
[1.0.17]: https://github.com/diegopalumbo/strsql-node/compare/v1.0.16...v1.0.17
[1.0.16]: https://github.com/diegopalumbo/strsql-node/compare/v1.0.15...v1.0.16
[1.0.15]: https://github.com/diegopalumbo/strsql-node/compare/v1.0.14...v1.0.15
[1.0.14]: https://github.com/diegopalumbo/strsql-node/compare/v1.0.13...v1.0.14
[1.0.13]: https://github.com/diegopalumbo/strsql-node/compare/v1.0.12...v1.0.13
[1.0.12]: https://github.com/diegopalumbo/strsql-node/compare/v1.0.11...v1.0.12
[1.0.11]: https://github.com/diegopalumbo/strsql-node/compare/v1.0.10...v1.0.11
[1.0.10]: https://github.com/diegopalumbo/strsql-node/compare/v1.0.9...v1.0.10
[1.0.9]: https://github.com/diegopalumbo/strsql-node/compare/v1.0.8...v1.0.9
[1.0.8]: https://github.com/diegopalumbo/strsql-node/compare/v1.0.7...v1.0.8
[1.0.7]: https://github.com/diegopalumbo/strsql-node/compare/v1.0.6...v1.0.7
[1.0.6]: https://github.com/diegopalumbo/strsql-node/compare/v1.0.5...v1.0.6
[1.0.5]: https://github.com/diegopalumbo/strsql-node/compare/v1.0.4...v1.0.5
[1.0.4]: https://github.com/diegopalumbo/strsql-node/compare/v1.0.3...v1.0.4
[1.0.3]: https://github.com/diegopalumbo/strsql-node/compare/v1.0.2...v1.0.3
[1.0.2]: https://github.com/diegopalumbo/strsql-node/compare/v1.0.1...v1.0.2
[1.0.1]: https://github.com/diegopalumbo/strsql-node/releases/tag/v1.0.1
