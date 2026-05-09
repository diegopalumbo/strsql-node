# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

[Unreleased]: https://github.com/diegopalumbo/strsql-node/compare/v1.0.11...HEAD
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
