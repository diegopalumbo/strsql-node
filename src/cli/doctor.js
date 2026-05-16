'use strict';

const os    = require('os');
const chalk = require('chalk');

const {
  DRIVER_REGISTRY,
  checkOdbcManager,
  listInstalledDrivers,
  findDriverForType,
} = require('../lib/driverRegistry');

// ─── helpers ─────────────────────────────────────────────────────────────────

function ok(msg)   { return `${chalk.green('✓')} ${msg}`; }
function fail(msg) { return `${chalk.red('✗')} ${msg}`; }
function warn(msg) { return `${chalk.yellow('!')} ${msg}`; }
function dim(msg)  { return chalk.dim(msg); }

function platformLabel() {
  const p = process.platform;
  const labels = { darwin: 'macOS', linux: 'Linux', win32: 'Windows' };
  return `${labels[p] || p} ${os.arch()}`;
}

function checkNodeVersion() {
  const major = parseInt(process.version.slice(1), 10);
  if (major < 16) {
    return { ok: false, label: `Node.js ${process.version} (minimum 16 recommended)` };
  }
  return { ok: true, label: `Node.js ${process.version}` };
}

function checkOdbcModule() {
  try {
    require('odbc');
    return { ok: true, label: 'npm odbc module loaded' };
  } catch (e) {
    return { ok: false, label: `npm odbc module: ${e.message}` };
  }
}

// ─── main ─────────────────────────────────────────────────────────────────────

/**
 * Run the doctor diagnosis.
 *
 * @param {object}  opts
 * @param {string}  [opts.type]   - narrow diagnosis to a specific DB type
 * @param {boolean} [opts.json]   - emit JSON output instead of pretty text
 */
function runDoctor(opts = {}) {
  const { type: filterType, json: jsonOutput } = opts;

  // ── 1. Platform ───────────────────────────────────────────────────────────
  const platform   = platformLabel();
  const nodeCheck  = checkNodeVersion();
  const odbcModule = checkOdbcModule();

  // ── 2. ODBC manager ───────────────────────────────────────────────────────
  const odbcMgr = checkOdbcManager();

  // ── 3. Installed drivers ──────────────────────────────────────────────────
  const installedDrivers = odbcMgr.found ? listInstalledDrivers() : null;

  // ── 4. Per-type check ─────────────────────────────────────────────────────
  const typesToCheck = filterType
    ? (DRIVER_REGISTRY[filterType] ? [filterType] : [])
    : Object.keys(DRIVER_REGISTRY);

  const driverChecks = typesToCheck.map(t => {
    const entry  = DRIVER_REGISTRY[t];
    const found  = findDriverForType(t, installedDrivers);
    return {
      type:  t,
      label: entry.label,
      found: !!found,
      matchedDriver: found || null,
      expectedDrivers: entry.driverNames,
    };
  });

  // ── 5. Next steps ─────────────────────────────────────────────────────────
  const issues = [];

  if (!nodeCheck.ok)   issues.push(`Upgrade Node.js to v16 or later.`);
  if (!odbcModule.ok)  issues.push(`Reinstall the odbc npm package: npm install odbc`);
  if (!odbcMgr.found) {
    if (process.platform === 'win32') {
      issues.push('ODBC manager not detected on Windows — this is unexpected. Check odbcad32.exe.');
    } else {
      issues.push('Install unixODBC: ' +
        (process.platform === 'darwin'
          ? 'brew install unixodbc'
          : 'sudo apt-get install unixodbc  (or dnf/yum equivalent)'));
    }
  }

  for (const dc of driverChecks) {
    if (!dc.found) {
      const entry = DRIVER_REGISTRY[dc.type];
      issues.push(`${dc.label} ODBC driver not found. See: ${entry.officialUrl}`);
    }
  }

  // ── 6. Output ─────────────────────────────────────────────────────────────
  if (jsonOutput) {
    console.log(JSON.stringify({
      platform,
      node: { version: process.version, ok: nodeCheck.ok },
      odbcModule: odbcModule.ok,
      odbcManager: { found: odbcMgr.found, version: odbcMgr.version },
      installedDrivers: installedDrivers || [],
      driverChecks,
      issues,
    }, null, 2));
    return;
  }

  console.log();
  console.log(chalk.bold('strsql doctor'));
  console.log(chalk.dim('─'.repeat(50)));

  // Platform
  console.log();
  console.log(chalk.bold('System'));
  console.log(`  ${dim('Platform:')}   ${platform}`);
  console.log(`  ${nodeCheck.ok ? ok(nodeCheck.label) : fail(nodeCheck.label)}`);

  // ODBC module
  console.log();
  console.log(chalk.bold('npm odbc module'));
  console.log(`  ${odbcModule.ok ? ok(odbcModule.label) : fail(odbcModule.label)}`);

  // ODBC manager
  console.log();
  console.log(chalk.bold('ODBC manager'));
  if (odbcMgr.found) {
    console.log(`  ${ok(`Found: ${odbcMgr.version}`)}`);
  } else {
    console.log(`  ${fail('Not found (unixODBC / ODBC manager missing)')}`);
  }

  // Installed drivers
  console.log();
  console.log(chalk.bold('Installed ODBC drivers'));
  if (!odbcMgr.found) {
    console.log(`  ${warn('Cannot list drivers — ODBC manager not installed')}`);
  } else if (!installedDrivers || installedDrivers.length === 0) {
    console.log(`  ${warn('No ODBC drivers detected')}`);
  } else {
    for (const d of installedDrivers) {
      console.log(`  ${chalk.dim('•')} ${d}`);
    }
  }

  // Per-type driver check
  if (driverChecks.length > 0) {
    console.log();
    console.log(chalk.bold(filterType ? `Driver check: ${DRIVER_REGISTRY[filterType].label}` : 'Driver check'));
    for (const dc of driverChecks) {
      if (dc.found) {
        console.log(`  ${ok(`${dc.label}: ${chalk.dim(dc.matchedDriver)}`)}`);
      } else {
        console.log(`  ${fail(`${dc.label}: not found`)}`);
        console.log(`      ${dim('Expected one of:')}`);
        for (const n of dc.expectedDrivers) {
          console.log(`        ${dim('• ' + n)}`);
        }
      }
    }
  }

  // Issues / next steps
  console.log();
  if (issues.length === 0) {
    console.log(chalk.green.bold('All checks passed.'));
  } else {
    console.log(chalk.bold('Next steps'));
    for (const issue of issues) {
      console.log(`  ${chalk.yellow('→')} ${issue}`);
    }
    console.log();
    console.log(dim('  Run `strsql setup` for guided installation.'));
  }

  console.log();
}

module.exports = { runDoctor };
