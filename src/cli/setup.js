'use strict';

const readline = require('readline');
const os       = require('os');
const chalk    = require('chalk');
const { execSync, spawnSync } = require('child_process');

const {
  DRIVER_REGISTRY,
  detectPackageManager,
  checkOdbcManager,
  listInstalledDrivers,
  findDriverForType,
} = require('../lib/driverRegistry');

// ─── helpers ──────────────────────────────────────────────────────────────────

function prompt(rl, question) {
  return new Promise(resolve => rl.question(question, resolve));
}

async function promptYN(rl, question, defaultYes = false) {
  const hint = defaultYes ? '[Y/n]' : '[y/N]';
  const answer = (await prompt(rl, `${question} ${hint} `)).trim().toLowerCase();
  if (answer === '') return defaultYes;
  return answer === 'y' || answer === 'yes';
}

async function promptChoice(rl, question, choices) {
  console.log();
  console.log(chalk.bold(question));
  choices.forEach((c, i) => {
    console.log(`  ${chalk.cyan((i + 1).toString().padStart(2))}. ${c.label}`);
  });
  console.log();
  while (true) {
    const answer = (await prompt(rl, `  Enter number [1-${choices.length}]: `)).trim();
    const idx = parseInt(answer, 10) - 1;
    if (idx >= 0 && idx < choices.length) return choices[idx];
    console.log(chalk.yellow(`  Please enter a number between 1 and ${choices.length}.`));
  }
}

async function promptText(rl, question, defaultVal) {
  const hint = defaultVal ? chalk.dim(` (${defaultVal})`) : '';
  const answer = (await prompt(rl, `  ${question}${hint}: `)).trim();
  return answer || defaultVal || '';
}

function platformLabel() {
  const p = process.platform;
  const labels = { darwin: 'macOS', linux: 'Linux', win32: 'Windows' };
  return `${labels[p] || p} ${os.arch()}`;
}

function printInstallCommands(commands) {
  console.log();
  console.log(chalk.bold('  Install commands:'));
  for (const cmd of commands) {
    console.log(`  ${chalk.cyan('$')} ${cmd}`);
  }
}

function runCommands(commands) {
  for (const cmd of commands) {
    if (cmd.startsWith('#')) continue;   // skip comments
    console.log(chalk.dim(`  Running: ${cmd}`));
    const result = spawnSync(cmd, { shell: true, stdio: 'inherit' });
    if (result.status !== 0) {
      console.log(chalk.red(`  Command failed (exit code ${result.status}): ${cmd}`));
      return false;
    }
  }
  return true;
}

function resolveInstallCommands(entry, platform, pkgMgr) {
  const platEntry = entry.platforms[platform];
  if (!platEntry) return null;

  // Try preferred package manager first, then fallback to 'manual'
  const install = platEntry.install;
  if (install[pkgMgr]) return install[pkgMgr];
  if (install.manual)  return install.manual;

  // Return first available
  const first = Object.values(install)[0];
  return first || null;
}

// ─── main ─────────────────────────────────────────────────────────────────────

/**
 * Run the guided setup wizard.
 *
 * @param {object}  opts
 * @param {string}  [opts.type]     - pre-select DB type (skip the menu)
 * @param {string}  [opts.profile]  - profile name to create at the end
 * @param {boolean} [opts.install]  - if true, default 'run commands' to yes
 */
async function runSetup(opts = {}) {
  const rl = readline.createInterface({
    input:  process.stdin,
    output: process.stdout,
  });

  // Ensure rl doesn't keep process alive after we close it
  rl.on('close', () => {});

  try {
    await _runSetup(rl, opts);
  } finally {
    rl.close();
  }
}

async function _runSetup(rl, opts) {
  console.log();
  console.log(chalk.bold.green('strsql setup'));
  console.log(chalk.dim('Guided ODBC driver and profile setup'));
  console.log(chalk.dim('─'.repeat(50)));

  const platform = process.platform;
  const pkgMgr   = detectPackageManager();
  const odbcMgr  = checkOdbcManager();

  // ── Step 1: choose DB type ────────────────────────────────────────────────
  let dbType = opts.type;

  if (dbType && !DRIVER_REGISTRY[dbType]) {
    console.log(chalk.red(`  Unknown type "${dbType}". Valid types: ${Object.keys(DRIVER_REGISTRY).join(', ')}`));
    dbType = null;
  }

  if (!dbType) {
    const typeChoices = Object.entries(DRIVER_REGISTRY).map(([type, entry]) => ({
      type,
      label: entry.label,
    }));

    const chosen = await promptChoice(rl, 'Which database do you want to connect to?', typeChoices);
    dbType = chosen.type;
  }

  const entry = DRIVER_REGISTRY[dbType];

  // ── Step 2: environment summary ───────────────────────────────────────────
  console.log();
  console.log(chalk.bold('Environment'));
  console.log(`  Platform:      ${platformLabel()}`);
  console.log(`  Node.js:       ${process.version}`);
  console.log(`  ODBC manager:  ${odbcMgr.found ? chalk.green(odbcMgr.version) : chalk.red('not found')}`);

  const installedDrivers = odbcMgr.found ? listInstalledDrivers() : null;
  const matchedDriver    = findDriverForType(dbType, installedDrivers);

  if (matchedDriver) {
    console.log(`  ODBC driver:   ${chalk.green(matchedDriver)}`);
  } else {
    console.log(`  ODBC driver:   ${chalk.red('not found')}`);
    console.log(`  Expected:      ${entry.driverNames[0]}`);
  }

  // ── Step 3: install guide ─────────────────────────────────────────────────
  const platEntry = entry.platforms[platform];

  if (!matchedDriver) {
    console.log();
    console.log(chalk.bold('Recommended setup steps'));

    if (!odbcMgr.found && platform !== 'win32') {
      const mgrs = platEntry ? platEntry.managers : ['unixODBC'];
      console.log(`  1. Install ODBC manager: ${mgrs.join(', ')}`);
      console.log(`  2. Install ${entry.label} ODBC driver`);
      console.log(`  3. Create a strsql profile`);
    } else {
      console.log(`  1. Install ${entry.label} ODBC driver`);
      console.log(`  2. Create a strsql profile`);
    }

    if (platEntry && platEntry.notes) {
      console.log();
      console.log(chalk.dim(`  Note: ${platEntry.notes}`));
    }

    const installCmds = platEntry ? resolveInstallCommands(entry, platform, pkgMgr) : null;

    if (installCmds) {
      console.log();
      console.log(chalk.bold(`  Detected package manager: ${chalk.cyan(pkgMgr)}`));
      const showCmds = await promptYN(rl, 'Show install commands?', true);
      if (showCmds) {
        printInstallCommands(installCmds);
        console.log();
        console.log(chalk.dim(`  Official driver page: ${entry.officialUrl}`));

        if (pkgMgr !== 'manual') {
          const runCmds = await promptYN(
            rl,
            opts.install
              ? 'Run install commands now?'
              : 'Run install commands now? (use --install to default to yes)',
            opts.install || false
          );
          if (runCmds) {
            console.log();
            console.log(chalk.bold('Running install commands...'));
            const success = runCommands(installCmds);
            if (!success) {
              console.log(chalk.yellow('  Some commands failed. Fix the errors above and re-run.'));
              console.log(chalk.dim('  You can re-run setup with: strsql setup --type ' + dbType));
            } else {
              console.log(chalk.green('  Installation complete.'));
            }
          }
        } else {
          console.log();
          console.log(chalk.yellow('  No supported package manager found. Copy and run the commands above manually.'));
        }
      }
    } else {
      console.log();
      console.log(`  Official driver page: ${chalk.cyan(entry.officialUrl)}`);
    }
  } else {
    console.log();
    console.log(chalk.green('  ODBC driver already installed. Skipping driver installation.'));
  }

  // ── Step 4: create a profile ──────────────────────────────────────────────
  console.log();
  const createProfile = await promptYN(rl, 'Create a strsql connection profile now?', true);

  if (createProfile) {
    await _createProfileInteractive(rl, dbType, opts.profile);
  } else {
    console.log();
    console.log(chalk.dim('  You can create a profile later with:'));
    console.log(chalk.dim(`  strsql profiles add <name> --type ${dbType} --host <host>`));
  }

  console.log();
  console.log(chalk.green.bold('Done.'));
  console.log(chalk.dim('  Run `strsql doctor` to verify your setup.'));
  console.log();
}

async function _createProfileInteractive(rl, dbType, presetName) {
  const { ProfileManager } = require('../lib/profiles');
  const profiles = new ProfileManager();

  console.log();
  console.log(chalk.bold('Create connection profile'));

  const name = await promptText(rl, 'Profile name', presetName || dbType);

  if (profiles.exists(name)) {
    const overwrite = await promptYN(rl, `  Profile "${name}" already exists. Overwrite?`, false);
    if (!overwrite) {
      console.log(chalk.yellow('  Skipped profile creation.'));
      return;
    }
  }

  let host, database, username, password, schema, adapter;

  if (dbType === 'sqlite') {
    host = await promptText(rl, 'Database file path', '/path/to/db.sqlite');
  } else if (dbType === 'ibmi') {
    const isIbmiPase = process.platform === 'os400' ||
      process.platform === 'aix' ||
      (process.platform === 'linux' && process.arch === 'ppc64');

    adapter = 'odbc';

    if (isIbmiPase) {
      const adapterChoice = await promptChoice(rl, 'Which adapter do you want to use?', [
        { type: 'odbc', label: 'odbc  — IBM i Access ODBC Driver' },
        { type: 'idb',  label: 'idb   — idb-pconnector native driver (recommended on IBM i / PASE)' },
      ]);
      adapter = adapterChoice.type;
    } else {
      console.log(chalk.dim('  Adapter: odbc (IBM i Access ODBC Driver)'));
    }

    if (adapter === 'idb') {
      console.log(chalk.dim('  idb-pconnector connects locally; host is usually *LOCAL.'));
    }

    host     = await promptText(rl, 'IBM i hostname or IP', adapter === 'idb' ? '*LOCAL' : '');
    username = await promptText(rl, 'Username');
    password = await promptText(rl, 'Password (stored in plain text)');
    schema   = await promptText(rl, 'Default schema/library (optional)');
  } else {
    host     = await promptText(rl, 'Hostname or IP');
    database = await promptText(rl, 'Database name');
    username = await promptText(rl, 'Username');
    password = await promptText(rl, 'Password (stored in plain text)');
    schema   = await promptText(rl, 'Default schema (optional)');
  }

  // Custom driver name? (not applicable for idb-pconnector)
  let driverName;
  if (adapter !== 'idb') {
    const entry    = DRIVER_REGISTRY[dbType];
    const defDriver = entry.driverNames[0];
    console.log();
    console.log(chalk.dim(`  Default ODBC driver name: ${defDriver}`));
    const customDriver = await promptYN(rl, 'Specify a custom driver name?', false);
    if (customDriver) {
      driverName = await promptText(rl, 'ODBC driver name', defDriver);
    }
  }

  // Test connection?
  const doTest = await promptYN(rl, 'Test connection now?', true);

  const config = {
    type:    dbType,
    adapter: adapter || 'odbc',
    host,
    ...(database   && { database }),
    ...(username   && { username }),
    ...(password   && { password }),
    ...(schema     && { defaultSchema: schema }),
    ...(driverName && { driverName }),
  };

  profiles.set(name, config);
  console.log(chalk.green(`  Profile "${name}" saved.`));

  if (doTest) {
    await _testConnection(config, name);
  }
}

async function _testConnection(config, profileName) {
  console.log();
  process.stdout.write(chalk.dim(`  Testing connection to ${config.host || config.database}...`));

  try {
    const { IBMiConnection } = require('../lib/connection');
    const conn = new IBMiConnection(config);
    await conn.connect();
    console.log(chalk.green(' connected!'));
    await conn.disconnect().catch(() => {});
    console.log(chalk.green(`  Profile "${profileName}" is ready to use.`));
    console.log(chalk.dim(`  Start a session with: strsql session --profile ${profileName}`));
  } catch (err) {
    console.log(chalk.red(' failed'));
    console.log(chalk.red(`  Error: ${err.message}`));
    console.log(chalk.dim('  Check hostname, credentials, and driver name.'));
    console.log(chalk.dim('  Run `strsql doctor --type ' + config.type + '` for more details.'));
  }
}

module.exports = { runSetup };
