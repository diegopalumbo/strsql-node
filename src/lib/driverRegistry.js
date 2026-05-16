'use strict';

/**
 * Registry of ODBC driver installation knowledge per database type and platform.
 *
 * Each entry:
 *   label         {string}    human-readable name
 *   driverNames   {string[]}  known ODBC driver name patterns (case-insensitive match)
 *   platforms     {object}    per-platform install info
 *     darwin / linux / win32
 *       managers  {string[]}  required ODBC manager package names
 *       install   {object}    package-manager → install command(s)
 *       notes     {string}    optional platform-specific note
 *   officialUrl   {string}    link to official driver download
 */
const DRIVER_REGISTRY = {

  // ── IBM i / AS400 ──────────────────────────────────────────────────────────
  ibmi: {
    label: 'IBM i (AS/400)',
    driverNames: [
      'IBM i Access ODBC Driver',
      'IBM i Access ODBC Driver (32-bit)',
      'iSeries Access ODBC Driver',
    ],
    platforms: {
      darwin: {
        managers: ['unixODBC'],
        install: {
          brew: [
            'brew install unixodbc',
            '# Then download IBM i Access Client Solutions for macOS',
            '# and install the ODBC driver from the ACS package.',
          ],
        },
        notes: 'IBM i Access Client Solutions (ACS) must be downloaded from IBM Fix Central or IBM support portal.',
      },
      linux: {
        managers: ['unixODBC'],
        install: {
          apt: [
            'sudo apt-get install -y unixodbc unixodbc-dev',
            '# Then install IBM i Access ODBC Driver for Linux from IBM Fix Central.',
          ],
          dnf: [
            'sudo dnf install -y unixODBC unixODBC-devel',
            '# Then install IBM i Access ODBC Driver for Linux from IBM Fix Central.',
          ],
          yum: [
            'sudo yum install -y unixODBC unixODBC-devel',
            '# Then install IBM i Access ODBC Driver for Linux from IBM Fix Central.',
          ],
        },
        notes: 'On IBM i PASE, use --adapter idb (idb-pconnector) to avoid ODBC entirely.',
      },
      win32: {
        managers: ['ODBC Data Source Administrator (built-in)'],
        install: {
          manual: [
            '# Download IBM i Access Client Solutions for Windows from IBM Fix Central.',
            '# Run the installer and select the ODBC driver component.',
          ],
        },
        notes: 'Requires 64-bit ACS installer for 64-bit Node.js. Admin privileges required.',
      },
    },
    officialUrl: 'https://www.ibm.com/support/pages/ibm-i-access-client-solutions',
  },

  // ── SQL Server ─────────────────────────────────────────────────────────────
  sqlserver: {
    label: 'SQL Server',
    driverNames: [
      'ODBC Driver 18 for SQL Server',
      'ODBC Driver 17 for SQL Server',
      'ODBC Driver 13 for SQL Server',
      'SQL Server Native Client 11.0',
      'SQL Server',
    ],
    platforms: {
      darwin: {
        managers: ['unixODBC'],
        install: {
          brew: [
            'brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release',
            'brew install msodbcsql18',
            'brew install mssql-tools18',
          ],
        },
      },
      linux: {
        managers: ['unixODBC'],
        install: {
          apt: [
            'curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -',
            'curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list',
            'sudo apt-get update',
            'sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18',
          ],
          dnf: [
            'curl https://packages.microsoft.com/config/rhel/9/prod.repo | sudo tee /etc/yum.repos.d/mssql-release.repo',
            'sudo ACCEPT_EULA=Y dnf install -y msodbcsql18',
          ],
        },
      },
      win32: {
        managers: ['ODBC Data Source Administrator (built-in)'],
        install: {
          winget: [
            'winget install Microsoft.ODBCDriverForSQLServer',
          ],
          manual: [
            '# Download from: https://docs.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server',
          ],
        },
      },
    },
    officialUrl: 'https://docs.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server',
  },

  // ── PostgreSQL ─────────────────────────────────────────────────────────────
  postgresql: {
    label: 'PostgreSQL',
    driverNames: [
      'PostgreSQL Unicode',
      'PostgreSQL Unicode(x64)',
      'PostgreSQL ANSI',
      'PostgreSQL',
      'psqlODBC',
    ],
    platforms: {
      darwin: {
        managers: ['unixODBC'],
        install: {
          brew: [
            'brew install unixodbc',
            'brew install psqlodbc',
          ],
        },
      },
      linux: {
        managers: ['unixODBC'],
        install: {
          apt: [
            'sudo apt-get install -y unixodbc odbc-postgresql',
          ],
          dnf: [
            'sudo dnf install -y unixODBC postgresql-odbc',
          ],
        },
      },
      win32: {
        managers: ['ODBC Data Source Administrator (built-in)'],
        install: {
          winget: [
            'winget install PostgreSQL.psqlODBC',
          ],
          manual: [
            '# Download psqlODBC from: https://odbc.postgresql.org/',
          ],
        },
      },
    },
    officialUrl: 'https://odbc.postgresql.org/',
  },

  // ── MySQL / MariaDB ────────────────────────────────────────────────────────
  mysql: {
    label: 'MySQL / MariaDB',
    driverNames: [
      'MySQL ODBC 8.0 Unicode Driver',
      'MySQL ODBC 8.0 ANSI Driver',
      'MySQL ODBC 9.0 Unicode Driver',
      'MySQL ODBC 9.0 ANSI Driver',
      'MySQL ODBC 5.3 Unicode Driver',
      'MariaDB ODBC 3.1 Driver',
      'MySQL',
    ],
    platforms: {
      darwin: {
        managers: ['unixODBC'],
        install: {
          brew: [
            'brew install unixodbc',
            'brew install mysql-connector-c',
            '# Then install MySQL Connector/ODBC from dev.mysql.com/downloads/connector/odbc/',
          ],
        },
      },
      linux: {
        managers: ['unixODBC'],
        install: {
          apt: [
            'sudo apt-get install -y unixodbc libmyodbc',
            '# Or for newer versions: download from dev.mysql.com/downloads/connector/odbc/',
          ],
          dnf: [
            'sudo dnf install -y unixODBC mysql-connector-odbc',
          ],
        },
      },
      win32: {
        managers: ['ODBC Data Source Administrator (built-in)'],
        install: {
          winget: [
            'winget install Oracle.MySQLConnectorODBC',
          ],
          manual: [
            '# Download MySQL Connector/ODBC from: https://dev.mysql.com/downloads/connector/odbc/',
          ],
        },
      },
    },
    officialUrl: 'https://dev.mysql.com/downloads/connector/odbc/',
  },

  // ── Oracle ─────────────────────────────────────────────────────────────────
  oracle: {
    label: 'Oracle',
    driverNames: [
      'Oracle 21 ODBC driver',
      'Oracle 19 ODBC driver',
      'Oracle 12 ODBC driver',
      'Oracle in OraClient',
      'Oracle',
    ],
    platforms: {
      darwin: {
        managers: ['unixODBC'],
        install: {
          brew: [
            'brew install unixodbc',
            '# Download Oracle Instant Client + ODBC from:',
            '# https://www.oracle.com/database/technologies/instant-client/macos-intel-x86-downloads.html',
            'sudo odbcinst -i -d -f /opt/oracle/instantclient_19_8/odbc_update_ini.sh',
          ],
        },
        notes: 'Instant Client requires manual registration with odbcinst.',
      },
      linux: {
        managers: ['unixODBC'],
        install: {
          manual: [
            '# Download Oracle Instant Client Basic + ODBC RPMs or ZIPs from:',
            '# https://www.oracle.com/database/technologies/instant-client/linux-x86-64-downloads.html',
            'sudo apt-get install -y unixodbc  # or dnf/yum',
            'sudo sh /opt/oracle/instantclient_21_x/odbc_update_ini.sh / /opt/oracle/instantclient_21_x',
          ],
        },
      },
      win32: {
        managers: ['ODBC Data Source Administrator (built-in)'],
        install: {
          manual: [
            '# Download Oracle Instant Client + ODBC from:',
            '# https://www.oracle.com/database/technologies/instant-client/winx64-64-downloads.html',
            '# Run odbc_install.exe from the Instant Client ODBC package.',
          ],
        },
        notes: 'Requires Visual C++ Redistributable. Match bitness (64-bit) with Node.js.',
      },
    },
    officialUrl: 'https://www.oracle.com/database/technologies/instant-client.html',
  },

  // ── DB2 LUW ────────────────────────────────────────────────────────────────
  db2: {
    label: 'DB2 LUW',
    driverNames: [
      'IBM DB2 ODBC DRIVER',
      'IBM DB2 ODBC DRIVER - DB2COPY1',
      'DB2',
    ],
    platforms: {
      darwin: {
        managers: ['unixODBC'],
        install: {
          manual: [
            '# Download IBM Data Server Driver for ODBC and CLI from IBM Fix Central:',
            '# https://www.ibm.com/support/fixcentral/quickorder?product=ibm%2FInformation+Management%2FIBM+Data+Server+Client+Packages',
            'brew install unixodbc',
            '# Register driver: db2cli install -setup',
          ],
        },
      },
      linux: {
        managers: ['unixODBC'],
        install: {
          manual: [
            '# Download IBM Data Server Driver Package from Fix Central.',
            'sudo apt-get install -y unixodbc',
            '# Register: <installdir>/bin/db2cli install -setup',
          ],
        },
      },
      win32: {
        managers: ['ODBC Data Source Administrator (built-in)'],
        install: {
          manual: [
            '# Download IBM Data Server Driver Package for Windows from Fix Central.',
            '# Run the installer — ODBC driver is registered automatically.',
          ],
        },
      },
    },
    officialUrl: 'https://www.ibm.com/support/fixcentral/quickorder?product=ibm%2FInformation+Management%2FIBM+Data+Server+Client+Packages',
  },

  // ── SQLite ─────────────────────────────────────────────────────────────────
  sqlite: {
    label: 'SQLite',
    driverNames: [
      'SQLite3 ODBC Driver',
      'SQLite ODBC Driver',
      'SQLite',
    ],
    platforms: {
      darwin: {
        managers: ['unixODBC'],
        install: {
          brew: [
            'brew install unixodbc',
            'brew install sqliteodbc',
          ],
        },
      },
      linux: {
        managers: ['unixODBC'],
        install: {
          apt: [
            'sudo apt-get install -y unixodbc libsqliteodbc',
          ],
          dnf: [
            'sudo dnf install -y unixODBC sqliteodbc',
          ],
        },
      },
      win32: {
        managers: ['ODBC Data Source Administrator (built-in)'],
        install: {
          manual: [
            '# Download SQLite ODBC Driver from: http://www.ch-werner.de/sqliteodbc/',
            '# Run the installer.',
          ],
        },
      },
    },
    officialUrl: 'http://www.ch-werner.de/sqliteodbc/',
  },
};

/**
 * Resolve the preferred package manager for the current platform.
 * Returns the first found among the common ones, or null.
 */
function detectPackageManager() {
  const { execSync } = require('child_process');
  const platform = process.platform;

  if (platform === 'win32') {
    try { execSync('winget --version', { stdio: 'ignore' }); return 'winget'; } catch {}
    try { execSync('choco --version', { stdio: 'ignore' }); return 'choco'; } catch {}
    return 'manual';
  }

  if (platform === 'darwin') {
    try { execSync('brew --version', { stdio: 'ignore' }); return 'brew'; } catch {}
    return 'manual';
  }

  // Linux
  try { execSync('apt-get --version', { stdio: 'ignore' }); return 'apt'; } catch {}
  try { execSync('dnf --version', { stdio: 'ignore' }); return 'dnf'; } catch {}
  try { execSync('yum --version', { stdio: 'ignore' }); return 'yum'; } catch {}
  return 'manual';
}

/**
 * Check whether the unixODBC manager is present (macOS / Linux).
 * Returns { found: bool, version: string|null }.
 */
function checkOdbcManager() {
  const platform = process.platform;

  if (platform === 'win32') {
    // On Windows the ODBC manager is always present (odbcad32.exe).
    const windir = process.env.WINDIR || 'C:\\Windows';
    const fs = require('fs');
    const found = fs.existsSync(`${windir}\\System32\\odbcad32.exe`);
    return { found, version: found ? 'built-in' : null };
  }

  const { execSync } = require('child_process');
  try {
    const out = execSync('odbcinst --version 2>&1', { encoding: 'utf8' }).trim();
    return { found: true, version: out.split('\n')[0] || 'unknown' };
  } catch {
    // odbcinst not found → unixODBC not installed
    return { found: false, version: null };
  }
}

/**
 * List ODBC drivers installed on the system.
 * Returns string[] of driver names, or null if detection is not possible.
 */
function listInstalledDrivers() {
  const platform = process.platform;
  const { execSync } = require('child_process');

  if (platform === 'win32') {
    // Read HKLM\SOFTWARE\ODBC\ODBCINST.INI\ODBC Drivers
    try {
      const out = execSync(
        'reg query "HKLM\\SOFTWARE\\ODBC\\ODBCINST.INI\\ODBC Drivers"',
        { encoding: 'utf8' }
      );
      return out
        .split('\n')
        .map(l => l.trim())
        .filter(l => l && !l.startsWith('HKEY') && !l.startsWith('(Default)'))
        .map(l => l.split(/\s+REG_SZ\s+/)[0].trim())
        .filter(Boolean);
    } catch {
      return null;
    }
  }

  // macOS / Linux: use odbcinst
  try {
    const out = execSync('odbcinst -q -d 2>/dev/null', { encoding: 'utf8' });
    return out
      .split('\n')
      .map(l => l.trim())
      .filter(l => l.startsWith('[') && l.endsWith(']'))
      .map(l => l.slice(1, -1));
  } catch {
    return null;
  }
}

/**
 * Check whether a specific type's driver is present in the installed list.
 * Returns the matched driver name or null.
 */
function findDriverForType(type, installedDrivers) {
  if (!installedDrivers) return null;
  const entry = DRIVER_REGISTRY[type];
  if (!entry) return null;

  const expected = entry.driverNames.map(n => n.toLowerCase());
  for (const installed of installedDrivers) {
    const lc = installed.toLowerCase();
    if (expected.some(e => lc.includes(e.toLowerCase()) || e.toLowerCase().includes(lc))) {
      return installed;
    }
  }
  return null;
}

/**
 * Get all registry entries as an array for display.
 */
function listRegistry() {
  return Object.entries(DRIVER_REGISTRY).map(([type, entry]) => ({
    type,
    label: entry.label,
    officialUrl: entry.officialUrl,
  }));
}

module.exports = {
  DRIVER_REGISTRY,
  detectPackageManager,
  checkOdbcManager,
  listInstalledDrivers,
  findDriverForType,
  listRegistry,
};
