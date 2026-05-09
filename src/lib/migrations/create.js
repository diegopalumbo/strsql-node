'use strict';

const fs   = require('fs');
const path = require('path');

function pad(n) { return n < 10 ? '0' + n : n; }

function getTimestamp() {
  const d = new Date();
  return [
    d.getFullYear(),
    pad(d.getMonth() + 1),
    pad(d.getDate()),
    pad(d.getHours()),
    pad(d.getMinutes()),
    pad(d.getSeconds()),
  ].join('');
}

/**
 * Create a migration file pair (.up.sql / .down.sql).
 *
 * @param {string} name       Migration name, used as suffix in the filename.
 * @param {string} targetDir  Directory where files are created. Defaults to ./migrations.
 * @param {object} [opts]
 * @param {string} [opts.upContent]   Content written to .up.sql (default: path comment only).
 * @param {string} [opts.downContent] Content written to .down.sql (default: path comment only).
 * @returns {{ upFile: string, downFile: string }}
 */
function createMigration(name, targetDir, opts = {}) {
  const timestamp = getTimestamp();
  const dir = targetDir ? path.resolve(targetDir) : path.join(process.cwd(), 'migrations');
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });

  const upFile   = path.join(dir, `${timestamp}_${name}.up.sql`);
  const downFile = path.join(dir, `${timestamp}_${name}.down.sql`);

  fs.writeFileSync(upFile,   opts.upContent   !== undefined ? opts.upContent   : `-- ${upFile}\n`);
  fs.writeFileSync(downFile, opts.downContent !== undefined ? opts.downContent : `-- ${downFile}\n`);

  return { upFile, downFile };
}

/**
 * Create a seed file pair (.up.sql / .down.sql).
 *
 * @param {string} name       Seed name, used as suffix in the filename.
 * @param {string} targetDir  Directory where files are created. Defaults to ./seeds.
 * @param {object} [opts]
 * @param {string} [opts.upContent]   Content written to .up.sql (default: path comment only).
 * @param {string} [opts.downContent] Content written to .down.sql (default: path comment only).
 * @returns {{ upFile: string, downFile: string }}
 */
function createSeed(name, targetDir, opts = {}) {
  const timestamp = getTimestamp();
  const dir = targetDir ? path.resolve(targetDir) : path.join(process.cwd(), 'seeds');
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });

  const upFile   = path.join(dir, `${timestamp}_${name}.up.sql`);
  const downFile = path.join(dir, `${timestamp}_${name}.down.sql`);

  fs.writeFileSync(upFile,   opts.upContent   !== undefined ? opts.upContent   : `-- ${upFile}\n`);
  fs.writeFileSync(downFile, opts.downContent !== undefined ? opts.downContent : `-- ${downFile}\n`);

  return { upFile, downFile };
}

module.exports = { createMigration, createSeed };
