'use strict';

/**
 * pager.js — Terminal pager integration for strsql-node.
 *
 * Detects `less`, `more` or `most` on the current system and pipes
 * formatted output through them so the user can scroll horizontally
 * (-S flag) and vertically without the table disappearing off screen.
 *
 * Usage:
 *   const { printWithPager } = require('./pager');
 *   await printWithPager(formattedString, { force: false });
 *
 * Behaviour:
 *   - In a non-interactive session (batch / pipe / redirect) it falls
 *     back to plain process.stdout.write — identical to today.
 *   - When the output fits in the terminal (fewer lines than rows) it
 *     also skips the pager to avoid unnecessary overhead.
 *   - Set STRSQL_NO_PAGER=1 or pass { force: false } to disable.
 *   - Set PAGER env var to override the auto-detected command.
 */

const { spawnSync, spawn } = require('child_process');
const fs = require('fs');
const os = require('os');
const path = require('path');

// ─── detection ───────────────────────────────────────────────────────────────

/**
 * Find the best available pager on this system.
 * Returns null if none is found (e.g. bare Windows without Git Bash).
 */
function detectPager() {
  // On Windows, prefer Git for Windows less.exe even if it's not on PATH.
  // This gives horizontal scrolling (-S) while `more` always wraps lines.
  let windowsLess = null;
  if (os.platform() === 'win32') {
    const candidates = [
      path.join(process.env['ProgramFiles'] || 'C:\\Program Files', 'Git', 'usr', 'bin', 'less.exe'),
      path.join(process.env['ProgramFiles(x86)'] || 'C:\\Program Files (x86)', 'Git', 'usr', 'bin', 'less.exe'),
      path.join(process.env['LocalAppData'] || '', 'Programs', 'Git', 'usr', 'bin', 'less.exe'),
    ];
    for (const p of candidates) {
      try {
        if (p && fs.existsSync(p)) {
          windowsLess = p;
          break;
        }
      } catch {
        // ignore
      }
    }
  }

  // Respect explicit user preference, except `more` on Windows when less exists.
  if (process.env.PAGER) {
    const envPager = process.env.PAGER;
    const envName = pagerName(envPagerCommand(envPager));
    if (os.platform() === 'win32' && (envName === 'more' || envName === 'more.com')) {
      if (windowsLess) return windowsLess;
      // On Windows `more` always wraps long lines; skip pager if less is unavailable.
      return null;
    }
    if (os.platform() === 'win32' && windowsLess && envName === 'less') {
      // Normalize to a concrete executable path to avoid shell alias/function quirks.
      return windowsLess;
    }
    return envPager;
  }

  if (windowsLess) return windowsLess;

  const candidates = os.platform() === 'win32'
    ? ['less', 'most']
    : ['less', 'more', 'most'];

  // On Windows, where/which behaves differently
  const whichCmd = os.platform() === 'win32' ? 'where' : 'which';

  for (const p of candidates) {
    try {
      const r = spawnSync(whichCmd, [p], { stdio: 'pipe', timeout: 2000 });
      if (r.status === 0 && r.stdout && r.stdout.toString().trim()) {
        return p;
      }
    } catch {
      // ignore
    }
  }
  return null;
}

function pagerName(pager) {
  return (pager || '').split(/[\\/]/).pop().toLowerCase();
}

function envPagerCommand(pagerEnv) {
  const v = String(pagerEnv || '').trim();
  if (!v) return '';
  const m = v.match(/^(["'])(.+?)\1/);
  if (m) return m[2];
  return v.split(/\s+/)[0];
}

function windowsCodePage() {
  if (os.platform() !== 'win32') return null;
  try {
    const r = spawnSync('chcp', [], { stdio: 'pipe', timeout: 2000 });
    if (r.status !== 0 || !r.stdout) return null;
    const out = r.stdout.toString();
    const m = out.match(/(\d{3,5})/);
    return m ? parseInt(m[1], 10) : null;
  } catch {
    return null;
  }
}

function prefersAsciiOutput() {
  if (process.env.STRSQL_ASCII === '1') return true;
  if (process.env.STRSQL_ASCII === '0') return false;

  if (os.platform() !== 'win32') return false;

  const pager = detectPager();
  const name = pagerName(pager);

  // Windows `more` is unreliable with UTF-8 box-drawing and ANSI styles.
  if (name === 'more' || name === 'more.com') return true;

  const cp = windowsCodePage();
  if (cp && cp !== 65001) return true;

  return false;
}

/**
 * Build the argument list for the detected pager.
 *
 * less flags used:
 *   -S   chop long lines (enables horizontal scrolling with ← →)
 *   -R   pass ANSI colour codes through raw (chalk colours visible)
 *   -F   quit immediately if output fits on one screen (no pager overhead)
 *   -X   don't clear the screen on exit (result stays visible)
 *
 * more / most: no flags — they handle colour poorly but work as fallback.
 */
function pagerArgs(pager) {
  const name = pagerName(pager);
  if (name === 'less' || name === 'less.exe') return ['-S', '-R', '-F', '-X'];
  return [];
}

function pagerDebugInfo() {
  const pager = detectPager();
  return {
    pager,
    pagerName: pagerName(pager),
    args: pager ? pagerArgs(pager) : [],
    interactive: isInteractive(),
    ascii: prefersAsciiOutput(),
    envPager: process.env.PAGER || null,
    envLess: process.env.LESS || null,
  };
}

// ─── isInteractive ────────────────────────────────────────────────────────────

/**
 * Returns true when both stdin and stdout are real TTYs (interactive terminal).
 * When output is piped or redirected this returns false and we skip the pager.
 */
function isInteractive() {
  return Boolean(process.stdout.isTTY && process.stdin.isTTY);
}

// ─── line / terminal size helpers ─────────────────────────────────────────────

function terminalRows() {
  return process.stdout.rows || 24;
}

function countLines(text) {
  return text.split('\n').length;
}

// ─── core API ─────────────────────────────────────────────────────────────────

/**
 * Print `text` through a pager if appropriate, otherwise write directly.
 *
 * @param {string} text     The formatted string to display.
 * @param {object} opts
 * @param {boolean} [opts.force]      Force pager even on small output (default: false).
 * @param {boolean} [opts.disabled]   Bypass pager entirely (default: false).
 * @returns {Promise<void>}
 */
async function printWithPager(text, opts = {}) {
  // 1. Batch / non-interactive → plain write, no pager
  if (!isInteractive() || opts.disabled || process.env.STRSQL_NO_PAGER === '1') {
    process.stdout.write(text + '\n');
    return;
  }

  // 2. Output fits in terminal → skip pager (avoids overhead for small results)
  const lines = countLines(text);
  if (!opts.force && lines < terminalRows() - 2) {
    process.stdout.write(text + '\n');
    return;
  }

  // 3. Try to find a pager
  const pager = detectPager();
  if (!pager) {
    // No pager available (bare Windows, stripped container, etc.)
    process.stdout.write(text + '\n');
    return;
  }

  // 4. Spawn pager on a temp file with inherited stdio so it can own stdin.
  // This prevents REPL keystrokes from leaking while the pager is open.
  return new Promise((resolve) => {
    const args  = pagerArgs(pager);
    const tmpFile = path.join(
      os.tmpdir(),
      `strsql-pager-${process.pid}-${Date.now()}-${Math.random().toString(36).slice(2)}.txt`
    );

    let child;
    try {
      fs.writeFileSync(tmpFile, text + '\n', 'utf8');
      child = spawn(pager, [...args, tmpFile], { stdio: 'inherit' });
    } catch {
      process.stdout.write(text + '\n');
      try { fs.unlinkSync(tmpFile); } catch {}
      resolve();
      return;
    }

    child.on('close', () => {
      try { fs.unlinkSync(tmpFile); } catch {}
      resolve();
    });
    child.on('error', () => {
      // Pager couldn't start — fall back to plain write
      process.stdout.write(text + '\n');
      try { fs.unlinkSync(tmpFile); } catch {}
      resolve();
    });
  });
}

module.exports = { printWithPager, detectPager, isInteractive, prefersAsciiOutput, pagerDebugInfo };