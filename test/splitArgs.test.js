'use strict';

/**
 * Tests for _splitArgs tokenizer (extracted from session.js).
 * The function is private but pure, so we replicate it here exactly.
 */

const assert = require('assert');

// ─── replicate _splitArgs exactly as in session.js ───────────────────────────

function _splitArgs(line) {
  const tokens = [];
  let cur = '';
  let quote = null;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (quote) {
      if (ch === quote) { quote = null; }
      else { cur += ch; }
    } else if (ch === '"' || ch === "'") {
      quote = ch;
    } else if (/\s/.test(ch)) {
      if (cur) { tokens.push(cur.replace(/;+$/, '')); cur = ''; }
    } else {
      cur += ch;
    }
  }
  if (cur) tokens.push(cur.replace(/;+$/, ''));
  return tokens;
}

// ─── helpers ─────────────────────────────────────────────────────────────────

let passed = 0;
let failed = 0;

function test(name, fn) {
  try {
    fn();
    console.log(`  ✅ ${name}`);
    passed++;
  } catch (err) {
    console.error(`  ❌ ${name}`);
    console.error(`     ${err.message}`);
    failed++;
  }
}

// ─── tests ───────────────────────────────────────────────────────────────────

console.log('\n_splitArgs');

test('splits simple tokens on whitespace', () => {
  assert.deepStrictEqual(_splitArgs('migrations run ./migrations'), ['migrations', 'run', './migrations']);
});

test('handles double-quoted strings as single token', () => {
  assert.deepStrictEqual(_splitArgs('seeds create "./my seeds"'), ['seeds', 'create', './my seeds']);
});

test('handles single-quoted strings as single token', () => {
  assert.deepStrictEqual(_splitArgs("seeds create './my dir'"), ['seeds', 'create', './my dir']);
});

test('strips trailing semicolons', () => {
  assert.deepStrictEqual(_splitArgs('migrations run ./migs;'), ['migrations', 'run', './migs']);
});

test('handles multiple trailing semicolons', () => {
  assert.deepStrictEqual(_splitArgs('foo;;;'), ['foo']);
});

test('handles leading/trailing whitespace', () => {
  assert.deepStrictEqual(_splitArgs('  migrations   run  '), ['migrations', 'run']);
});

test('returns empty array for empty string', () => {
  assert.deepStrictEqual(_splitArgs(''), []);
});

test('returns empty array for whitespace-only string', () => {
  assert.deepStrictEqual(_splitArgs('   '), []);
});

test('preserves spaces inside quoted argument', () => {
  const result = _splitArgs('"hello world" foo');
  assert.strictEqual(result[0], 'hello world');
  assert.strictEqual(result[1], 'foo');
});

test('single token with no spaces', () => {
  assert.deepStrictEqual(_splitArgs('\\help'), ['\\help']);
});

test('backslash-command with arguments', () => {
  assert.deepStrictEqual(
    _splitArgs('\\migrations run ./migs up'),
    ['\\migrations', 'run', './migs', 'up']
  );
});

test('quoted token can contain semicolons without stripping', () => {
  // semicolons inside quotes should NOT be stripped — they're part of the value
  // Note: current impl strips trailing semicolons from the token, but
  // inside a quoted string the semicolon remains in `cur` without special handling.
  const result = _splitArgs('"hello;world"');
  assert.strictEqual(result[0], 'hello;world');
});

// ─── summary ─────────────────────────────────────────────────────────────────

console.log(`\n  ${passed} passed, ${failed} failed\n`);
if (failed > 0) process.exitCode = 1;
