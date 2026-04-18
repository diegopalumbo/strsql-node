'use strict';

const chalk = require('chalk');

const BAR_WIDTH = 30;

/**
 * Inline terminal progress bar.
 * Call tick(done, total) to update in-place.
 * Call finish() to print the final line and move to next line.
 */
class ProgressBar {
  constructor(label = 'Importing') {
    this.label    = label;
    this._started = false;
    this._done    = false;
  }

  tick(done, total) {
    if (this._done) return;
    this._started = true;

    const pct   = total > 0 ? Math.min(1, done / total) : 0;
    const filled = Math.round(BAR_WIDTH * pct);
    const empty  = BAR_WIDTH - filled;

    const bar  = chalk.green('█'.repeat(filled)) + chalk.dim('░'.repeat(empty));
    const pctS = String(Math.round(pct * 100)).padStart(3) + '%';
    const line = `\r  ${this.label}  [${bar}]  ${pctS}  ${done}/${total} rows `;

    process.stdout.write(line);
  }

  finish(result) {
    if (!this._started) return;
    this._done = true;

    // Clear the bar line
    process.stdout.write('\r' + ' '.repeat(80) + '\r');

    const icon    = result.errors.length === 0 ? chalk.green('✓') : chalk.yellow('⚠');
    const dryTag  = result.dryRun ? chalk.dim(' [DRY RUN]') : '';
    const errTag  = result.errors.length > 0
      ? chalk.yellow(`  errors=${result.errors.length}`)
      : '';

    console.log(
      `  ${icon} ${chalk.bold(result.inserted)} inserted` +
      (result.skipped > 0 ? chalk.dim(`  ${result.skipped} skipped`) : '') +
      errTag +
      chalk.dim(`  ${result.elapsed}ms`) +
      dryTag
    );
  }
}

module.exports = { ProgressBar };
