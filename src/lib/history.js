'use strict';

const fs = require('fs');
const path = require('path');
const { PROFILES_DIR } = require('./profiles');

const HISTORY_FILE = path.join(PROFILES_DIR, 'history.json');
const MAX_HISTORY = 500;

class HistoryManager {
  constructor(sessionLimit = 100) {
    this.sessionLimit = sessionLimit;
    this._entries = this._load();
  }

  _load() {
    try {
      if (fs.existsSync(HISTORY_FILE)) {
        return JSON.parse(fs.readFileSync(HISTORY_FILE, 'utf8'));
      }
    } catch { /* ignore */ }
    return [];
  }

  _save() {
    try {
      fs.writeFileSync(HISTORY_FILE, JSON.stringify(this._entries.slice(-MAX_HISTORY), null, 2));
    } catch { /* non-fatal */ }
  }

  add(sql) {
    const trimmed = sql.trim();
    if (!trimmed) return;
    // Remove consecutive duplicates
    if (this._entries[this._entries.length - 1] === trimmed) return;
    this._entries.push(trimmed);
    this._save();
  }

  /**
   * Return entries for readline history (most recent first).
   */
  forReadline() {
    return [...this._entries].reverse().slice(0, this.sessionLimit);
  }

  all() {
    return [...this._entries];
  }

  clear() {
    this._entries = [];
    this._save();
  }

  search(keyword) {
    const kw = keyword.toLowerCase();
    return this._entries.filter(e => e.toLowerCase().includes(kw));
  }
}

module.exports = { HistoryManager };
