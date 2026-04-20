'use strict';

const fs = require('fs');
const path = require('path');
const os = require('os');

const PROFILES_DIR = path.join(os.homedir(), '.strsql-node');
const PROFILES_FILE = path.join(PROFILES_DIR, 'profiles.json');

/**
 * Named connection profiles stored in ~/.strsql-node/profiles.json
 * Each profile: { host, username, password, defaultSchema, namingMode, ... }
 */
class ProfileManager {
  constructor() {
    this._ensureDir();
  }

  _ensureDir() {
    if (!fs.existsSync(PROFILES_DIR)) {
      fs.mkdirSync(PROFILES_DIR, { recursive: true });
    }
  }

  _load() {
    if (!fs.existsSync(PROFILES_FILE)) return {};
    try {
      return JSON.parse(fs.readFileSync(PROFILES_FILE, 'utf8'));
    } catch {
      return {};
    }
  }

  _save(profiles) {
    fs.writeFileSync(PROFILES_FILE, JSON.stringify(profiles, null, 2), 'utf8');
  }

  list() {
    const profiles = this._load();
    return Object.keys(profiles).map(name => ({
      name,
      type:          profiles[name].type || 'ibmi',
      host:          profiles[name].host || profiles[name].database || '',
      username:      profiles[name].username,
      defaultSchema: profiles[name].defaultSchema || '',
      database:      profiles[name].database || '',
    }));
  }

  get(name) {
    const profiles = this._load();
    if (!profiles[name]) throw new Error(`Profile "${name}" not found.`);
    return profiles[name];
  }

  set(name, config) {
    const profiles = this._load();
    profiles[name] = { ...config };
    this._save(profiles);
  }

  remove(name) {
    const profiles = this._load();
    if (!profiles[name]) throw new Error(`Profile "${name}" not found.`);
    delete profiles[name];
    this._save(profiles);
  }

  exists(name) {
    const profiles = this._load();
    return !!profiles[name];
  }

  /**
   * Load a profile and merge with environment variables.
   * ENV override keys: STRSQL_HOST, STRSQL_USER, STRSQL_PASSWORD, STRSQL_SCHEMA
   */
  resolve(name) {
    const base = name ? this.get(name) : {};
    return {
      type:          base.type || 'ibmi',
      host:          process.env.STRSQL_HOST         || base.host,
      username:      process.env.STRSQL_USER         || base.username,
      password:      process.env.STRSQL_PASSWORD     || base.password,
      defaultSchema: process.env.STRSQL_SCHEMA       || base.defaultSchema,
      libraryList:   process.env.STRSQL_LIBRARY_LIST || base.libraryList,
      namingMode:    base.namingMode || 'sql',
      ...base,
      // ENV always wins
      ...(process.env.STRSQL_HOST         && { host:          process.env.STRSQL_HOST }),
      ...(process.env.STRSQL_USER         && { username:      process.env.STRSQL_USER }),
      ...(process.env.STRSQL_PASSWORD     && { password:      process.env.STRSQL_PASSWORD }),
      ...(process.env.STRSQL_SCHEMA       && { defaultSchema: process.env.STRSQL_SCHEMA }),
      ...(process.env.STRSQL_LIBRARY_LIST && { libraryList:   process.env.STRSQL_LIBRARY_LIST }),
    };
  }
}

module.exports = { ProfileManager, PROFILES_DIR };
