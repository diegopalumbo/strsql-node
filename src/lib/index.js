'use strict';

const { ODBCConnection, IBMiConnection } = require('./connection');
const { ProfileManager }  = require('./profiles');
const { HistoryManager }  = require('./history');
const { formatTable, formatExecResult, toCSV, toJSON, toInsert, toMerge, exportToFile } = require('./formatter');
const { Importer, ImportResult, ERROR_MODE } = require('./importer');
const { Pipe, PipeResult, generateDDL }      = require('./pipe');
const { listDrivers, getDriver, DRIVERS }    = require('./drivers');
const { Dialect }                            = require('./dialect');

module.exports = {
  ODBCConnection,
  IBMiConnection,   // backward-compat alias
  ProfileManager,
  HistoryManager,
  formatTable,
  formatExecResult,
  toCSV,
  toJSON,
  toInsert,
  toMerge,
  exportToFile,
  Importer,
  ImportResult,
  ERROR_MODE,
  Pipe,
  PipeResult,
  generateDDL,
  listDrivers,
  getDriver,
  DRIVERS,
  Dialect,
};
