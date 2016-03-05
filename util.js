'use strict'

const AbstractLevelDOWN = require('abstract-leveldown/abstract-leveldown')
const mts = require('monotonic-timestamp')
const pg = require('pg')
const Postgres = require('any-db-postgres')
const ConnectionPool = require('any-db-pool')
const transaction = require('any-db-transaction')
const errors = require('level-errors')

const util = exports

util.escapeIdentifier = pg.Client.prototype.escapeIdentifier

util.isBuffer = AbstractLevelDOWN.prototype._isBuffer

util.serialize = function (source) {
  return util.isBuffer(source) ? source : source == null ? '' : String(source)
}

util.deserialize = function (source, asBuffer) {
  return asBuffer ? source : String(source || '')
}

util.comparators = {
  eq: () => '=',
  ne: () => '<>',
  lt: () => '<',
  lte: () => '<=',
  min: () => '<=',
  gt: () => '>',
  gte: () => '>=',
  max: () => '>=',
  start: (range) => range.reverse ? '<=' : '>=',
  end: (range) => range.reverse ? '>=' : '<='
}

util.NotFoundError = errors.NotFoundError

util.createPool = function (config) {
  return new ConnectionPool(Postgres, config, util.POOL_CONFIG)
}

util.createTransaction = function (conn) {
  return transaction(conn)
}

// set up pg connection defaults with standard PG* env var overrides
const PG_DEFAULTS = util.PG_DEFAULTS = {}

PG_DEFAULTS.database = process.env.PGDATABASE || PG_DEFAULTS.database || 'postgres'
PG_DEFAULTS.host = process.env.PGHOSTADDR || PG_DEFAULTS.host || pg.defaults.host
PG_DEFAULTS.port = Number(process.env.PGPORT) || PG_DEFAULTS.port || pg.defaults.port
PG_DEFAULTS.user = process.env.PGUSER || PG_DEFAULTS.user || pg.defaults.user
PG_DEFAULTS.password = process.env.PGPASSWORD || PG_DEFAULTS.password || pg.defaults.password

// pool config:
//   min: Number?,
//   max: Number?,
//   idleTimeout: Number?,
//   reapInterval: Number?,
//   refreshIdle: Boolean?,
//   onConnect: (Connection, ready: Continuation<Connection>) => void
//   reset: (Connection, done: Continuation<void>) => void
//   shouldDestroyConnection: (error: Error) => Boolean

util.POOL_CONFIG = {
  min: 2,
  max: 20,
  reset: function (conn, done) {
    conn.query('ROLLBACK', done)
  }
}


util.parseConfig = function (location) {
  const config = {}

  // TODO: complete postgres:// uri parsing
  const parts = location.split('/')

  // last component of location specifies table name
  const table = config._table = parts.pop()
  if (!table) throw new Error('location must specify table name')

  // copy over pg defaults
  for (var key in PG_DEFAULTS) {
    if (PG_DEFAULTS[key] !== undefined) config[key] = PG_DEFAULTS[key]
  }

  // location beginning with slash specifies database name
  if (location[0] === '/') {
    parts.shift()
    config.database = parts.shift() || config.database
  }

  // NB: this will eventually allow us to support subleveling natively
  // TODO: use extra path parts for schema name
  if (parts.length) throw new Error('schema paths NYI')

  // remaining components represent schema namespace
  // TODO: surface default `public` schema in opts?
  // config._schema = parts.length ? parts.join('__') : 'public'

  return config
}

// TODO: create/drop database, e.g.:
// https://github.com/olalonde/pgtools/blob/master/index.js

util.dropTable = function (db, cb) {
  const conn = Postgres.createConnection(db._config)
  conn.query(`DROP TABLE IF EXISTS ${db._qname}`, (err) => cb(err || null))
}
