'use strict'

const AbstractLevelDOWN = require('abstract-leveldown/abstract-leveldown')
const mts = require('monotonic-timestamp')
const pg = require('pg')
const Cursor = require('pg-cursor')
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

util.createTransaction = function (connection) {
  return transaction(connection)
}

util.createCursor = function (db, statement) {
  const pool = db._pool
  const connection = Postgres.createConnection(db._config)
  const cursor = connection.query(new Cursor(statement.text, statement.values))
  cursor.close = (cb) => {
    connection.removeAllListeners()
    connection.on('error', function () {})
    connection.end()
    process.nextTick(cb)
  }
  return cursor
}

// set up pg connection defaults with standard PG* env var overrides
const PG_DEFAULTS = util.PG_DEFAULTS = {}

PG_DEFAULTS.database = process.env.PGDATABASE || 'postgres'
PG_DEFAULTS.host = process.env.PGHOSTADDR || pg.defaults.host
PG_DEFAULTS.port = Number(process.env.PGPORT) || pg.defaults.port
PG_DEFAULTS.user = process.env.PGUSER || pg.defaults.user
PG_DEFAULTS.password = process.env.PGPASSWORD || pg.defaults.password
PG_DEFAULTS.idleTimeout = pg.defaults.idleTimeoutMillis
PG_DEFAULTS.reapInterval = pg.defaults.reapIntervalMillis

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
