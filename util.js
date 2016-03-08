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

util.serialize = (type, source) => {
  const isBuffer = util.isBuffer(source)

  if (type === 'bytea') {
    return isBuffer ? source : source == null ? '' : String(source)
  }

  if (type === 'text') {
    return isBuffer ? source.toString('utf8') : source == null ? '' : String(source)
  }

  if (type === 'jsonb' || type === 'json') {
    return JSON.parse(isBuffer ? source.toString('utf8') : source)
  }

  throw new Error('cannot serialize unknown data type:' + type)
}

util.deserialize = (type, source, asBuffer) => {
  if (type === 'bytea') {
    return asBuffer ? source : String(source || '')
  }

  if (type === 'text') {
    return asBuffer ? source.toString('utf8') : source == null ? '' : String(source)
  }

  if (type === 'jsonb' || type === 'json') {
    // TODO: id encoding to use as a pass through?
    return JSON.stringify(asBuffer ? source.toString('utf8') : source)
  }

  throw new Error('cannot deserialize unknown data type:' + type)
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

util.createPool = (config) => {
  config.name = mts()
  const pool = new ConnectionPool(Postgres, config, util.POOL_CONFIG)

  // const _query = pool.query
  // pool.query = function (text) {
  //   console.warn('SQL:', text)
  //   const query = _query.apply(this, arguments)
  //   return query
  // }

  // pool.__clients = []
  // pool.on('acquire', (client) => {
  //   pool.__clients.push(client)
  // })
  // pool.on('release', (client) => {
  //   pool.__clients = pool.__clients.filter((c) => c !== client)
  // })

  return pool
}

util.destroyPool = (pool, cb) => {
  pool.close(cb)
  // pool.close((err) => {
  //   if (pool.__clients.length) {
  //     // pool.__clients.forEach((client) => pool.destroy(client))
  //     cb(new Error('dangling clients: ' + pool.__clients.length))
  //   } else {
  //     cb(err)
  //   }
  // })
}

util.createTransaction = (client) => {
  return transaction(client)
}

util.createCursor = (db, statement) => {
  const client = Postgres.createConnection(db._config)
  const cursor = client.query(new Cursor(statement.text, statement.values))

  client.on('error', (err) => {
    console.warn('GOT CURSOR ERR:', err)
    client.close()
  })

  cursor.close = (cb) => {
    // NB: dirty hack to test the pool hanging issues... not working anyway...
    if (cursor.connection) {
      cursor.connection.close({type: 'P'})
      cursor.connection.sync()
      cursor.state = 'done'
      cursor.connection.once('closeComplete', () => {
        client.end()
        client.removeAllListeners()
        cb && cb()
      })
    } else {
      client.end()
      client.removeAllListeners()
      cb && process.nextTick(cb)
    }
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

util.POOL_CONFIG = {
  min: 0,
  max: 10,
  reset: function (conn, done) {
    conn.query('ROLLBACK', done)
  }
}

util.parseConfig = (location) => {
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
  if (parts.length) throw new Error('sublevel paths NYI')

  // remaining components represent schema namespace
  // TODO: surface default `public` schema in opts?
  // config._path = parts.length ? parts.join('__') : null

  return config
}

// TODO: create/drop database, e.g.:
// https://github.com/olalonde/pgtools/blob/master/index.js

util.dropTable = (db, cb) => {
  const client = Postgres.createConnection(db._config)
  client.on('error', (err) => destroyClient(err, client, cb))
  client.query(`DROP TABLE IF EXISTS ${db._rel}`, (err) => {
    destroyClient(err, client, cb)
  })
}

const destroyClient = (err, client, cb) => {
  if (err) return cb(err)
  client && client.end()
  process.nextTick(cb)
}
