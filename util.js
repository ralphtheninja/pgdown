'use strict'

const util = exports

const AbstractLevelDOWN = require('abstract-leveldown/abstract-leveldown')
const mts = require('monotonic-timestamp')
const pg = require('pg')
const Cursor = require('pg-cursor')
const Postgres = require('any-db-postgres')
const ConnectionPool = require('any-db-pool')
const transaction = require('any-db-transaction')
const errors = require('level-errors')

util.escape = require('pg-format')

util.isBuffer = AbstractLevelDOWN.prototype._isBuffer

util.serialize = (type, source) => {
  const fn = util.serialize[type]
  if (!fn) throw new Error('unable to serialize unknown data type:' + type)
  return fn(source)
}

util.serialize.bytea = (source) => (
  util.isBuffer(source) ? source : source == null ? '' : String(source)
)

util.serialize.text = (source) => (
  util.isBuffer(source) ? source.toString('utf8') : source == null ? '' : String(source)
)

util.serialize.jsonb = (source) => (
  JSON.parse(util.isBuffer(source) ? source.toString('utf8') : source)
)

util.deserialize = (type, source, asBuffer) => {
  const fn = util.deserialize[type]
  if (!fn) throw new Error('unable to deserialize unknown data type:' + type)
  return fn(source, asBuffer)
}

util.deserialize.bytea = (source, asBuffer) => (
  asBuffer ? source : String(source || '')
)

util.deserialize.text = (source, asBuffer) => (
  asBuffer ? source.toString('utf8') : source == null ? '' : String(source)
)

util.deserialize.jsonb = (source, asBuffer) => (
  JSON.stringify(asBuffer ? source.toString('utf8') : source)
)

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
  // pool.query = function (text, params) {
  //   console.warn('SQL:', text, params)
  //   const query = _query.apply(this, arguments)
  //   query.on('error', (err) => console.error('QUERY ERR:', err))
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
  //     pool.__clients.forEach((client) => pool.destroy(client))
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
PG_DEFAULTS.binary = false

util.POOL_CONFIG = {
  min: 0,
  max: 10,
  reset: function (conn, done) {
    conn.query('ROLLBACK', done)
  }
}

// TODO: move this into PgDOWN class
util.schemaName = 'pgdown'

util.parseLocation = (location) => {
  const config = {}

  // copy over pg defaults
  for (var key in PG_DEFAULTS) {
    if (PG_DEFAULTS[key] !== undefined) config[key] = PG_DEFAULTS[key]
  }

  // TODO: complete postgres:// uri parsing
  const parts = location.split('/')

  // location beginning with slash specifies database name
  if (location[0] === '/') {
    parts.shift()
    config.database = parts.shift() || config.database
  }

  // remaining components of location specifiy sublevel path/table name
  const tableName = parts.join('/')
  if (!tableName) throw new Error('table name required')

  const table = config._table = util.escape.ident(tableName)
  const schema = config._schema = util.escape.ident(util.schemaName)

  // set relation name using (assuming pgdown as schema name)
  config._relation = schema + '.' + table

  return config
}

// TODO: create/drop database, e.g.:
// https://github.com/olalonde/pgtools/blob/master/index.js

util.dropTable = (location, cb) => {
  const config = util.parseLocation(location)
  const client = Postgres.createConnection(config)
  client.on('error', (err) => destroyClient(err, client, cb))
  client.query(`DROP TABLE IF EXISTS ${config._relation}`, (err) => {
    destroyClient(err, client, cb)
  })
}

const destroyClient = (err, client, cb) => {
  if (err) return cb(err)
  client && client.end()
  process.nextTick(cb)
}
