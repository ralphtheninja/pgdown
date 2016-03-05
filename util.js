'use strict'

const AbstractLevelDOWN = require('abstract-leveldown/abstract-leveldown')
const mts = require('monotonic-timestamp')
const pg = require('pg')
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
  // create a unique id to keep from pissing in the connection pool on close
  config.__id = mts()
  return pg.pools.getOrCreate(config)
}

util.destroyPool = function (pool, cb) {
  // remove from pg pools
  if (!pool) return process.nextTick(cb)

  delete pg.pools.all[pool.getName()]

  pool.emit('destroy', new Error('pool destroyed'))

  // TODO: timeout to handle drain hangs?
  pool.drain(() => {
    pool.destroyAllNow(cb)
  })
}

util.destroyAll = function (cb) {
  process.nextTick(() => {
    pg.end()
    cb()
  })
}

// set up pg defaults
const PG_DEFAULTS = util.PG_DEFAULTS = {}
for (var key in pg.defaults) {
  PG_DEFAULTS[key] = pg.defaults[key]
}

// allow standard pg env vars to override some defaults
PG_DEFAULTS.database = process.env.PGDATABASE || PG_DEFAULTS.database || 'postgres'
PG_DEFAULTS.user = process.env.PGUSER || PG_DEFAULTS.user
PG_DEFAULTS.password = process.env.PGPASSWORD || PG_DEFAULTS.password
PG_DEFAULTS.host = process.env.PGHOSTADDR || PG_DEFAULTS.host
PG_DEFAULTS.port = Number(process.env.PGPORT) || PG_DEFAULTS.port

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

util.PG_ERRORS = {
  drop_failure_non_existent: '42P01',
  duplicate_database: '42P04',
  invalid_catalog_name: '3D000'
  // ...
}

util.connect = function (db) {
  return new Promise((resolve, reject) => {
    pg.connect(db._config, (err, client, done) => {
      if (err) return reject(err)

      // add creation timestamp to client
      client.__id = mts()

      // create a slightly better client query method
      client._exec = function (command, params, cb) {
        if (typeof params === 'function') {
          cb = params
          params = null
        }

        // console.warn('COMMAND:', command.text || command)
        const result = client.query(command, params)

        if (cb) {
          const rows = []
          result.on('error', cb)
          .on('row', (row) => rows.push(row))
          .on('end', () => cb(null, rows))
        }

        return result
      }

      // add connection pool helper
      client.release = (err) => {
        client.release = () => {}
        done(err)
      }
      resolve(client)
    })
  })
}

// TODO: create/drop database, e.g.:
// https://github.com/olalonde/pgtools/blob/master/index.js

util.dropTable = function (db, cb) {
  util.connect(db).then((client) => {
    client._exec(`DROP TABLE IF EXISTS ${db._qname}`, (err) => {
      client.release(err)
      cb(err || null)
    })
  })
  .catch((err) => cb(err))
}
