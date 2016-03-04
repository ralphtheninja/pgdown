'use strict'

const AbstractLevelDOWN = require('abstract-leveldown/abstract-leveldown')
const mts = require('monotonic-timestamp')
const pg = require('pg')
const errors = require('level-errors')

const util = exports

util.escapeIdentifier = pg.Client.prototype.escapeIdentifier

util.isBuffer = AbstractLevelDOWN.prototype._isBuffer

function deserialize (source, asBuffer) {
  return asBuffer ? source : String(source || '')
}

function serialize (source) {
  return util.isBuffer(source) ? source : source == null ? '' : String(source)
}

util.deserializeKey = deserialize
util.deserializeValue = deserialize
util.serializeKey = serialize
util.serializeValue = serialize

util.NotFoundError = errors.NotFoundError

util.createPool = function (db) {
  // create a unique id to keep from pissing in the connection pool on close
  db._config._poolId = mts()
  return (db._pool = pg.pools.getOrCreate(db._config))
}

util.destroyPool = function (db, cb) {
  // grab a handle to current pool
  const pool = db._pool

  // TODO: add timeout for when drain hangs?
  pool.drain(() => {
    pool.destroyAllNow()
    cb()
  })
}

util.destroyAll = function (cb) {
  process.nextTick(() => {
    pg.end()
    cb()
  })
}

util.connect = function (db) {
  return new Promise((resolve, reject) => {
    pg.connect(db._config, (err, client, done) => {
      if (err) return reject(err)

      // // override client query method
      // var _query = client.query
      // client.query = function (command, params) {
      //   console.warn('SQL:', command, params)
      //   return _query.apply(this, arguments)
      // }

      // add query pool helper
      client.release = (err) => {
        client.release = () => {}
        done(err)
      }
      resolve(client)
    })
  })
}

util.drop = function (db, cb) {
  util.connect(db).then((client) => {
    client.query(`DROP TABLE ${db._qname}`, (err) => {
      client.release(err)
      cb(err || null)
    })
  })
  .catch((err) => cb(err))
}

// set pg defaults
const defaults = util.defaults = {}
for (var key in pg.defaults) {
  defaults[key] = pg.defaults[key]
}

// allow standard pg env vars to override some defaults
defaults.database = process.env.PGDATABASE || defaults.database || 'postgres'
defaults.user = process.env.PGUSER || defaults.user
defaults.password = process.env.PGPASSWORD || defaults.password
defaults.host = process.env.PGHOSTADDR || defaults.host
defaults.port = Number(process.env.PGPORT) || defaults.port

util.config = function (location) {
  const config = {}

  // TODO: complete postgres:// uri parsing
  const parts = location.split('/')

  // last component of location specifies table name
  const table = config._table = parts.pop()
  if (!table) throw new Error('location must specify table name')

  // copy over defaults
  for (var key in defaults) {
    if (defaults[key] !== undefined) config[key] = defaults[key]
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
