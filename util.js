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

util.pg = pg

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

util.connect = function (db) {
  return new Promise((resolve, reject) => {
    pg.connect(db._config, (err, client, done) => {
      if (err) {
        reject(err)
      } else {
        client.release = (err) => {
          client.release = () => {}
          done(err)
        }
        resolve(client)
      }
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

// TODO: binary? parseInt8?

util.CONFIG_KEYS = [
  'user',
  'password',
  'host',
  'port',
  'ssl',
  'rows',
  'poolSize',
  'poolIdleTimeout',
  'reapIntervalMillis'
]
