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

util.connect = function (config) {
  return new Promise((resolve, reject) => {
    pg.connect(config, (err, client, done) => {
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

util.pg = pg

util.createPool = function (config) {
  // create a unique id to keep from pissing in the connection pool on close
  config._poolId = mts()
  return pg.pools.getOrCreate(config)
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
