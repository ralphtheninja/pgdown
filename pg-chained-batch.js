'use strict'

const inherits = require('inherits')
const AbstractChainedBatch = require('abstract-leveldown/abstract-chained-batch')
const util = require('./util')
const debug = require('debug')('pgdown')
const debug_v = require('debug')('pgdown:verbose')

function PgChainedBatch (db) {
  debug('# new PgChainedBatch (db)')

  AbstractChainedBatch.call(this, db)

  this._qname = db._qname

  this._client = this._begin()
}

inherits(PgChainedBatch, AbstractChainedBatch)

PgChainedBatch.prototype._begin = function () {
  const client = util.connect(this._db).then((client) => {
    client._exec('BEGIN')
    return client
  })

  // ensure cleanup for initialization errors
  client.catch((err) => {
    debug('_chainedBatch initialization error: %j', err)
    if (this._client === client) this._cleanup(err)
  })

  return client
}

PgChainedBatch.prototype._put = function (key, value) {
  debug_v('# PgChainedBatch _put (key = %j, value = %j)', key, value)

  const statement = this._db._prepareStatement('_put', [ key, value ])
  this._client.then((client) => {
    client._exec(statement)
  })
  .catch((err) => this._cleanup(err))
}

PgChainedBatch.prototype._del = function (key) {
  debug_v('# PgChainedBatch _del (key = %j)', key)

  const statement = this._db._prepareStatement('_del', [ key ])
  this._client.then((client) => {
    client._exec(statement)
  })
  .catch((err) => this._cleanup(err))
}

PgChainedBatch.prototype._clear = function () {
  debug('# PgChainedBatch _clear ()')

  // noop, for now
}

PgChainedBatch.prototype._write = function (cb) {
  debug('# PgChainedBatch _write (cb)')
  this._cb = cb
  this._client.then((client) => {
    // client.on('drain', (arg) => console.warn('COMMIT DRAIN', arg))
    client._exec('COMMIT', [])
    .on('error', () => (err) => this._cleanup(err, cb))
    .on('end', () => this._cleanup(null, cb))
  })
  .catch((err) => this._cleanup(err, cb))
}

PgChainedBatch.prototype._cleanup = function (err, cb) {
  this._client.then((client) => {
    const error = this._error || err

    client.release(error)
    if (cb) cb(error || null)
  })
  .catch(cb || ((err) => {
    if (!this._error) this._error = err
  }))
}

module.exports = PgChainedBatch
