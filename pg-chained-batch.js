'use strict'

const inherits = require('inherits')
const AbstractChainedBatch = require('abstract-leveldown/abstract-chained-batch')
const util = require('./util')
const debug = require('debug')('pgdown')
const debug_v = require('debug')('pgdown:verbose')

function PgChainedBatch (db) {
  debug('# new PgChainedBatch (db)')

  AbstractChainedBatch.call(this, db)

  this._client = this._begin()
}

inherits(PgChainedBatch, AbstractChainedBatch)

PgChainedBatch.prototype._begin = function () {
  const client = util.connect(this._db).then((client) => {
    this._db._pool.on('destroy', () => {
      client._exec('ROLLBACK', (err) => client.release(err))
    })

    client._exec('BEGIN', (err) => {
      if (err || !this.error) this.error = err
    })
    return client
  })

  // ensure cleanup for initialization errors
  client.catch((err) => {
    debug('batch initialization error %j', err)
    this._cleanup(err)
  })

  return client
}

PgChainedBatch.prototype._put = function (key, value) {
  debug_v('# PgChainedBatch _put (key = %j, value = %j)', key, value)

  const statement = this._db._prepareStatement('_put', [ key, value ])
  this._client.then((client) => {
    client._exec(statement.text, statement.values, (err) => {
      this._error = this._error || err
    })
  })
  .catch((err) => this._cleanup(err))
}

PgChainedBatch.prototype._del = function (key) {
  debug_v('# PgChainedBatch _del (key = %j)', key)

  const statement = this._db._prepareStatement('_del', [ key ])
  this._client.then((client) => {
    client._exec(statement.text, statement.values, (err) => {
      this._error = this._error || err
    })
  })
  .catch((err) => this._cleanup(err))
}

PgChainedBatch.prototype._clear = function () {
  debug('# PgChainedBatch _clear ()')

  this._client.then((client) => {
    // abort existing transaction and start a fresh one
    client._exec('ROLLBACK; BEGIN', (err) => {
      this._error = this._error || err
    })
  })
  .catch((err) => this._cleanup(err))
}

PgChainedBatch.prototype._write = function (cb) {
  debug('# PgChainedBatch _write (cb)')

  this._client.then((client) => {
    const action = this._error ? 'ROLLBACK' : 'COMMIT'
    client._exec(action, (err) => this._cleanup(err, cb))
  })
  .catch((err) => this._cleanup(err, cb))
}

PgChainedBatch.prototype._cleanup = function (err, cb) {
  this._client.then((client) => {
    client.release(this._error || err)
    if (cb) cb(this._error || err || null)
  })
  .catch(cb || ((err) => {
    if (!this._error) this._error = err
  }))
}

module.exports = PgChainedBatch
