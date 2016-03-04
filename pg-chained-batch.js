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

  this._client = util.connect(db).then((client) => {
    client.query('BEGIN', [])
    return client
  })

  // ensure cleanup for initialization errors
  this._client.catch((err) => {
    debug('_chainedBatch initialization error: %j', err)
    this._cleanup(err)
  })
}

inherits(PgChainedBatch, AbstractChainedBatch)

PgChainedBatch.prototype._write = function (cb) {
  debug('# PgChainedBatch _write (cb)')
  this._cb = cb
  this._client.then((client) => {
    client.query('COMMIT', [], (err) => this._cleanup(err, cb))
  })
  .catch((err) => this._cleanup(err, cb))
}

PgChainedBatch.prototype._put = function (key, value) {
  debug_v('# PgChainedBatch _put (key = %j, value = %j)', key, value)

  this._client.then((client) => {
    const op = { type: 'put', key: key, value: value }
    PgChainedBatch._commands.put(client, this._qname, op)
  })
  .catch((err) => this._cleanup(err))
}

PgChainedBatch.prototype._del = function (key) {
  debug_v('# PgChainedBatch _del (key = %j)', key)

  this._client.then((client) => {
    const op = { type: 'del', key: key }
    PgChainedBatch._commands.del(client, this._qname, op)
  })
  .catch((err) => this._cleanup(err))
}

PgChainedBatch.prototype._clear = function () {
  debug('# PgChainedBatch _clear ()')
  this._client.then((client) => {
    // abort existing transaction and start a fresh one
    client.query('ROLLBACK', [])
    client.query('BEGIN', [])
  })
  .catch((err) => this._cleanup(err))
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

PgChainedBatch._commands = {}

PgChainedBatch._commands.put = function (client, qname, op, cb) {
  const INSERT = `INSERT INTO ${qname} (key,value) VALUES($1,$2)`
  const UPSERT = INSERT + ' ON CONFLICT (key) DO UPDATE SET value=excluded.value'
  // const UPDATE = `UPDATE ${qname} SET value=($2) WHERE key=($1)'`

  // always an upsert for now
  const command = UPSERT
  const params = [ op.key, op.value ]

  return client.query(command, params, cb)
}

PgChainedBatch._commands.del = function (client, qname, op, cb) {
  const command = `DELETE FROM ${qname} WHERE (key) = $1`
  const params = [ op.key ]

  return client.query(command, params, cb)
}

module.exports = PgChainedBatch
