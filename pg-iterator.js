'use strict'

const inherits = require('inherits')
const Cursor = require('pg-cursor')
const AbstractIterator = require('abstract-leveldown/abstract-iterator')
const util = require('./util')
const debug = require('debug')('pgdown')
const debug_v = require('debug')('pgdown:verbose')

function PgIterator (db, options) {
  debug('# new PgIterator (db, options = %j)', options)

  AbstractIterator.call(this, db)

  this._keyAsBuffer = options.keyAsBuffer
  this._valueAsBuffer = options.valueAsBuffer

  const context = this._parseOptions(options)

  this._client = util.connect(db).then((client) => {
    this._cursor = client._exec(new Cursor(context.text, context.values))
    return client
  })

  // ensure cleanup for initialization errors
  this._client.catch((err) => {
    debug('_iterator initialization error: %j', err)
    this._cleanup(err)
  })
}

inherits(PgIterator, AbstractIterator)

PgIterator._comparators = util.comparators

PgIterator.prototype._windowSize = 100

PgIterator.prototype._parseOptions = function (options) {
  const context = {}
  const clauses = context.clauses = []
  const values = context.values = []

  clauses.push(`SELECT key::bytea, value::bytea FROM ${this.db._qname}`)

  PgIterator._parseRange(this.db, options, context)

  clauses.push('ORDER BY key ' + (options.reverse ? 'DESC' : 'ASC'))

  const limit = options.limit
  if (limit >= 0) {
    values.push(limit)
    clauses.push('LIMIT $' + values.length)
  }

  // TODO: any reason not to add this?
  // if (options.offset > 0) {
  //   values.push(options.offset)
  //   clauses.push('OFFSET $' + values.length)
  // }

  context.text = context.clauses.join(' ')
  return context
}

PgIterator._parseRange = function (db, range, context) {
  context = context || {}
  const clauses = context.clauses = context.clauses || []
  const values = context.values = context.values || []

  clauses.push('WHERE')

  for (var k in range) {
    const v = range[k]
    const comp = PgIterator._comparators[k]
    const op = comp && comp(range)
    if (op && v) {
      values.push(db._serializeKey(v))
      clauses.push(`(key) ${op} ($${values.length})`)
      clauses.push('AND')
    } else {
      // throw on unknown?
    }
  }

  // drop the trailing clause
  clauses.pop()

  return context
}

PgIterator.prototype._send = function (row, cb) {
  const db = this.db
  const key = db._deserializeKey(row.key, this._keyAsBuffer)
  const value = db._deserializeValue(row.value, this._valueAsBuffer)

  cb(null, key, value)
}

PgIterator.prototype._next = function (cb) {
  debug_v('# PgIterator _next (cb)')

  this._client.then((client) => {
    const nextRow = this._rows && this._rows.shift()
    if (nextRow) return this._send(nextRow, cb)

    this._cursor.read(this._windowSize, (err, rows) => {
      if (err) return cb(err)

      this._rows = rows
      const firstRow = rows.shift()
      firstRow ? this._send(firstRow, cb) : cb()
    })
  })
  .catch((err) => this._cleanup(err, cb))
}

PgIterator.prototype._end = function (cb) {
  debug_v('# PgIterator _end (cb)')

  this._cleanup(null, cb)
}

PgIterator.prototype._cleanup = function (err, cb) {
  const result = this._client.then((client) => {
    if (this._finalized) {
      client.release(err)
    } else {
      this._finalized = true
      this._cursor.close(() => {
        debug_v('_iterator: cursor closed')
        client.release(err)
      })
    }

    if (cb) err ? cb(err) : cb()
  })

  if (cb) result.catch(cb)
}

module.exports = PgIterator
