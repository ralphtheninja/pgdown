'use strict'

const inherits = require('inherits')
const Cursor = require('pg-cursor')
const AbstractIterator = require('abstract-leveldown/abstract-iterator')
const util = require('./util')
const debug = require('debug')('pgdown')
const debugv = require('debug')('pgdown:verbose')

function PgIterator (db, options) {
  debug('# PgIterator (db, options = %j)', options)

  AbstractIterator.call(this, db)

  this._keyAsBuffer = options.keyAsBuffer
  this._valueAsBuffer = options.valueAsBuffer

  const params = this._params = []
  const clauses = this._clauses = []
  clauses.push(`SELECT key::bytea, value::bytea FROM ${db._qname}`)

  this._processRange(options)

  clauses.push('ORDER BY key ' + (options.reverse ? 'DESC' : 'ASC'))

  const limit = this._limit = options.limit
  if (limit >= 0) {
    params.push(limit)
    clauses.push('LIMIT $' + params.length)
  }

  // TODO: any reason not to add this?
  // if (options.offset > 0) {
  //   params.push(options.offset)
  //   clauses.push('OFFSET $' + params.length)
  // }

  const command = this._clauses.join(' ')
  debug('# PgIterator command %s %j', command, params)

  this._cursorCommand = new Cursor(command, params)

  this._client = db._connect()

  // ensure cleanup for initialization errors
  this._client.catch((err) => {
    debug('_iterator initialization error: %j', err)
    this._cleanup(err)
  })
}

inherits(PgIterator, AbstractIterator)

PgIterator._comparators = {
  eq: () => '=',
  ne: () => '<>',
  lt: () => '<',
  lte: () => '<=',
  min: () => '<=',
  gt: () => '>',
  gte: () => '>=',
  max: () => '>=',
  start: (range) => range.reverse ? '<=' : '>=',
  end: (range) => range.reverse ? '>=' : '<=',
}

PgIterator.prototype._processRange = function (range) {
  const params = this._params
  const clauses = this._clauses
  clauses.push('WHERE')

  for (var k in range) {
    const v = range[k]
    const comp = PgIterator._comparators[k]
    const op = comp && comp(range)
    if (op && v) {
      params.push(util.serializeKey(v))
      clauses.push(`(key) ${op} ($${params.length})`)
      clauses.push('AND')
    } else {
      // throw on unknown?
    }
  }

  // drop the trailing clause
  clauses.pop()
}

PgIterator.prototype._windowSize = 100


PgIterator.prototype._write = function (row, cb) {
  const key = util.deserializeKey(row.key, this._keyAsBuffer)
  const value = util.deserializeValue(row.value, this._valueAsBuffer)

  debugv('iterator write - row: %j, key: %j, value: %j', row, key, value)
  cb(null, key, value)
}

PgIterator.prototype._next = function (cb) {
  debugv('# PgIterator _next (cb)')
  
  this._client.then((client) => {
    const nextRow = this._rows && this._rows.shift()
    if (nextRow) return this._write(nextRow, cb)

    // create query from compiled cursor if not already available
    if (!this._cursor) {
      this._cursor = client.query(this._cursorCommand)
    }

    this._cursor.read(this._windowSize, (err, rows) => {
      if (err) return cb(err)

      this._rows = rows
      const firstRow = rows.shift()
      firstRow ? this._write(firstRow, cb) : cb()
    })
  })
  .catch((err) => this._cleanup(err, cb))
}

PgIterator.prototype._end = function (cb) {
  debug('# PgIterator _end (cb)')

  this._cleanup(null, cb)
}

PgIterator.prototype._cleanup = function (err, cb) {
  const result = this._client.then((client) => {
    client.release(err)

    if (this._cursor) {
      this._cursor.close(() => {
        debugv('_iterator: cursor closed')
      })
      this._cursor = null
    }

    if (cb) cb(err || null)
  })

  if (cb) result.catch(cb)
}

module.exports = PgIterator
