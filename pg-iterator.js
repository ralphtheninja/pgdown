'use strict'

const inherits = require('inherits')
const AbstractIterator = require('abstract-leveldown/abstract-iterator')
const util = require('./util')
const debug = require('debug')('pgdown:info')
const debug_v = require('debug')('pgdown:verbose')

module.exports = PgIterator

inherits(PgIterator, AbstractIterator)
function PgIterator (db, options) {
  debug('# new PgIterator (db, options = %j)', options)

  AbstractIterator.call(this, db)

  this._keyAsBuffer = options.keyAsBuffer
  this._valueAsBuffer = options.valueAsBuffer

  const statement = PgIterator._parseOptions(db, options)
  const ident = db._config._identifier
  const head = `
    SELECT key::${db._keyDataType}, value::${db._valueDataType} FROM ${ident}
  `

  statement.clauses.unshift(head)
  statement.text = statement.clauses.join(' ')

  this._cursor = util.createCursor(db, statement)
}

PgIterator._comparators = util.comparators

PgIterator._parseOptions = function (db, options) {
  const context = {}
  const clauses = context.clauses = context.clauses || []
  const values = context.values = context.values || []
  PgIterator._parseRange(db, options, context)

  if (options.reverse != null) {
    clauses.push('ORDER BY key ' + (options.reverse ? 'DESC' : 'ASC'))
  }

  if (options.limit != null && options.limit >= 0) {
    values.push(options.limit)
    clauses.push('LIMIT $' + values.length)
  }

  if (options.offset > 0) {
    values.push(options.offset)
    clauses.push('OFFSET $' + values.length)
  }

  return context
}

PgIterator._parseRange = function (db, range, context) {
  const clauses = context.clauses
  const values = context.values

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

PgIterator.prototype._batchSize = 100

PgIterator.prototype._next = function (cb) {
  debug_v('# PgIterator _next (cb)')

  const nextRow = this._rows && this._rows.shift()
  if (nextRow) return this._send(nextRow, cb)

  this._cursor.read(this._batchSize, (err, rows) => {
    if (err) return cb(err)

    this._rows = rows
    this._send(rows.shift(), cb)
  })
}

PgIterator.prototype._end = function (cb) {
  debug_v('# PgIterator _end (cb)')
  this._cursor.close(cb)
}

PgIterator.prototype._send = function (row, cb) {
  if (!row) return process.nextTick(cb)

  const db = this.db
  const key = db._deserializeKey(row.key, this._keyAsBuffer)
  const value = db._deserializeValue(row.value, this._valueAsBuffer)

  cb(null, key, value)
}
