'use strict'

const inherits = require('inherits')
const AbstractIterator = require('abstract-leveldown/abstract-iterator')
const util = require('./util')
const debug = require('debug')('pgdown')
const debug_v = require('debug')('pgdown:verbose')

function PgIterator (db, options) {
  debug('# new PgIterator (db, options = %j)', options)

  AbstractIterator.call(this, db)

  this._keyAsBuffer = options.keyAsBuffer
  this._valueAsBuffer = options.valueAsBuffer
  this._hasEnded = false
  this._error = null

  const context = this._parseOptions(options)
  this._stream = db._pool.query(context.text, context.values)

  this._stream.on('end', () => {
    this._hasEnded = true
    this._check()
  })

  this._stream.on('error', () => {
    this._error = err
    this._check()
  })
}

inherits(PgIterator, AbstractIterator)

PgIterator._comparators = util.comparators

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

PgIterator.prototype._next = function (cb) {
  debug_v('# PgIterator _next (cb)')
  this._cb = cb
  this._check()
}

PgIterator.prototype._check = function (cb) {
  if (this._error) return setImmediate(this._cb, this._error)

  if (this._hasEnded) return setImmediate(() => this._cb())

  var row = this._stream.read()

  if (row != null) {
    const key = this.db._deserializeKey(row.key, this._keyAsBuffer)
    const value = this.db._deserializeValue(row.value, this._valueAsBuffer)
    return setImmediate(this._cb, null, key, value)
  }

  this._stream.once('readable', () => { this._check() })
}

module.exports = PgIterator
