const Cursor = require('pg-cursor')
const inherits = require('inherits')
const AbstractIterator = require('abstract-leveldown/abstract-iterator')
const debug = require('debug')('pgdown')

// TODO: sanitization
function _constraintSql (constraints) {
  // handle `or` clauses
  if (Array.isArray(constraints)) {
    return '(' + constraints.map(_constraintSql).join(') OR (') + ')'
  }

  const clauses = []
  const ops = PgIterator._comparators
  for (var k in constraints) {
    const v = constraints[k]
    const op = ops[k]
    if (op) {
      clauses.push(`(key) ${op} (${v})`)
    } else if (op === 'or') {
      // TODO: just being lazy, but should fix up extra array wrapping cruft
      clauses.push(_constraintSql([ constraints[k] ]))
    }
  }

  return clauses.filter(Boolean).join(' AND ')
}

function PgIterator (db, options) {
  AbstractIterator.call(this, db)

  this._keyAsBuffer = options.keyAsBuffer
  this._valueAsBuffer = options.valueAsBuffer
  this._pool = db._pool

  const params = this._params = []
  const clauses = []
  clauses.push(`SELECT key, value FROM ${db._qname}`)

  const constraints = _constraintSql(options)
  if (constraints) {
    params.push(constraints)
    clauses.push('WHERE $' + params.length)
  }

  clauses.push('ORDER BY key ' + (options.reverse ? 'DESC' : 'ASC'))

  const limit = options.limit
  if (limit >= 0) {
    params.push(limit)
    clauses.push('LIMIT $' + params.length)
  }

  // TODO: any reason not to add this?
  // if (options.offset > 0) {
  //   params.push(options.offset)
  //   clauses.push('OFFSET $' + params.length)
  // }

  const sql = this._command = clauses.join(' ')
  debug('_iterator: sql: %s %j', sql, params)
}

inherits(PgIterator, AbstractIterator)

PgIterator._comparators = {
  lt: '<',
  lte: '<=',
  gte: '>=',
  gt: '>',
  eq: '=',
  ne: '<>'
}

PgIterator._deserialize = (source, asBuffer) => {
  return asBuffer ? source : String(source || '')
}

PgIterator.prototype._windowSize = 100

PgIterator.prototype._write = function (row, cb) {
  const key = PgIterator._deserialize(row.key, this._keyAsBuffer)
  const value = PgIterator._deserialize(row.key, this._valueAsBuffer)
  cb(null, key, value)
}

PgIterator.prototype._next = function (cb) {
  const client = this._client
  if (!client) {
    return this._pool.acquire((err, client) => {
      if (err) return this._close(err, cb)

      this._client = client
      this._next(cb)
    })
  }

  if (this._cursor) {
    const nextRow = this._rows && this._rows.shift()
    if (nextRow) return this._write(nextRow, cb)
  } else {
    this._cursor = this._client.query(new Cursor(this._command, this._params))
  }

  this._cursor.read(this._windowSize, (err, rows) => {
    if (err) return this._close(err, cb)

    this._rows = rows
    const firstRow = rows.shift()
    firstRow ? this._write(firstRow, cb) : this._close(null, cb)
  })
}

PgIterator.prototype._end = function (cb) {
  debug('# PgIterator _end (cb = %s)', !!cb)
  this._close(null, cb)
}

PgIterator.prototype._close = function (err, cb) {
  // TODO: close cursor?
  if (err) {
    this._pool.destroy(this._client)
    cb(err)
  } else {
    this._pool.release(this._client)
    cb()
  }
}

module.exports = PgIterator
