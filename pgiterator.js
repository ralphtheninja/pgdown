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

  const params = []
  const clauses = []
  clauses.push(`SELECT key, value FROM ${this._qname}`)

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

  const sql = clauses.join(' ')
  debug('_iterator: sql: %s %j', sql, params)

  this._connecting = true
  db._pool.acquire((err, client) => {
    this._connecting = false
    if (err) return (this._error = err)

    this._client = client
    this._cursor = client.query(new Cursor(sql, params))

    this._fillRowBuffer((err, key, value) => {
      // TODO: tragic...
      if (this._firstCb) this._firstCb(err, key, value)
      else this._firstV = [ err, key, value ]
    })
  })
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

PgIterator.prototype._rowBufferSize = 100

PgIterator.prototype._sendBufferedRow = function (cb) {
  // TODO: kill me... please...
  const firstV = this._firstV
  if (firstV) {
    this._firstV = null
    return cb.apply(null, firstV)
  }

  const row = this._rowBuffer.shift()
  debug('iterator - row: %j', row)

  const key = PgIterator._deserialize(row.key, this._keyAsBuffer)
  const value = PgIterator._deserialize(row.key, this._valueAsBuffer)
  debug('iterator - deserialized key: %j, value: %j', key, value)

  cb(null, key, value)
}

PgIterator.prototype._fillRowBuffer = function (cb) {
  this._cursor.read(this._rowBufferSize, (err, rows) => {
    debug('_cursor read result: %j, %j', err, rows)
    if (err) return cb(err)

    this._rowBuffer = rows
    if (rows.length) {
      this._sendBufferedRow(cb)
    } else {
      cb()
    }
  })
}

PgIterator.prototype._next = function (cb) {
  debug('# PgIterator next (cb = %s)', cb)
  if (this._error) {
    this.db._pool.destroy(this._client)
    process.nextTick(() => cb(this._error))
  } else if (this._rowBuffer && this._rowBuffer.length) {
    this._sendBufferedRow(cb)
  } else if (this._client) {
    this._fillRowBuffer(cb)
  } else {
    this._firstCb = cb
  }
}

PgIterator.prototype._end = function (cb) {
  debug('# PgIterator end (cb = %s)', cb)

  this.db._pool.destroy(this._client)
  if (this._error) {
    process.nextTick(() => cb(this._error))
  } else {
    this._cursor.close(cb)
  }
}

PgIterator._deserialize = (source, asBuffer) => {
  debug('# _deserialize (source = %j, asBuffer: %s)', source, asBuffer)

  return asBuffer ? source : String(source || '')
}

module.exports = PgIterator
