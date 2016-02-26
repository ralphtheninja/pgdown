const inherits = require('inherits')
const Cursor = require('pg-cursor')
const AbstractIterator = require('abstract-leveldown').AbstractIterator

const debug = require('debug')('pgdown')

PgIterator.operators = {
  lt: '<',
  lte: '<=',
  gte: '>=',
  gt: '>',
  eq: '=',
  ne: '<>'
}

// TODO: sanitization
function formatConstraints (constraints) {
  // handle `or` clauses
  if (Array.isArray(constraints)) {
    return '(' + constraints.map(formatConstraints).join(') OR (') + ')'
  }

  const clauses = []
  const operators = PgIterator.operators
  for (var k in constraints) {
    const v = constraints[k]
    const op = operators[k]
    if (op) {
      clauses.push(`key${op}(${v})`)
    } else if (op === 'or') {
      // TODO: just being lazy, but should fix up extra array wrapping cruft
      clauses.push(formatConstraints([ constraints[k] ]))
    }
  }

  return clauses.filter(Boolean).join(' AND ')
}

module.exports = PgIterator

function PgIterator (db, options) {
  AbstractIterator.call(this, db)

  options = options || {}

  this._count = 0
  this._limit = isNaN(options.limit) ? -1 : options.limit
  this._reverse = !!options.reverse
  this._constraints = formatConstraints(options)

  // TODO: buffer results
  // this._highWaterMark = options.highWaterMark || db.highWaterMark || 1000
  // this._batchSize = 100
}

inherits(PgIterator, AbstractIterator)

PgIterator.prototype._ensureCursor = function (cb) {
  if (this._cursor) return cb()

  const clauses = []
  const args = []
  const table = this.db.pg.table

  clauses.push(`SELECT key, value::text FROM ${table}`)

  if (this._constraints) {
    args.push(this._constraints)
    clauses.push('WHERE $' + args.length)
  }

  clauses.push('ORDER BY key ' + (this._reverse ? 'DESC' : 'ASC'))

  if (this._limit.limit >= 0) {
    args.push(this._limit)
    clauses.push('LIMIT $' + args.length)
  }

  // TODO: any reason not to add this?
  // if (options.offset > 0) {
  //   args.push(options.offset)
  //   clauses.push('OFFSET $' + args.length)
  // }

  const sql = clauses.join(' ')
  debug('cursor sql: %s %j', sql, args)

  // create cursor
  this.db._connect((err, client, release) => {
    if (err) {
      release()
      return cb(err)
    }

    this._release = release

    debug('creating cursor')
    this._cursor = client.query(new Cursor(sql, args))
    cb()

    client.on('error', (err) => {
      // TODO: do we have to listen for this?
      // and do we need something like `release(err)`
      debug('cursor query error: %j', err)
    })
  })
}

PgIterator.prototype._next = function (cb) {
  this._ensureCursor((err) => {
    if (err) return cb(err)

    // TODO: read in batches, up to some reasonable limit, and cache
    this._cursor.read(1, (err, rows) => {
      debug('cursor result: %j %j', err, rows)

      if (err || !rows.length) return cb(err || null)

      const row = rows[0] || {}
      cb(null, row.key, row.value)
    })
  })
}

PgIterator.prototype._end = function (cb) {
  this._release && this._release()
  this._cursor && this._cursor.close(cb)
  this._cursor = this._release = null
}
