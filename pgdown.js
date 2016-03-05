'use strict'

const inherits = require('inherits')
const AbstractLevelDOWN = require('abstract-leveldown/abstract-leveldown')
const util = require('./util')
const PgIterator = require('./pg-iterator')
const PgChainedBatch = require('./pg-chained-batch')
const debug = require('debug')('pgdown')
const debug_v = require('debug')('pgdown:verbose')

function PgDOWN (location) {
  if (!(this instanceof PgDOWN)) {
    return new PgDOWN(location)
  }

  AbstractLevelDOWN.call(this, location)
  debug('# new PgDOWN (location = %j)', location)

  this._config = util.parseConfig(location)
  debug('pg config: %j', this._config)

  // set qualified name
  const qname = this._qname = util.escapeIdentifier(this._config._table)
  // TODO: if (schema) qname = schema + '.' + table, escaped

  this._sql_insert = `INSERT INTO ${qname} (key,value) VALUES($1,$2)`
  this._sql_update = `UPDATE ${qname} SET value=($2) WHERE key=($1)`

  this._sql_get = `SELECT value FROM ${qname} WHERE (key)=$1`
  this._sql_put = this._sql_insert + ' ON CONFLICT (key) DO UPDATE SET value=excluded.value'
  this._sql_del = `DELETE FROM ${qname} WHERE (key) = $1`
}

inherits(PgDOWN, AbstractLevelDOWN)

PgDOWN.prototype._serializeKey = function (key) {
  debug_v('## _serializeKey (key = %j)', key)
  return util.serialize(key)
}

PgDOWN.prototype._serializeValue = function (value) {
  debug_v('## _serializeValue (value = %j)', value)
  return util.serialize(value)
}

PgDOWN.prototype._deserializeKey = function (key, asBuffer) {
  debug_v('## _deserializeKey (key = %j, asBuffer = %j)', key)
  return util.deserialize(key, asBuffer)
}

PgDOWN.prototype._deserializeValue = function (value, asBuffer) {
  debug_v('## _deserializeValue (value = %j, asBuffer = %j)', value)
  return util.deserialize(value, asBuffer)
}

PgDOWN.prototype._open = function (options, cb) {
  debug('## _open (options = %j, cb)', options)

  this._pool = util.createPool(this._config)

  const createIfMissing = options.createIfMissing
  const errorIfExists = options.errorIfExists
  const IF_NOT_EXISTS = errorIfExists ? '' : 'IF NOT EXISTS'
  const qname = this._qname
  var text = ''

  if (errorIfExists || !createIfMissing) {
    // TODO: find a cleaner way to do this (e.g. pg_class, pg_namespace tables)
    text += `
      SELECT COUNT(*) from ${qname} LIMIT 1;
    `
  }

  // create associated schema along w/ table, if specified
  // const schema = util.escapeIdentifier(this._config._schema)
  // if (createIfMissing && schema) {
  //   text += `
  //     CREATE SCHEMA ${IF_NOT_EXISTS} ${util.escapeIdentifier(this._schema)};
  //   `
  // }

  if (createIfMissing) {
    // TODO: support for jsonb, bytea using serialize[Key|Value]
    const kType = 'bytea'
    const vType = 'bytea'
    text += `
      CREATE TABLE ${IF_NOT_EXISTS} ${qname} (
        key ${kType} PRIMARY KEY,
        value ${vType}
      );
    `
  }

  this._pool.query(text, (err) => {
    debug('_open: query result %j', err)

    if (err) {
      cb(err)
    } else if (errorIfExists && !createIfMissing) {
      cb(new Error('table exists: ' + qname))
    } else {
      cb()
    }
  })
}

PgDOWN.prototype._close = function (cb) {
  debug('## _close (cb)')
  this._pool.close(cb)
}

PgDOWN.prototype._get = function (key, options, cb) {
  debug('## _get (key = %j, options = %j, cb)', key, options)

  this._pool.query(this._sql_get, [ key ], (err, result) => {
    debug('_get: query result %j %j', err, result)

    if (err) {
      cb(err)
    } else if (result.rowCount === 1) {
      cb(null, this._deserializeValue(result.rows[0].value, options.asBuffer))
    } else if (result.rowCount === 0) {
      cb(new util.NotFoundError('not found: ' + key))
    } else {
      cb(new Error('unexpected result for key: ' + key))
    }
  })
}

PgDOWN.prototype._put = function (key, value, options, cb) {
  debug('## _put (key = %j, value = %j, options = %j, cb)', key, value, options)
  this._pool.query(this._sql_put, [ key, value ], (err) => cb(err || null))
}

PgDOWN.prototype._del = function (key, options, cb) {
  debug('## _del (key = %j, options = %j, cb)', key, options)
  this._pool.query(this._sql_del, [ key ], (err) => cb(err || null))
}

PgDOWN.prototype._batch = function (ops, options, cb) {
  const tx = util.createTransaction(this._pool)

  ops.forEach((op) => {
    // TODO: merge op.options with batch options?
    if (op.type === 'put') {
      tx.query(this._sql_put, [ op.key, op.value ])
    } else if (op.type === 'del') {
      tx.query(this._sql_del, [ op.key ])
    } else {
      debug('_batch: unknown operation %j', op)
    }
  })

  tx.commit((err) => cb(err || null))
}

PgDOWN.prototype._chainedBatch = function () {
  debug('## _chainedBatch ()')
  return new PgChainedBatch(this)
}

PgDOWN.prototype._iterator = function (options) {
  debug('## _iterator (options = %j)', options)
  return new PgIterator(this, options)
}

PgDOWN.prototype._approximateSize = function (start, end, cb) {
  const options = { start: start, end: end }
  // generate standard iterator sql and replace head clause
  const context = PgIterator._parseRange(this, options)

  const head = `SELECT sum(pg_column_size(tbl)) as size FROM ${this._qname} as tbl`
  context.clauses.unshift(head)
  const text = context.clauses.join(' ')

  this._pool.query(text, context.values, (err, result) => {
    debug('_approximateSize: query result %j %j', err, result)
    if (err) return cb(err)

    const row = Number(result.rows[0])
    const size = result.rowCount && Number(result.rows[0].size)
    if (result.rowCount === 1 && !isNaN(size)) {
      cb(null, size)
    } else {
      cb(new Error('failed to calculate approximate size'))
    }
  })
}

module.exports = PgDOWN
