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

  // TODO: move all this to a lib
  const SQL = this._statements = {}

  SQL.__insert = `INSERT INTO ${qname} (key,value) VALUES($1,$2)`
  SQL.__update = `UPDATE ${qname} SET value=($2) WHERE key=($1)`

  SQL._get = `SELECT value FROM ${qname} WHERE (key)=$1`
  SQL._put = SQL.__insert + ' ON CONFLICT (key) DO UPDATE SET value=excluded.value'
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

  util.connect(this).then((client) => {
    client._exec(text, [], (err, rows) => {
      debug('_open: query result %j %j', err, rows)
      client.release(err)

      if (err) {
        cb(err)
      } else if (errorIfExists && !createIfMissing) {
        cb(new Error('table exists: ' + qname))
      } else {
        cb()
      }
    })
  })
  .catch((err) => {
    debug('_open: error: %j', err)
    cb(err)
  })
}

PgDOWN.prototype._close = function (cb) {
  debug('## _close (cb)')

  util.destroyPool(this._pool, (err) => {
    if (err) return cb(err)

    // remove pool reference from db
    this._pool = null
    cb()
  })
}

PgDOWN.prototype._prepareStatement = function (method, values) {
  const text = this._statements[method]
  if (!text) throw new Error('no statement for method: ' + method)

  const statement = {}
  statement.name = 'pgdown##' + method
  statement.text = text
  statement.values = values
  return statement
}

PgDOWN.prototype._get = function (key, options, cb) {
  debug('## _get (key = %j, options = %j, cb)', key, options)

  const statement = this._prepareStatement('_get', [ key ])

  util.connect(this).then((client) => {
    // TODO: actually send statement properly
    client._exec(statement.text, statement.values, (err, rows) => {
      debug('_get: query result %j %j', err, rows)
      client.release(err)

      if (err) {
        cb(err)
      } else if (!rows || rows.length > 1) {
        cb(new Error('unexpected result for key: ' + key))
      } else if (!rows.length) {
        cb(new util.NotFoundError('not found: ' + key))
      } else {
        cb(null, this._deserializeValue(rows[0].value, options.asBuffer))
      }
    })
  })
  .catch((err) => {
    debug('_get: error: %j', err)
    cb(err)
  })
}

PgDOWN.prototype._put = function (key, value, options, cb) {
  debug('## _put (key = %j, value = %j, options = %j, cb)', key, value, options)
  this._batch([{ type: 'put', key: key, value: value, options: options }], {}, cb)
}

PgDOWN.prototype._del = function (key, options, cb) {
  debug('## _del (key = %j, options = %j, cb)', key, options)
  this._batch([{ type: 'del', key: key, options: options }], {}, cb)
}

PgDOWN.prototype._batch = function (ops, options, cb) {
  var batch = this._chainedBatch()

  ops.forEach((op) => {
    // TODO: merge op.options with batch options?
    if (op.type === 'del') {
      batch._del(op.key, op.options)
    } else if (op.type === 'put') {
      batch._put(op.key, op.value, op.options)
    } else {
      debug('_batch: unknown operation: %j', op)
    }
  })

  batch._write(cb)
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
  const values = context.values

  util.connect(this).then((client) => {
    client._exec(text, values, (err, rows) => {
      debug('_approximateSize: query result %j %j', err, rows)
      client.release(err)
      const size = Number(rows[0] && rows[0].size)
      if (isNaN(size)) {
        cb(new Error('failed to calculate approximate size'))
      } else {
        cb(null, size)
      }
    })
  })
  .catch((err) => {
    debug('_approximateSize: error: %j', err)
    cb(err)
  })
}

module.exports = PgDOWN
