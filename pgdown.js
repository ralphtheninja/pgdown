'use strict'

const inherits = require('inherits')
const AbstractLevelDOWN = require('abstract-leveldown/abstract-leveldown')
const util = require('./util')
const PgIterator = require('./pg-iterator')
const PgChainedBatch = require('./pg-chained-batch')
const debug = require('debug')('pgdown:info')
const debug_v = require('debug')('pgdown:verbose')

module.exports = PgDOWN

inherits(PgDOWN, AbstractLevelDOWN)
function PgDOWN (location) {
  if (!(this instanceof PgDOWN)) {
    return new PgDOWN(location)
  }

  AbstractLevelDOWN.call(this, location)
  debug('# new PgDOWN (location = %j)', location)

  this._config = util.parseLocation(location)
  debug('pg config: %j', this._config)

  const ident = this._config._identifier

  this._sql_insert = `INSERT INTO ${ident} (key,value) VALUES($1,$2)`
  this._sql_update = `UPDATE ${ident} SET value=($2) WHERE key=($1)`

  this._sql_get = `SELECT value FROM ${ident} WHERE (key)=$1`
  this._sql_put = this._sql_insert + ' ON CONFLICT (key) DO UPDATE SET value=excluded.value'
  this._sql_del = `DELETE FROM ${ident} WHERE (key) = $1`
}

const proto = PgDOWN.prototype

proto._serializeKey = function (key) {
  debug_v('## _serializeKey (key = %j)', key)
  return util.serialize(this._keyDataType, key)
}

proto._serializeValue = function (value) {
  debug_v('## _serializeValue (value = %j)', value)
  return util.serialize(this._valueDataType, value)
}

proto._deserializeKey = function (key, asBuffer) {
  debug_v('## _deserializeKey (key = %j, asBuffer = %j)', key)
  return util.deserialize(this._keyDataType, key, asBuffer)
}

proto._deserializeValue = function (value, asBuffer) {
  debug_v('## _deserializeValue (value = %j, asBuffer = %j)', value)
  return util.deserialize(this._valueDataType, value, asBuffer)
}

proto._keyDataType = 'bytea'

proto._valueDataType = 'bytea'

proto._open = function (options, cb) {
  debug('## _open (options = %j, cb)', options)

  const pool = this._pool = util.createPool(this._config)

  const createIfMissing = options.createIfMissing
  const errorIfExists = options.errorIfExists
  const IF_NOT_EXISTS = errorIfExists ? '' : 'IF NOT EXISTS'

  const table = this._config._tablePath
  const ident = this._config._identifier

  // TODO: move to helper methods
  const kEnc = options.keyEncoding
  if (kEnc === 'utf8') {
    this._keyDataType = 'text'
  } else if (kEnc === 'json') {
    this._keyDataType = 'jsonb'
  }

  const vEnc = options.valueEncoding
  if (vEnc === 'utf8') {
    this._valueDataType = 'text'
  } else if (vEnc === 'json') {
    this._valueDataType = 'jsonb'
  }

  debug('column types: key %j, value %j', this._keyDataType, this._valueDataType)

  // always create pgdown schema
  pool.query(`
    CREATE SCHEMA IF NOT EXISTS ${util.escapeIdentifier(util.schemaName)}
  `, (err) => err ? fail(err) : info())

  const info = () => {
    pool.query(`
      SELECT tablename FROM pg_tables WHERE schemaname=$1 AND tablename=$2
    `, [ util.schemaName, table ], (err, result) => {
      const exists = result && result.rowCount === 1

      if (errorIfExists && exists) {
        err = new Error('table already exists: ' + table)
      } else if (!createIfMissing && !exists) {
        err = new Error('table does not exist: ' + table)
      }

      if (err) {
        fail(err)
      } else if (createIfMissing) {
        create()
      } else {
        cb()
      }
    })
  }

  const create = () => {
    // TODO: support for jsonb, bytea using serialize[Key|Value]
    pool.query(`
      CREATE TABLE ${IF_NOT_EXISTS} ${ident} (
        key ${this._keyDataType} PRIMARY KEY,
        value ${this._valueDataType}
      )
    `, (err) => {
      debug('_open: query result %j', err)

      err ? fail(err) : cb()
    })
  }

  const fail = (err) => {
    this._pool = null
    util.destroyPool(pool, (err_) => {
      if (err_) debug('failed to destroy pool on open err %j', err_)
      cb(err)
    })
  }
}

proto._close = function (cb) {
  debug('## _close (cb)')

  const pool = this._pool
  if (pool) {
    this._pool = null
    util.destroyPool(pool, cb)
  } else {
    process.nextTick(cb)
  }
}

proto._get = function (key, options, cb) {
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

proto._put = function (key, value, options, cb) {
  debug('## _put (key = %j, value = %j, options = %j, cb)', key, value, options)
  const batch = [{ type: 'put', key: key, value: value }]
  this._batch(batch, options, (err) => cb(err || null))
}

proto._del = function (key, options, cb) {
  debug('## _del (key = %j, options = %j, cb)', key, options)
  const batch = [{ type: 'del', key: key }]
  this._batch(batch, options, (err) => cb(err || null))
}

proto._batch = function (ops, options, cb) {
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

proto._chainedBatch = function () {
  debug('## _chainedBatch ()')
  return new PgChainedBatch(this)
}

proto._iterator = function (options) {
  debug('## _iterator (options = %j)', options)
  return new PgIterator(this, options)
}

proto._approximateSize = function (start, end, cb) {
  const options = { start: start, end: end }
  // generate standard iterator sql and replace head clause
  const context = PgIterator._parseOptions(this, options)

  const ident = this._config._identifier
  const head = `SELECT sum(pg_column_size(tbl)) as size FROM ${ident} as tbl`
  context.clauses.unshift(head)
  const text = context.clauses.join(' ')

  this._pool.query(text, context.values, (err, result) => {
    debug('_approximateSize: query result %j %j', err, result)
    if (err) return cb(err)

    const size = result.rowCount && Number(result.rows[0].size)
    if (result.rowCount === 1 && !isNaN(size)) {
      cb(null, size)
    } else {
      cb(new Error('failed to calculate approximate size'))
    }
  })
}

PgDOWN.destroy = util.dropTable
