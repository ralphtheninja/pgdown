'use strict'

const inherits = require('inherits')
const AbstractLevelDOWN = require('abstract-leveldown/abstract-leveldown')
const util = require('./util')
const PgIterator = require('./pg-iterator')
const PgChainedBatch = require('./pg-chained-batch')
const debug = require('debug')('pgdown')
const debug_v = require('debug')('pgdown:verbose')

module.exports = PgDOWN

inherits(PgDOWN, AbstractLevelDOWN)
function PgDOWN (location) {
  if (!(this instanceof PgDOWN)) {
    return new PgDOWN(location)
  }

  AbstractLevelDOWN.call(this, location)
  debug('# new PgDOWN (location = %j)', location)

  this._config = util.parseConfig(location)
  debug('pg config: %j', this._config)

  this._table = this._config._table
  const table = util.escapeIdentifier(this._table)
  const schema = util.escapeIdentifier(this._schema)

  // set relation name using (assuming pgdown as schema name)
  const rel = this._rel = schema + '.' + table

  this._sql_insert = `INSERT INTO ${rel} (key,value) VALUES($1,$2)`
  this._sql_update = `UPDATE ${rel} SET value=($2) WHERE key=($1)`

  this._sql_get = `SELECT value FROM ${rel} WHERE (key)=$1`
  this._sql_put = this._sql_insert + ' ON CONFLICT (key) DO UPDATE SET value=excluded.value'
  this._sql_del = `DELETE FROM ${rel} WHERE (key) = $1`
}

const proto = PgDOWN.prototype

proto._serializeKey = function (key) {
  debug_v('## _serializeKey (key = %j)', key)
  return util.serialize(key)
}

proto._serializeValue = function (value) {
  debug_v('## _serializeValue (value = %j)', value)
  return util.serialize(value)
}

proto._deserializeKey = function (key, asBuffer) {
  debug_v('## _deserializeKey (key = %j, asBuffer = %j)', key)
  return util.deserialize(key, asBuffer)
}

proto._deserializeValue = function (value, asBuffer) {
  debug_v('## _deserializeValue (value = %j, asBuffer = %j)', value)
  return util.deserialize(value, asBuffer)
}

proto._schema = 'pgdown'

proto._open = function (options, cb) {
  debug('## _open (options = %j, cb)', options)

  const pool = this._pool = util.createPool(this._config)

  const createIfMissing = options.createIfMissing
  const errorIfExists = options.errorIfExists
  const IF_NOT_EXISTS = errorIfExists ? '' : 'IF NOT EXISTS'

  const table = this._table
  const schema = this._schema
  const rel = this._rel

  // always create pgdown schema
  pool.query(`CREATE SCHEMA IF NOT EXISTS ${util.escapeIdentifier(schema)}`, info)

  function info (err) {
    if (err) return cb(err)

    pool.query(`
      SELECT tablename FROM pg_tables WHERE schemaname=$1 AND tablename=$2
    `, [ schema, table ], (err, result) => {
      const exists = result && result.rowCount === 1

      if (errorIfExists && exists) {
        err = new Error('table exists: ' + table)
      } else if (!createIfMissing && !exists) {
        err = new Error('table does not exist')
      }

      if (err || !createIfMissing) {
        cb(err || null)
      } else {
        create()
      }
    })
  }

  function create () {
    // TODO: support for jsonb, bytea using serialize[Key|Value]
    const kType = 'bytea'
    const vType = 'bytea'
    pool.query(`
      CREATE TABLE ${IF_NOT_EXISTS} ${rel} (
        key ${kType} PRIMARY KEY,
        value ${vType}
      );
    `, (err) => {
      debug('_open: query result %j', err)

      cb(err || null)
    })
  }
}

proto._close = function (cb) {
  debug('## _close (cb)')
  if (this._pool) {
    util.destroyPool(this._pool, cb)
    this._pool = null
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

  const head = `SELECT sum(pg_column_size(tbl)) as size FROM ${this._rel} as tbl`
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
