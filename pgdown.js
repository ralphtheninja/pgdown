'use strict'

const inherits = require('inherits')
const AbstractLevelDOWN = require('abstract-leveldown/abstract-leveldown')
const util = require('./util')
const PgIterator = require('./pg-iterator')
const PgChainedBatch = require('./pg-chained-batch')

module.exports = PgDOWN

inherits(PgDOWN, AbstractLevelDOWN)
function PgDOWN (location) {
  if (!(this instanceof PgDOWN)) {
    return new PgDOWN(location)
  }

  AbstractLevelDOWN.call(this, location)
  this._config = util.parseLocation(location)
}

const proto = PgDOWN.prototype

// NB: keys should *always* be stored using 'bytea'
proto._keyColumnType = 'bytea'
proto._valueColumnType = 'bytea'

proto._serializeKey = function (key) {
  return util.serialize(this._keyColumnType, key)
}

proto._serializeValue = function (value) {
  return util.serialize(this._valueColumnType, value)
}

proto._deserializeKey = function (key, asBuffer) {
  return util.deserialize(this._keyColumnType, key, asBuffer)
}

proto._deserializeValue = function (value, asBuffer) {
  return util.deserialize(this._valueColumnType, value, asBuffer)
}

// TODO: memoized getters

proto._sql_get = function (key) {
  return `
    SELECT value FROM ${this._config._relation} WHERE (key)=$1
  `
}

proto._sql_del = function (key) {
  return `
    DELETE FROM ${this._config._relation} WHERE (key)=$1
  `
}

proto._sql_insert = function () {
  return `
    INSERT INTO ${this._config._relation} (key, value) VALUES($1,$2)
  `
}

proto._sql_update = function () {
  return `
    UPDATE ${this._config._relation} SET value=($2) WHERE key=($1)
  `
}

proto._sql_put = function (key, value) {
  return this._sql_insert() + ' ON CONFLICT (key) DO UPDATE SET value=excluded.value'
}

proto._open = function (options, cb) {
  const config = this._config
  const pool = this._pool = util.createPool(config)
  // TODO: make pool init async, do create schema if not exists dance just once

  const createIfMissing = options.createIfMissing
  const errorIfExists = options.errorIfExists
  const IF_NOT_EXISTS = errorIfExists ? '' : 'IF NOT EXISTS'

  const schema = config._schema
  const table = config._table
  const relation = config._relation

  // always create pgdown schema
  pool.query(`
    CREATE SCHEMA IF NOT EXISTS ${schema}
  `, (err) => err ? fail(err) : info())

  const info = () => {
    pool.query(`
      SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname=$1 AND tablename=$2
    `, [ schema, table ], (err, result) => {
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
    // TODO: use separate column names for different value types?
    pool.query(`
      CREATE TABLE ${IF_NOT_EXISTS} ${relation} (
        key ${this._keyColumnType} PRIMARY KEY,
        value ${this._valueColumnType}
      )
    `, (err) => {
      err ? fail(err) : cb()
    })
  }

  const fail = (err) => {
    this._pool = null
    util.destroyPool(pool, (err_) => {
      cb(err)
    })
  }
}

proto._close = function (cb) {
  const pool = this._pool
  if (pool) {
    this._pool = null
    util.destroyPool(pool, cb)
  } else {
    process.nextTick(cb)
  }
}

proto._get = function (key, options, cb) {
  this._pool.query(this._sql_get(), [ key ], (err, result) => {
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
  const batch = [ { type: 'put', key: key, value: value } ]
  this._batch(batch, options, (err) => cb(err || null))
}

proto._del = function (key, options, cb) {
  const batch = [ { type: 'del', key: key } ]
  this._batch(batch, options, (err) => cb(err || null))
}

proto._batch = function (ops, options, cb) {
  const tx = util.createTransaction(this._pool, cb)

  ops.forEach((op) => {
    // TODO: merge op.options with batch options?
    if (op.type === 'put') {
      tx.query(this._sql_put(), [ op.key, op.value ])
    } else if (op.type === 'del') {
      tx.query(this._sql_del(), [ op.key ])
    }
  })

  tx.commit()
}

proto._chainedBatch = function () {
  return new PgChainedBatch(this)
}

proto._iterator = function (options) {
  return new PgIterator(this, options)
}

// NB: represents exact compressed size?
proto._approximateSize = function (start, end, cb) {
  const options = { start: start, end: end }
  // generate standard iterator sql and replace head clause
  const context = PgIterator._parseOptions(this, options)

  const relation = this._config._relation
  const head = `SELECT sum(pg_column_size(tbl)) as size FROM ${relation} as tbl`
  context.clauses.unshift(head)
  const text = context.clauses.join(' ')

  this._pool.query(text, context.values, (err, result) => {
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
