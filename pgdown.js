'use strict'

const inherits = require('inherits')
const AbstractLevelDOWN = require('abstract-leveldown/abstract-leveldown')
const util = require('./util')
const PgIterator = require('./pg-iterator')
const PgChainedBatch = require('./pg-chained-batch')
const debug = require('debug')('pgdown')
const debugv = require('debug')('pgdown:verbose')

function PgDOWN (location) {
  if (!(this instanceof PgDOWN)) {
    return new PgDOWN(location)
  }

  AbstractLevelDOWN.call(this, location)
  debug('# new PgDOWN (location = %j)', location)

  this._config = util.config(location)
  debug('pg config: %j', this._config)

  // set qualified name
  this._qname = util.escapeIdentifier(this._config._table)
  // TODO: if (schema) qname = schema + '.' + table, escaped
}

inherits(PgDOWN, AbstractLevelDOWN)

PgDOWN.prototype._serializeKey = function (key) {
  debugv('## _serializeKey (key = %j)', key)
  return util.serializeKey(key)
}

PgDOWN.prototype._serializeValue = function (value) {
  debugv('## _serializeValue (value = %j)', value)
  return util.serializeValue(value)
}

PgDOWN.prototype._open = function (options, cb) {
  debug('## _open (options = %j, cb)', options)

  util.createPool(this)

  const createIfMissing = options.createIfMissing
  const errorIfExists = options.errorIfExists
  const IF_NOT_EXISTS = errorIfExists ? '' : 'IF NOT EXISTS'
  const qname = this._qname
  var command = ''

  if (errorIfExists || !createIfMissing) {
    // TODO: find a cleaner way to do this (e.g. pg_class, pg_namespace tables)
    command += `
      SELECT COUNT(*) from ${qname} LIMIT 1;
    `
  }

  // create associated schema along w/ table, if specified
  // const schema = util.escapeIdentifier(this._config._schema)
  // if (createIfMissing && schema) {
  //   command += `
  //     CREATE SCHEMA ${IF_NOT_EXISTS} ${util.escapeIdentifier(this._schema)};
  //   `
  // }

  if (createIfMissing) {
    // TODO: support for jsonb, bytea using serialize[Key|Value]
    const kType = 'bytea'
    const vType = 'bytea'
    command += `
      CREATE TABLE ${IF_NOT_EXISTS} ${qname} (
        key ${kType} PRIMARY KEY,
        value ${vType}
      );
    `
  }

  debug('_open: command: %s', command)
  util.connect(this).then((client) => {
    client.query(command, (err) => {
      client.release(err)

      if (!err && !createIfMissing && errorIfExists) {
        err = new Error('table exists: ' + qname)
      }

      cb(err || null)
    })
  }).catch((err) => {
    debug('_open: error: %j', err)
    cb(err)
  })
}

PgDOWN.prototype._close = function (cb) {
  debug('## _close (cb)')

  util.destroyPool(this, cb)
}

PgDOWN.prototype._get = function (key, options, cb) {
  debug('## _get (key = %j, options = %j, cb)', key, options)

  // TODO: most efficient way to disable jsonb field parsing in pg lib?
  const command = `SELECT value FROM ${this._qname} WHERE (key)=$1`
  const params = [ key ]
  debug('_get: command %s %j', command, params)

  util.connect(this).then((client) => {
    var result, rowErr
    client.query(command, params)
    .on('error', (err) => {
      client.release(err)

      debug('_get: query error: %j', err)
    }).on('end', () => {
      client.release()

      debug('_get: query end')

      if (rowErr) {
        cb(rowErr)
      } else if (result) {
        cb(null, util.deserializeValue(result.value, options.asBuffer))
      } else {
        cb(new util.NotFoundError('not found: ' + key))
      }
    }).on('row', (row) => {
      debug('_get: row %j', row)
      if (result) {
        rowErr = new Error('expected unique value for key: ' + key)
      } else {
        result = row
      }
    })
  }).catch((err) => {
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

PgDOWN.prototype.approximateSize = function () {
  throw new Error('NYI')

  // const command = `SELECT sum(pg_column_size(table)) FROM ${table} as table WHERE ...`
}

module.exports = PgDOWN
