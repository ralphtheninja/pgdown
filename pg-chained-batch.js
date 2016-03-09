'use strict'

const inherits = require('inherits')
const AbstractChainedBatch = require('abstract-leveldown/abstract-chained-batch')
const util = require('./util')
const debug = require('debug')('pgdown:info')
// const debug_v = require('debug')('pgdown:verbose')

module.exports = PgChainedBatch

inherits(PgChainedBatch, AbstractChainedBatch)
function PgChainedBatch (db) {
  debug('# new PgChainedBatch (db)')

  AbstractChainedBatch.call(this, db)

  // this._txWrap = util.createTransaction(db._pool, { autoRollback: false })
  // this._tx = util.createTransaction(this._ctx)
}

// PgChainedBatch.prototype._put = function (key, value) {
//   debug_v('# PgChainedBatch _put (key = %j, value = %j)', key, value)
//   this._tx.query(this._db._sql_put(), [ key, value ])
// }

// PgChainedBatch.prototype._del = function (key) {
//   debug_v('# PgChainedBatch _del (key = %j)', key)
//   this._tx.query(this._db._sql_del(), [ key ])
// }

// PgChainedBatch.prototype._clear = function () {
//   debug('# PgChainedBatch _clear ()')
//   // TODO: use autoRollback false on top level tx context
//   // then roll back child tx and start fresh
// }

PgChainedBatch.prototype._write = function (cb) {
  debug('# PgChainedBatch _write (cb)')

  const tx = util.createTransaction(this._db._pool)
  this._operations.forEach((op) => {
    if (op.type === 'put') {
      tx.query(this._db._sql_put(), [ op.key, op.value ])
    } else if (op.type === 'del') {
      tx.query(this._db._sql_del(), [ op.key ])
    } else {
      debug('_batch: unknown chained batch operation %j', op)
    }
  })

  tx.commit((err) => cb(err || null))
}
