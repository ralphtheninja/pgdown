'use strict'

const inherits = require('inherits')
const AbstractChainedBatch = require('abstract-leveldown/abstract-chained-batch')
const util = require('./util')

module.exports = PgChainedBatch

inherits(PgChainedBatch, AbstractChainedBatch)
function PgChainedBatch (db) {
  AbstractChainedBatch.call(this, db)

  // TODO: once queued batch exceeds some threshold create a temp table
  // then flush batch ops to temp table periodically and clear ops
}

// PgChainedBatch.prototype._put = function (key, value) {
//   TODO: send ops to temp table if passed buffer threshold
// }

// PgChainedBatch.prototype._del = function (key) {
//   TODO: send ops to temp table if passed buffer threshold
// }

// PgChainedBatch.prototype._clear = function () {
//   TODO: drop temp table, if any
// }

PgChainedBatch.prototype._write = function (cb) {
  const tx = util.createTransaction(this._db._pool, cb)

  this._operations.forEach((op) => {
    if (op.type === 'put') {
      tx.query(this._db._sql_put(), [op.key, op.value])
    } else if (op.type === 'del') {
      tx.query(this._db._sql_del(), [op.key])
    }
  })

  tx.commit()
}
