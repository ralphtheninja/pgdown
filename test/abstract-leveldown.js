'use strict'

const test = require('tape')
const common = require('./_common')
const PgDOWN = require('../')

// TODO: use a larger buffer
const buffer = new Buffer('00ff61626301feffff00000000ffff', 'hex')

// compatibility w/ leveldown api

require('abstract-leveldown/abstract/leveldown-test').args(PgDOWN, test, common)

require('abstract-leveldown/abstract/open-test').all(PgDOWN, test, common)

require('abstract-leveldown/abstract/close-test').close(PgDOWN, test, common)

require('abstract-leveldown/abstract/put-test').all(PgDOWN, test, common)

require('abstract-leveldown/abstract/del-test').all(PgDOWN, test, common)

require('abstract-leveldown/abstract/get-test').all(PgDOWN, test, common)

require('abstract-leveldown/abstract/put-get-del-test').all(PgDOWN, test, common, buffer)

require('abstract-leveldown/abstract/iterator-test').all(PgDOWN, test, common)

require('abstract-leveldown/abstract/ranges-test').all(PgDOWN, test, common)

require('abstract-leveldown/abstract/batch-test').all(PgDOWN, test, common)

// NB: hack chained batch to keep operations around for abstract-leveldown tests
const PgChainedBatch = require('../pg-chained-batch')

const _PgChainedBatch_put = PgChainedBatch.prototype._put
PgChainedBatch.prototype._put = function (key, value) {
  this._operations.push({ type: 'put', key: key, value: value })
  _PgChainedBatch_put.apply(this, arguments)
}

const _PgChainedBatch_del = PgChainedBatch.prototype._del
PgChainedBatch.prototype._del = function (key) {
  this._operations.push({ type: 'del', key: key })
  _PgChainedBatch_del.apply(this, arguments)
}

require('abstract-leveldown/abstract/chained-batch-test').args(PgDOWN, test, common)
require('abstract-leveldown/abstract/chained-batch-test').batch(PgDOWN, test, common)
