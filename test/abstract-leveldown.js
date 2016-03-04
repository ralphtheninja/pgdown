'use strict'

const inherits = require('inherits')
const test = require('tape')
const common = require('./_common')
const PgDOWN = require('../')

// TODO: use a larger buffer
const buffer = new Buffer('00ff61626301feffff00000000ffff', 'hex')

const PgChainedBatch = require('../pg-chained-batch')
const _testChainedBatch = require('abstract-leveldown/abstract/chained-batch-test')

// NB: hack chained batch to keep operations around for abstract-leveldown tests
function testChainedBatch (DOWN, test, common) {
  function QueuedChainedBatch (db) {
    QueuedChainedBatch.call(this, db)
  }

  inherits(QueuedChainedBatch, PgChainedBatch)

  QueuedChainedBatch.prototype._put = function (key, value) {
    this._operations.push({ type: 'put', key: key, value: value })
    PgChainedBatch.prototype._put.apply(this, arguments)
  }

  QueuedChainedBatch.prototype._del = function (key) {
    this._operations.push({ type: 'del', key: key })
    PgChainedBatch.prototype._del.apply(this, arguments)
  }

  // replace chained batch w/ something useful
  const _chainedBatch = DOWN.prototype._chainedBatch
  DOWN.prototype._chainedBatch = function () {
    return new QueuedChainedBatch(this)
  }

  _testChainedBatch.args(DOWN, test, common)
  _testChainedBatch.batch(DOWN, test, common)

  // replace chained batch
  DOWN.prototype._chainedBatch = _chainedBatch
}

// verify compatibility w/ leveldown api
const suites = {
  leveldown: require('abstract-leveldown/abstract/leveldown-test').args,
  open: require('abstract-leveldown/abstract/open-test').all,
  close: require('abstract-leveldown/abstract/close-test').close,
  // 'approximate-size': require('abstract-leveldown/abstract/approximate-size-test').all,
  put: require('abstract-leveldown/abstract/put-test').all,
  del: require('abstract-leveldown/abstract/del-test').all,
  get: require('abstract-leveldown/abstract/get-test').all,
  'put-get-del': require('abstract-leveldown/abstract/put-get-del-test').all,
  iterator: require('abstract-leveldown/abstract/iterator-test').all,
  ranges: require('abstract-leveldown/abstract/ranges-test').all,
  batch: require('abstract-leveldown/abstract/batch-test').all,
  'chained-batch': testChainedBatch
}

const factories = [ PgDOWN ]

factories.forEach((factory) => {
  Object.keys(suites).forEach((name) => {
    console.log('abstract-leveldown test: ' + name)
    suites[name](factory, test, common, buffer)
  })
})
