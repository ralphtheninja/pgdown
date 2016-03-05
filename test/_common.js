'use strict'

const after = require('after')
const util = require('../util')
const PgDOWN = require('../')

util.PG_DEFAULTS.database = process.env.PGDOWN_TEST_DATABASE || 'postgres'
util.PG_DEFAULTS.poolIdleTimeout = 2000

const common = exports

common.PREFIX = process.env.PGDOWN_TEST_PREFIX || 'pgdown_test_'

var _count = 0
var _last

common.lastLocation = () => _last

common.location = (loc) => (_last = loc || (common.PREFIX + (++_count)))

common.cleanup = (cb) => {
  const len = OPENED.length
  const done = after(len, cb)

  for (var i = 0; i < len; i++) {
    const db = OPENED[i]
    const pool = db && db._pool
    if (pool) pool.close(done)
    else done()
  }

  OPENED.length = 0
}

common.setUp = (t) => {
  common.cleanup((err) => {
    t.error(err, 'cleanup returned an error')
    t.end()
  })
}

common.tearDown = (t) => {
  common.setUp(t)
}

common.collectEntries = function (iterator, cb) {
  const data = []
  function next () {
    iterator.next(function (err, key, value) {
      if (err) return cb(err)
      if (!arguments.length) {
        return iterator.end(function (err) {
          cb(err, data)
        })
      }
      data.push({ key: key, value: value })
      setTimeout(next, 0)
    })
  }
  next()
}

common.maxCompressionFactor = 0.01

common.checkBatchSize = function (batch, size) {
  // very specific to leveldb, accounts for snappy compression
  const total = batch.reduce((n, op) => n + (op.key + op.value).length, 0)
  return size > total * common.maxCompressionFactor
}

// hack db class to drop tables at first open, track open pools to close on end
const DROPPED = {}
const OPENED = []

const _PgDOWN_open = PgDOWN.prototype._open
PgDOWN.prototype._open = function (options, cb) {
  const location = this.location

  if (location !== _last || DROPPED[location]) {
    return _PgDOWN_open.call(this, options, cb)
  }

  util.dropTable(this, (err) => {
    if (err) return cb(err)

    DROPPED[location] = true
    _PgDOWN_open.call(this, options, (err) => {
      OPENED.push(this)
      cb(err)
    })
  })
}
