'use strict'

const after = require('after')
const inherits = require('inherits')
const util = require('../util')
const PgDOWN = require('../')

const common = exports

common.PG_DEFAULTS = util.PG_DEFAULTS

common.PG_DEFAULTS.database = process.env.PGDOWN_TEST_DATABASE || 'postgres'
common.PG_DEFAULTS.idleTimeout = Number(process.env.PGDOWN_TEST_IDLE_TIMEOUT) || 2000
common.PREFIX = process.env.PGDOWN_TEST_PREFIX || 'table_'

// use a distinct schema for tests
common.SCHEMA = util.schemaName = process.env.PGDOWN_TEST_SCHEMA || 'pgdown_test'

var _count = 0
var _last

common.lastLocation = () => _last

common.location = (loc) => (_last = loc || (common.PREFIX + (++_count)))

common.cleanup = (cb) => {
  const len = common.OPENED.length
  const done = after(len, cb)

  for (var i = 0; i < len; i++) {
    const db = common.OPENED[i]
    const pool = db && db._pool
    if (pool) pool.close(done)
    else done()
  }

  common.OPENED.length = 0
}

common.setUp = (t) => {
  common.cleanup(t.end)
}

common.tearDown = (t) => {
  t.timeoutAfter(2000)
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

// hack db class to drop tables on first open, track open pools to close on end
common.OPENED = []
common.DROPPED = {}
common.factory = TestPgDOWN

inherits(TestPgDOWN, PgDOWN)
function TestPgDOWN (location) {
  if (!(this instanceof TestPgDOWN)) {
    return new TestPgDOWN(location)
  }
  PgDOWN.call(this, location)
}

const _PgDOWN_open = PgDOWN.prototype._open
TestPgDOWN.prototype._open = function (options, cb) {
  const location = this.location

  if (location !== _last || common.DROPPED[location]) {
    return _PgDOWN_open.call(this, options, cb)
  }

  util.dropTable(location, (err) => {
    if (err) return cb(err)

    common.DROPPED[location] = true
    _PgDOWN_open.call(this, options, (err) => {
      common.OPENED.push(this)
      cb(err)
    })
  })
}
