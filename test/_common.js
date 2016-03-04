'use strict'

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
  util.destroyAll(cb)
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

// hack _open to drop tables at first open
const DROPPED = {}

const _PgDOWN_open = PgDOWN.prototype._open
PgDOWN.prototype._open = function (options, cb) {
  const location = this.location

  if (location !== _last || DROPPED[location]) {
    return _PgDOWN_open.call(this, options, cb)
  }

  util.dropTable(this, (err) => {
    // TODO: err.code == '42P01'
    if (err && err.routine !== 'DropErrorMsgNonExistent') return cb(err)

    DROPPED[location] = true
    _PgDOWN_open.call(this, options, cb)
  })
}
