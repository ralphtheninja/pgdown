'use strict'

const util = require('../util')
const PgDOWN = require('../')

const common = exports

common._prefix = process.env.PGDOWN_TEST_PREFIX || 'pgdown_test_'
util.pg.defaults.database = process.env.PGDOWN_TEST_DATABASE || 'postgres'

var _count = 0
var _last

common.lastLocation = () => _last

common.location = (loc) => (_last = loc || (common._prefix + (++_count)))

common.cleanup = (cb) => {
  util.pg.end()
  cb()
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
const dropped = {}

const _PgDOWN_open = PgDOWN.prototype._open
PgDOWN.prototype._open = function (options, cb) {
  const table = this._table

  if (table !== _last || dropped[table]) {
    return _PgDOWN_open.call(this, options, cb)
  }

  util.drop(this, (err) => {
    if (err && err.routine !== 'DropErrorMsgNonExistent') return cb(err)

    dropped[table] = true
    _PgDOWN_open.call(this, options, cb)
  })
}
