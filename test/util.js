'use strict'

const pg = require('pg')
const PgDOWN = require('../')

const util = exports

util._prefix = process.env.PGDOWN_TEST_PREFIX || 'pgdown_test_'
pg.defaults.database = process.env.PGDOWN_TEST_DATABASE || 'postgres'

var _count = 0
var _last

util.lastLocation = () => _last

util.location = (loc) => (_last = loc || (util._prefix + (++_count)))

util.cleanup = (cb) => {
  pg.end()
  cb()
}

util.setUp = (t) => {
  util.cleanup((err) => {
    t.error(err, 'cleanup returned an error')
    t.end()
  })
}

util.tearDown = (t) => {
  util.setUp(t)
}

util.collectEntries = function (iterator, cb) {
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

  this._drop((err) => {
    if (err && err.routine !== 'DropErrorMsgNonExistent') return cb(err)

    dropped[table] = true
    _PgDOWN_open.call(this, options, cb)
  })
}
