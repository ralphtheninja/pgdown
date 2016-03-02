const pglib = require('pg')
const PgDOWN = require('../')

const util = exports

util._prefix = process.env.PGDOWN_TEST_PREFIX || 'pgdown_test_'

util._count = 0

util.location = (loc) => {
  return (util._last = loc || (util._prefix + (++util._count)))
}

util.setUp = (t) => {
  pglib.end()
  t.end()
}

util.tearDown = (t) => {
  util.setUp(t)
}

util.collectEntries = function (iterator, cb) {
  const data = []
  const next = () => {
    iterator.next((err, key, value) => {
      if (err) return cb(err)

      if (!arguments.length) {
        return process.nextTick(() => {
          iterator.end((err) => cb(err, data))
        })
      }

      data.push({ key: key, value: value })

      process.nextTick(next)
    })
  }
  next()
}

// hack _open to drop tables at first open
const dropped = {}

const _PgDOWN_open = PgDOWN.prototype._open
PgDOWN.prototype._open = function (options, cb) {
  const table = this._table

  // use a lower default pool idle timeout
  options.poolIdleTimeout = options.poolIdleTimeout || 2000

  if (table !== util._last || dropped[table]) {
    return _PgDOWN_open.call(this, options, cb)
  }

  this._drop((err) => {
    if (err && err.routine !== 'DropErrorMsgNonExistent') return cb(err)

    dropped[table] = true
    _PgDOWN_open.call(this, options, cb)
  })
}
