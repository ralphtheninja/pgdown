const pglib = require('pg')

pglib.defaults.poolIdleTimeout = 2000

const util = exports

util._prefix = process.env.PGDOWN_TEST_PREFIX || 'pgdown_test_'

util._count = 0

util.setUp = (t) => {
  pglib.end()

  // TODO: hook PgDOWN#open to drop table first
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
      if (!arguments.length) return iterator.end((err) => cb(err, data))

      data.push({ key: key, value: value })
      process.nextTick(next)
    })
  }
  next()
}

util.location = (loc) => loc || (util._prefix + (++util._count))
