const levelup = require('levelup')
const pg = require('pg')
const PgDOWN = require('../')

pg.poolIdleTimeout = 2000

const util = exports

util._config = require('rc')('pgdown', {
  database: 'postgres',
  table: 'pgdown_test_'
})

const _db = util._config.database
const _tbl = util._config.table

util._idx = 0

util.setUp = (t) => {
  // TODO: hook PgDOWN#open to drop table first
  t.end()
}

util.tearDown = (t) => {
  pg.end()
  t.end()
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

util.location = (db, tbl) => `/${db || _db}/${tbl || (_tbl + (++util._idx))}`

util.factory = factory

function factory (location, options) {
  if (typeof location !== 'string') {
    options = location
    location = null
  }

  options = options || {}
  options.db = PgDOWN

  return levelup(location, options)
}
