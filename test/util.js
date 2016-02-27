const levelup = require('levelup')
const pg = require('pg')
const pgdown = require('../')

const util = exports

util._config = require('rc')('pgdown', {
  database: 'postgres',
  table: 'pgdown_test'
})

const _db = util._config.database
const _tbl = util._config.table

util._idx = 0

util.setUp = (t) => {
  // TODO: drop table
  t.end()
}
util.tearDown = (t) => {
  pg.end()
  t.end()
}

util.location = (db, tbl) => `/${db || _db}/${tbl || (_tbl + (++util._idx))}`

util.factory = factory

function factory (location, options) {
  if (typeof location !== 'string') {
    options = location
    location = null
  }

  options = options || {}
  options.db = pgdown

  return levelup(location, options)
}
