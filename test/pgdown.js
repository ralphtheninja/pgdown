'use strict'

const test = require('tape')
const util = require('../util')
const common = require('./_common')
const PgDOWN = common.factory

test('constructor', (t) => {
  t.test('defaults', (t) => {
    const db = PgDOWN(common.location())
    const config = db._config
    t.equal(config.database, util.PG_DEFAULTS.database, 'uses default database')
    t.equal(db._table.indexOf(common.PREFIX), 0, 'table name uses test prefix')
    t.ok(db._rel.indexOf(config._table) >= 0, 'qualified name includes table name')
    t.end()
  })
})

test('open', (t) => {
  t.test('empty location', (t) => {
    t.throws(() => PgDOWN(), 'location required')
    t.throws(() => new PgDOWN(), 'location required')
    t.end()
  })

  t.test('malformed db name', (t) => {
    const database = 'pg_invalid_db__'
    const loc = common.location('/' + database + '/' + common.PREFIX)
    const db = PgDOWN(loc)
    t.equal(db._config.database, database, 'db name set')
    t.equal(db.location.indexOf(loc), 0, 'location set')

    db.open((err) => {
      t.ok(err, 'error on open')
      db.close(t.end)
    })
  })

  // TODO: catch null bytes at query time
  t.skip('malformed table name', (t) => {
    const table = common.location('malformed_\0_table')
    const db = PgDOWN(table)
    t.equal(db._config._table, table, 'table name in config')

    db.open((err) => {
      t.ok(err, 'error on open')
      db.close(t.end)
    })
  })

  t.test('no create if missing', (t) => {
    const loc = common.location()
    const opts = { createIfMissing: false }

    const db1 = PgDOWN(loc)
    db1.open(opts, (err) => {
      t.equal(db1.location, loc, 'location set')
      t.ok(err, 'error on open')

      const db2 = PgDOWN(loc)
      db2.open(opts, (err) => {
        t.equal(db2.location, loc, 'location set')
        t.ok(err, 'error on open')

        db1.close((err1) => {
          db2.close((err2) => {
            t.end(err1 || err2)
          })
        })
      })
    })
  })
})
