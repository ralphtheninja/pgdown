'use strict'

const test = require('tape')
const util = require('./util')
const pg = require('pg')
const PgDOWN = require('../')

test('constructor', (t) => {
  t.test('defaults', (t) => {
    const db = PgDOWN(util.location())
    t.equal(db._config.database, pg.defaults.database, 'default database set')
    t.equal(db._table.indexOf(util._prefix), 0, 'table name uses test prefix')
    t.ok(db._qname.indexOf(db._table) >= 0, 'qname includes table name')
    t.equal(db._schema, undefined, 'no schema')
    t.end()
  })
})

test('open', (t) => {
  t.test('empty location', (t) => {
    t.throws(() => PgDOWN(), 'location required')
    t.throws(() => new PgDOWN(), 'location required')
    t.end()
  })

  t.test('invalid db name', (t) => {
    const database = 'pg_invalid_db__'
    const loc = util.location('/' + database + '/' + util._prefix)
    const db = PgDOWN(loc)
    t.equal(db._config.database, database, 'db name set')
    t.equal(db.location.indexOf(loc), 0, 'location set')

    db.open((err) => {
      t.ok(err, 'error on open')
      t.end()
    })
  })

  t.skip('invalid table name', (t) => {
    const table = util.location('pg_invalid_table__')
    const db = PgDOWN(table)
    t.equal(db._table, table, 'table name set')

    db.open((err) => {
      t.ok(err, 'error on open')
      t.end()
    })
  })

  t.test('malformed table name', (t) => {
    const table = util.location('malformed_\0_table')
    const db = PgDOWN(table)
    t.equal(db._table, table, 'table name set')

    db.open((err) => {
      t.ok(err, 'error on open')
      t.end()
    })
  })

  t.test('no create if missing', (t) => {
    const loc = util.location()
    const opts = { createIfMissing: false }

    const db = PgDOWN(loc)
    db.open(opts, (err) => {
      t.equal(db.location, loc, 'location set')
      t.ok(err, 'error on open')

      const db2 = PgDOWN(loc)
      db2.open(opts, (err) => {
        t.equal(db2.location, loc, 'location set')
        t.ok(err, 'error on open')

        t.end()
      })
    })
  })
})
