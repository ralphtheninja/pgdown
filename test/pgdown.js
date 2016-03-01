const test = require('tape')
const util = require('./util')
const pglib = require('pg')
const PgDOWN = require('../')

test('basic', (t) => {
  const db = PgDOWN(util.location())

  t.test('defaults', (t) => {
    t.equal(db.pg.database, pglib.defaults.database, 'default database set')
    t.equal(db._table.indexOf(util._prefix), 0, 'table name uses test prefix')
    t.ok(db._qname.indexOf(db._table) >= 0, 'qname includes table name')
    t.equal(db._schema, undefined, 'no schema')
    t.end()
  })

  t.test('open db', (t) => {
    db.open(t.end)
  })

  t.test('drop db', (t) => {
    db._drop(t.end)
  })

  t.test('close db', (t) => {
    db.close(t.end)
  })
})

test('open', (t) => {
  t.test('invalid db name', (t) => {
    const badDb = 'pg_bad_db_name'
    const loc = '/' + badDb + '/' + util._prefix
    const db = PgDOWN('/' + badDb + '/' + util.location())
    t.equal(db.pg.database, badDb, 'bad db name set')
    t.equal(db.location.indexOf(loc), 0, 'location set')

    db.open((err) => {
      t.ok(err, 'invalid db name throws')
      t.end()
    })
  })

  t.test('invalid table name', (t) => {
    const badTable = 'bad\0_table'
    const db = PgDOWN(badTable)
    t.equal(db._table, badTable, 'bad table name')

    db.open((err) => {
      t.ok(err, 'invalid table name throws')
      t.end()
    })
  })

  // t.test('createIfMissing', (t) => {
  //   t.test('when true', (t) => {
  //     const db = pgupJSON(util.location(), {
  //       createIfMissing: true
  //     })
  //   })

  //   t.test('when false', (t) => {
  //     const db = pgupJSON(util.location(), {
  //       createIfMissing: false
  //     })
  //   })
  // })
})
