'use strict'

const test = require('tape')
const common = require('./_common')
const PgDOWN = common.db

test('constructor', (t) => {
  t.test('defaults', (t) => {
    const db = PgDOWN(common.location())
    const config = db._config
    t.equal(config.database, common.PG_DEFAULTS.database, 'uses default database')
    t.equal(config._table.indexOf(common.escape.ident(common.PREFIX)), 0, 'uses test table prefix')
    t.equal(config._schema, common.escape.ident(common.SCHEMA), 0, 'uses test schema')
    t.equal(config._relation.indexOf(config._schema), 0, 'rel name begins with schema')
    t.ok(config._relation.indexOf(config._table) >= 0, 'rel name includes table')
    t.end()
  })
})

test('open', (t) => {
  t.test('empty location', (t) => {
    t.throws(() => PgDOWN(), 'location required')
    t.throws(() => new PgDOWN(), 'location required')
    t.throws(() => new PgDOWN(''), 'location required')
    t.end()
  })

  t.test('throw on malformed db name', (t) => {
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

  t.test('error on illegal table name (null byte)', (t) => {
    const db = PgDOWN('illegal_\x00_table')
    db.open((err) => {
      t.ok(err, 'error on open')
      db.close(t.end)
    })
  })

  t.test('table path', (t) => {
    const db = PgDOWN(common.location('foo/bar/baz'))
    db.open((err) => {
      if (err) return t.end(err)
      db.close(t.end)
    })
  })

  t.test('weird table name (0x01 byte)', (t) => {
    const db = PgDOWN(common.location('weird_\x01_table'))
    db.open((err) => {
      if (err) return t.end(err)
      db.close(t.end)
    })
  })

  t.test('weird table name (0xff byte)', (t) => {
    const db = PgDOWN(common.location('weird_\xff_table'))
    db.open((err) => {
      if (err) return t.end(err)
      db.close(t.end)
    })
  })

  t.test('weird table name (empty quoted string)', (t) => {
    const db = PgDOWN(common.location('""'))
    db.open((err) => {
      if (err) return t.end(err)
      db.close(t.end)
    })
  })

  t.test('error for create if missing', (t) => {
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

  t.test('idempotent close', (t) => {
    const db = PgDOWN(common.location())
    db.open((err) => {
      if (err) return t.end(err)
      db.close((err) => {
        if (err) return t.end(err)
        db.close(t.end)
      })
    })
  })
})
