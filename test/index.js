const test = require('tape')
const pgdown = require('../')
const levelup = require('levelup')
const xtend = require('xtend')
const config = require('rc')('pgdown', {
  database: 'postgres',
  table: 'pgdown_test'
})

const path = (db, table) => `/${db || config.database}/${table || config.table}`

function factory (location, options) {
  if (typeof location !== 'string') {
    options = location
    location = path()
  }

  options = options || {}

  const db = levelup(location, xtend({
    db: pgdown,
    keyEncoding: 'utf8',
    valueEncoding: 'json'
  }, options))

  return db
}

test('pgdown', (t) => {
  const db = pgdown(path())
  const table = db.pg.table

  t.test('defaults', (t) => {
    t.equal(db.pg.database, config.database, 'test database')
    t.equal(table, '"' + config.table + '"', 'test table')
    t.end()
  })

  t.test('open', (t) => {
    db.open(t.end)
  })

  t.test('drop', (t) => {
    db.drop(t.end)
  })

  t.test('close', (t) => {
    db.close(t.end)
  })
})

test('open', (t) => {
  t.test('invalid db name', (t) => {
    const db = pgdown(path('pg_bad_db_'))
    db.open((err) => {
      t.ok(err, 'invalid db name throws')
      t.end()
    })
  })

  t.test('invalid table name', (t) => {
    const db = pgdown(path(null, 'bad\0_table_'))
    db.open((err) => {
      t.ok(err, 'invalid table name throws')
      t.end()
    })
  })

  // t.test('createIfMissing', (t) => {
  //   t.test('when true', (t) => {
  //     const db = factory({
  //       createIfMissing: true
  //     })
  //   })

  //   t.test('when false', (t) => {
  //     const db = factory({
  //       createIfMissing: false
  //     })
  //   })
  // })
})

// TODO: drop table
test('crud', (t) => {
  const db = factory({ createIfMissing: true })

  t.test('init', (t) => {
    db.open((err) => {
      if (err) return t.end(err)
      // This doesn't work
      // db.db.drop(t.end)
      t.end()
    })
  })

  t.test('put', (t) => {
    db.put('a', { str: 'foo', int: 123 }, function (err, result) {
      if (err) return t.end(err)
      t.ok(result == null, 'empty response')
      t.end()
    })
  })

  t.test('get', (t) => {
    db.get('a', function (err, result) {
      if (err) return t.end(err)
      t.deepEqual(result, { str: 'foo', int: 123 })
      t.end()
    })
  })

  t.test('del', (t) => {
    db.del('a', function (err, result) {
      if (err) return t.end(err)
      db.get('a', function (err, result) {
        t.ok(err && err.notFound, 'not found')
        t.ok(result == null, 'empty response')
        t.end()
      })
    })
  })

  t.test('batch', (t) => {
    const batch = [
      {
        type: 'put',
        key: 'aa',
        value: { k: 'aa' }
      },
      {
        type: 'put',
        key: 'ac',
        value: { k: 'ac' }
      },
      {
        type: 'put',
        key: 'ab',
        value: { k: 'ab' }
      }
    ]

    t.test('array batch', (t) => {
      db.batch(batch, t.end)
    })

    t.skip('createReadStream', (t) => {
      const data = []
      db.createReadStream()
      .on('error', t.end)
      .on('data', (d) => data.push(d))
      .on('end', () => {
        const sorted = batch.slice().sort()
        t.deepEqual(data, sorted, 'all records in order')
      })
    })
  })

  t.test('close', (t) => {
    db.close((err) => {
      if (err) return t.end(err)
      // idempotent close
      // TODO this is taking very long to finish
      db.close(t.end)
    })
  })
})

// compatibility w/ leveldown api

// require('abstract-leveldown/abstract/leveldown-test').args(factory, test, testCommon)

// require('abstract-leveldown/abstract/open-test').args(factory, test, testCommon)
// require('abstract-leveldown/abstract/open-test').open(factory, test, testCommon)

// require('abstract-leveldown/abstract/put-test').all(factory, test, testCommon)

// require('abstract-leveldown/abstract/del-test').all(factory, test, testCommon)

// require('abstract-leveldown/abstract/get-test').all(factory, test, testCommon)

// require('abstract-leveldown/abstract/put-get-del-test').all(factory, test, testCommon, testBuffer)

// require('abstract-leveldown/abstract/iterator-test').all(factory, test, testCommon)

// require('abstract-leveldown/abstract/batch-test').all(factory, test, testCommon)
// require('abstract-leveldown/abstract/chained-batch-test').all(factory, test, testCommon)

// require('abstract-leveldown/abstract/close-test').close(factory, test, testCommon)
