const after = require('after')
const test = require('tape')
const pgdown = require('../')
const util = require('./util')

function pgupJSON (location, options) {
  if (typeof location !== 'string') {
    options = location
    location = null
  }
  options = options || {}
  options.keyEncoding = 'utf8'
  options.valueEncoding = 'json'

  return util.factory(location, options)
}

test('raw pgdown', (t) => {
  const db = pgdown(util.location())

  t.test('defaults', (t) => {
    t.equal(db._database, util._config.database, 'test database')
    t.equal(db._table, util._config.table + util._idx, 'test table')
    t.equal(db._schema, null, 'no schema')
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
    const badDb = 'pg_bad_db_'
    const db = pgdown(util.location(badDb))
    t.equal(db._database, badDb, 'bad db name')

    db.open((err) => {
      t.ok(err, 'invalid db name throws')
      t.end()
    })
  })

  t.test('invalid table name', (t) => {
    const badTable = 'bad\0_table_'
    const db = pgdown(util.location(null, badTable))
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

test('crud', (t) => {
  const db = pgupJSON(util.location())

  t.test('initialize', (t) => {
    db.open((err) => {
      if (err) return t.end(err)

      db.db.drop((err) => {
        if (err) return t.end(err)
        db.close(t.end)
      })
    })
  })

  t.test('open', (t) => {
    db.open(t.end)
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

  const sorted = batch.slice().sort((a, b) => a.key < b.key ? -1 : (a.key > b.key ? 1 : 0))

  t.test('array batch', (t) => {
    db.batch(batch, (err) => {
      if (err) return t.end(err)

      const done = after(batch.length, t.end)

      db.get('aa', (err, record) => {
        done(err)
        t.deepEqual(record, sorted[0].value, 'aa')
      })
      db.get('ab', (err, record) => {
        done(err)
        t.deepEqual(record, sorted[1].value, 'ab')
      })
      db.get('ac', (err, record) => {
        done(err)
        t.deepEqual(record, sorted[2].value, 'ac')
      })
    })
  })

  t.test('read stream', (t) => {
    const data = []
    db.createReadStream()
    .on('error', t.end)
    .on('data', (d) => data.push(d))
    .on('end', () => {
      // add put op type to compare to sorted batch
      data.forEach((d) => { d.type = 'put' })
      t.deepEqual(data, sorted, 'all records in order')
      t.end()
    })
  })

  t.test('close', (t) => {
    db.close((err) => {
      if (err) return t.end(err)
      // idempotent close
      db.close(t.end)
    })
  })
})
