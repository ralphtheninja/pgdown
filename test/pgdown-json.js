'use strict'

const after = require('after')
const levelup = require('levelup')
const test = require('tape')
const PgDOWN = require('../')
const util = require('../util')
const common = require('./_common')

function pgupJSON (location, options) {
  if (typeof location !== 'string') {
    options = location
    location = null
  }

  options = options || {}
  options.db = PgDOWN
  options.keyEncoding = 'utf8'
  options.valueEncoding = 'json'

  return levelup(location, options)
}

test('crud', (t) => {
  const db = pgupJSON(common.location())

  t.test('initialize', (t) => {
    db.open((err) => {
      if (err) return t.end(err)

      util.dropTable(db.db, (err) => {
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

      db.get('aa', (err, value) => {
        t.deepEqual(value, sorted[0].value, 'aa')
        done(err)
      })
      db.get('ab', (err, value) => {
        t.deepEqual(value, sorted[1].value, 'ab')
        done(err)
      })
      db.get('ac', (err, value) => {
        t.deepEqual(value, sorted[2].value, 'ac')
        done(err)
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

  t.test('approximate size', (t) => {
    db.db.approximateSize('a', 'ac', (err, size1) => {
      if (err) return t.end(err)

      t.ok(size1 > 0, 'positive')
      t.equal(parseInt(size1), size1, 'integer')

      db.db.approximateSize('a', 'ab', (err, size2) => {
        if (err) return t.end(err)

        t.ok(size2 > 0, 'positive')
        t.equal(parseInt(size2), size2, 'integer')
        t.ok(size1 > size2)
        t.end()
      })
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
