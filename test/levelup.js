'use strict'

const after = require('after')
const levelup = require('levelup')
const test = require('tape')
const common = require('./_common')
const destroy = require('../').destroy

test('utf8 keyEncoding, json valueEncoding', (t) => {
  const db = levelup(common.location(), {
    db: common.factory,
    keyEncoding: 'utf8',
    valueEncoding: 'json'
  })

  t.test('initialize', (t) => {
    destroy(db.location, (err) => {
      if (err) return t.end(err)
      db.open(t.end)
    })
  })

  t.test('open', (t) => {
    db.open((err) => {
      if (err) return t.end(err)

      t.equal(db.db._keyDataType, 'text', 'text from utf8 keyEncoding')
      t.equal(db.db._valueDataType, 'jsonb', 'jsonb from json valueEncoding')
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

  t.skip('value with null byte', (t) => {
    const v = 'i can haz \0 byte?'
    db.put('nullv', v, (err) => {
      if (err) return t.end(err)

      db.get('nullv', (err, value) => {
        if (err) return t.end(err)
        t.equal(value, v, 'value with null byte')
        t.end()
      })
    })
  })

  t.skip('key with null byte', (t) => {
    const v = 'value for key with null byte'
    db.put('null\0', v, (err) => {
      if (err) return t.end(err)

      db.get('null\0', (err, value) => {
        if (err) return t.end(err)
        t.deepEqual(value, v, 'value for key with null byte')
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
