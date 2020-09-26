'use strict'

const after = require('after')
const levelup = require('levelup')
const test = require('tape')
const common = require('./_common')
const destroy = require('../').destroy

test('utf8 keyEncoding, json valueEncoding', (t) => {
  t.test('bytea keys, bytea values', (t) => {
    const db = levelup(common.location(), {
      db: common.db,
      keyEncoding: 'utf8',
      valueEncoding: 'json'
    })
    db._keyColumnType = 'text'
    testEncodings(db, t)
  })

  t.test('text keys, jsonb values', (t) => {
    const db = levelup(common.location(), {
      db: common.db,
      keyEncoding: 'utf8',
      valueEncoding: 'json'
    })
    db._keyColumnType = 'text'
    db._valueColumnType = 'jsonb'
    testEncodings(db, t)
  })

  t.test('jsonb keys, json values', (t) => {
    const db = levelup(common.location(), {
      db: common.db,
      keyEncoding: 'utf8',
      valueEncoding: 'json'
    })
    db._keyColumnType = 'jsonb'
    db._valueColumnType = 'json'
    testEncodings(db, t)
  })
})

const testEncodings = (db, t) => {
  t.test('initialize', (t) => {
    destroy(db.location, (err) => {
      if (err) return t.end(err)
      db.open(t.end)
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

  t.test('non-object values', (t) => {
    t.test('nullish values', (t) => {
      t.test('null value', (t) => {
        const k = 'null'
        db.put(k, null, (err) => {
          if (err) return t.end(err)
          db.get(k, (err, value) => {
            if (err) return t.end(err)
            t.equal(value, null, 'correct value')
            t.end()
          })
        })
      })

      t.test('NaN value', (t) => {
        const k = 'NaN'
        db.put(k, NaN, (err) => {
          if (err) return t.end(err)
          db.get(k, (err, value) => {
            if (err) return t.end(err)
            t.equal(value, null, 'correct value')
            t.end()
          })
        })
      })

      t.test('Invalid Date value', (t) => {
        const k = 'Invalid Date'
        db.put(k, new Date('xxxx'), (err) => {
          if (err) return t.end(err)
          db.get(k, (err, value) => {
            if (err) return t.end(err)
            t.equal(value, null, 'correct value')
            t.end()
          })
        })
      })
    })

    t.test('boolean values', (t) => {
      t.test('false value', (t) => {
        const k = 'false'
        db.put(k, false, (err) => {
          if (err) return t.end(err)
          db.get(k, (err, value) => {
            if (err) return t.end(err)
            t.equal(value, false, 'correct value')
            t.end()
          })
        })
      })

      t.test('true value', (t) => {
        const k = 'true'
        db.put(k, true, (err) => {
          if (err) return t.end(err)
          db.get(k, (err, value) => {
            if (err) return t.end(err)
            t.equal(value, true, 'correct value')
            t.end()
          })
        })
      })
    })

    t.test('numeric values', (t) => {
      t.test('negative zero', (t) => {
        const k = 'zero'
        db.put(k, -0, (err) => {
          if (err) return t.end(err)
          db.get(k, (err, value) => {
            if (err) return t.end(err)
            t.equal(value, 0, 'correct value')
            t.end()
          })
        })
      })

      t.test('integer', (t) => {
        const k = 'integer'
        const v = 21
        db.put(k, v, (err) => {
          if (err) return t.end(err)
          db.get(k, (err, value) => {
            if (err) return t.end(err)
            t.equal(value, v, 'correct value')
            t.end()
          })
        })
      })

      t.test('float', (t) => {
        const k = 'float'
        const v = -29.3123433726
        db.put(k, v, (err) => {
          if (err) return t.end(err)
          db.get(k, (err, value) => {
            if (err) return t.end(err)
            t.equal(value, v, 'correct value')
            t.end()
          })
        })
      })

      t.test('exponential', (t) => {
        const k = 'exponential'
        const v = 4.56e-123
        db.put(k, v, (err) => {
          if (err) return t.end(err)
          db.get(k, (err, value) => {
            if (err) return t.end(err)
            t.equal(value, v, 'correct value')
            t.end()
          })
        })
      })
    })

    t.test('date values', (t) => {
      t.test('y2k', (t) => {
        const k = 'y2k'
        const v = new Date('2000-01-01Z')
        db.put(k, v, (err) => {
          if (err) return t.end(err)
          db.get(k, (err, value) => {
            if (err) return t.end(err)
            t.equal(value, '2000-01-01T00:00:00.000Z', 'correct value')
            t.end()
          })
        })
      })
    })

    t.test('string values', (t) => {
      t.test('empty string', (t) => {
        const k = 'empty string'
        db.put(k, '', (err) => {
          if (err) return t.end(err)
          db.get(k, (err, value) => {
            if (err) return t.end(err)
            t.equal(value, '', 'correct value')
            t.end()
          })
        })
      })

      t.test('long string', (t) => {
        const k = 'long string'
        const v = Array.apply(null, Array(100000)).map(() => 'Hello "there".\r\n').join('')
        db.put(k, v, (err) => {
          if (err) return t.end(err)
          db.get(k, (err, value) => {
            if (err) return t.end(err)
            t.equal(value, v, 'correct value')
            t.end()
          })
        })
      })

      t.test('surrogate pair', (t) => {
        const k = 'surrogate pair'
        const v = 'pair \xc0\x80'
        db.put(k, v, (err) => {
          if (err) return t.end(err)
          db.get(k, (err, value) => {
            if (err) return t.end(err)
            t.equal(value, v, 'correct value')
            t.end()
          })
        })
      })

      t.test('string with control chars', (t) => {
        const k = 'null char'
        const v = 'chars: \x01\x02\xff\xfe\x80\x7f\uffff\ufffe'
        db.put(k, v, (err) => {
          if (err) return t.end(err)
          db.get(k, (err, value) => {
            if (err) return t.end(err)
            t.equal(value, v, 'correct value')
            t.end()
          })
        })
      })

      t.test('string with null chars', (t) => {
        const k = 'null char'
        const v = 'chars: \x01\x00\x02\x00\x01\x0101\x00\u0000\x7e\x01\x7d\xfe'
        db.put(k, v, (err) => {
          if (err) return t.end(err)
          db.get(k, (err, value) => {
            if (err) return t.end(err)
            t.equal(value, v, 'correct value')
            t.end()
          })
        })
      })
    })

    t.test('array values', (t) => {
      t.test('empty array', (t) => {
        const k = 'empty array'
        db.put(k, [], (err) => {
          if (err) return t.end(err)
          db.get(k, (err, value) => {
            if (err) return t.end(err)
            t.deepEqual(value, [], 'correct value')
            t.end()
          })
        })
      })

      t.test('long array', (t) => {
        const k = 'long array'
        const v = Array.apply(null, Array(100000)).map(() => 'Hello there.\r\n')
        db.put(k, v, (err) => {
          if (err) return t.end(err)
          db.get(k, (err, value) => {
            if (err) return t.end(err)
            t.deepEqual(value, v, 'correct value')
            t.end()
          })
        })
      })

      t.test('mixed array', (t) => {
        const k = 'mixed array'
        const v = ['foo', 123, [{}, { foo: { 0: null } }]]
        db.put(k, v, (err) => {
          if (err) return t.end(err)
          db.get(k, (err, value) => {
            if (err) return t.end(err)
            t.deepEqual(value, v, 'correct value')
            t.end()
          })
        })
      })
    })

    t.test('encoding failures', (t) => {
      t.skip('undefined value', (t) => {
        const k = 'undefined'
        db.put(k, undefined, (err, result) => {
          t.ok(err, 'fails to save')
          db.get(k, (err, record) => {
            t.ok(err && err.notFound, 'not found')
            t.ok(record == null, 'no value returned')
            t.end()
          })
        })
      })

      t.test('circular reference', (t) => {
        const k = 'circular'
        const v = { child: {} }
        v.child.parent = v

        t.throws(() => db.put(k, v, () => {}))
        t.end()
      })
    })
  })

  t.test('abnormal keys', (t) => {
    t.test('null byte', (t) => {
      const k = 'null\x00key'
      db.put(k, 'val', (err) => {
        if (err) return t.end(err)
        db.get(k, (err, value) => {
          if (err) return t.end(err)
          t.equal(value, 'val', 'correct value')
          t.end()
        })
      })
    })

    t.test('control char (\\x01)', (t) => {
      const k = 'weird\x01key'
      db.put(k, 'val', (err) => {
        if (err) return t.end(err)
        db.get(k, (err, value) => {
          if (err) return t.end(err)
          t.equal(value, 'val', 'correct value')
          t.end()
        })
      })
    })

    t.test('control char (\\xff)', (t) => {
      const k = 'weird\xffkey'
      db.put(k, 'val', (err) => {
        if (err) return t.end(err)
        db.get(k, (err, value) => {
          if (err) return t.end(err)
          t.equal(value, 'val', 'correct value')
          t.end()
        })
      })
    })

    t.test('control char (\\uffff)', (t) => {
      const k = 'weird\uffffkey'
      db.put(k, 'val', (err) => {
        if (err) return t.end(err)
        db.get(k, (err, value) => {
          if (err) return t.end(err)
          t.equal(value, 'val', 'correct value')
          t.end()
        })
      })
    })
  })

  t.test('approximate size', (t) => {
    db.db.approximateSize('a', 'ac', (err, size1) => {
      if (err) return t.end(err)

      t.ok(size1 > 0, 'positive')
      t.equal(parseInt(size1), size1, 'integer')

      db.db.approximateSize('a', 'ab', (err, size2) => {
        if (err) return t.end(err)

        t.ok(size2 < size1, 'smaller than superset size')
        t.ok(size2 > 0, 'positive')
        t.equal(parseInt(size2), size2, 'integer')
        t.ok(size1 > size2)
        t.end()
      })
    })
  })

  t.test('idempotent close', (t) => {
    db.close((err) => {
      if (err) return t.end(err)
      db.close(t.end)
    })
  })

  t.end()
}
