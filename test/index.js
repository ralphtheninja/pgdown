const test = require('tape')
const pgdown = require('../')
const levelup = require('levelup')
const xtend = require('xtend')
const DEFAULT_URI = require('./rc').uri

function factory (location, options) {
  if (typeof location === 'undefined') {
    location = DEFAULT_URI
    options = {}
  }

  if (typeof options === 'undefined') {
    options = {}
  }

  return levelup(location, xtend({
    db: pgdown,
    keyEncoding: 'utf8',
    valueEncoding: 'json'
  }, options))

  // // ensure we close our clients even when tests fail
  // // TODO: looks like we need introduce some delay to enable this
  // db.on('open', (a) => {
  //   db.db.pg.client.on('drain', db.close.bind(db))
  // })

  return db
}

test('initialization', (t) => {
  t.test('pgdown defaults', (t) => {
    const db = pgdown(DEFAULT_URI)
    t.ok(db.pg.client, 'client initialized')
    // t.equal(db.pg.id, '""table_name"', 'default db id')
    // t.equal(db.pg.id, '"nested__schema__path"."table_name"', 'db id w/ nested schema')
    db.close(t.end)
  })
})

// test('raw client queries', (t) => {
//   const db = factory()
//   db.open((err) => {
//     if (err) return t.end(err)

//     db.db.pg.client.query('DROP TABLE ${db.pg.id}', (err, result) => {
//       console.warn(err, result)
//       t.end(err)
//     })
//   })
// })

// test('open', (t) => {
//   t.test('createIfMissing', (t) => {
//     t.test('when true', (t) => {
//       const db = factory({
//         createIfMissing: true
//       })
//     })

//     t.test('when false', (t) => {
//       const db = factory({
//         createIfMissing: false
//       })
//     })
//   })
// })

var db = factory()

test('open', (t) => {
  db.open((err) => {
    t.end(err)
  })
})

// TODO: drop table
test('crud', (t) => {
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
})

test('close', (t) => {
  db.close((err) => {
    if (err) return t.end(err)
    // idempotent close
    db.close(t.end)
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
