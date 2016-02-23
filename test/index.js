const test = require('tape')
const postgresdown = require('../')
const levelup = require('levelup')

function factory (uri) {
  return levelup(uri, {
    db: postgresdown,
    keyEncoding: 'utf8',
    valueEncoding: 'json'
  })
}

const uri = 'postgres://dlandolt:@localhost:5432/postgresdown'
const db = factory(uri)

test('open', (t) => {
  // TODO
  db.open(function (err) {
    t.end(err)
  })
  // client.connect((err) => {
  //   if (err) return t.end(err)

  //   client.query('SELECT NOW() AS "time"', (err, result) => {
  //     if (err) return t.end(err)

  //     t.equal(result.rows.length, 1)
  //     t.ok(result.rows[0].time instanceof Date)

  //     t.end()
  //   })
  // })
})

// TODO: drop table

test('put', (t) => {
  db.put('a', { str: 'foo', int: 123 }, function (err, result) {
    if (err) return t.end(err)
    t.ok(result == null, 'empty response')
    t.end()
  })
})

test('get', (t) => {
  db.get('a', function (err, result) {
    if (err) return t.end(err)
    t.deepEqual(result, { str: 'foo', int: 123 })
    t.end()
  })
})

test('del', (t) => {
  db.del('a', function (err, result) {
    if (err) return t.end(err)
    db.get('a', function (err, result) {
      t.ok(err && err.notFound, 'not found')
      t.ok(result == null, 'empty response')
      t.end()
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
