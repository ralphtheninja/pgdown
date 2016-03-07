'use strict'

module.exports = require('abstract-leveldown/abstract/iterator-test')

// NB: updates snapshot test to wait for snapshot acquisition before write
module.exports.snapshot = function (leveldown, test, testCommon) {
  var db

  test('setUp #3', function (t) {
    db = leveldown(testCommon.location())
    db.open(function () {
      db.put('foobatch1', 'bar1', t.end.bind(t))
    })
  })

  test('iterator create snapshot correctly', function (t) {
    t.timeoutAfter(2000)
    var iterator = db.iterator()

    // NB: this is pretty shite -- for now just a hack for testing
    setTimeout(function () {
      db.del('foobatch1', function () {
        iterator.next(function (err, key, value) {
          t.error(err)
          t.ok(key, 'got a key')
          t.equal(key.toString(), 'foobatch1', 'correct key')
          t.equal(value.toString(), 'bar1', 'correct value')
          iterator.end(function (err) {
            if (err) return t.end(err)
            db.close(t.end)
          })
        })
      })
    }, 100)
  })
}
