#!/usr/bin/env node

const pg = require('pg')
const PostgresDOWN = require('../')
const after = require('after')
const db = PostgresDOWN(require('../test/rc').uri)

db.open({ createIfMissing: true }, function (err) {
  if (err) return console.error('failed to open db', err)
  const data = generateData()
  var done = after(data.length, function (err) {
    if (err) return console.error('failed to create test data', err)
    console.log('added %d elements', data.length)
    db.close(function (err) {
      if (err) return console.error('failed to close db', err)
      console.log('db closed successfully')
    })
  })
  data.forEach(function (d) {
    db.put(d.key, d.value, done)
  })
})

function generateData () {
  var data = []
  for (var i = 0; i < 100; i++) {
    data.push({
      key: i,
      value: {
        foo: 'bar' + i,
        random: Math.round(Math.random() * 1000)
      }
    })
  }
  return data
}

