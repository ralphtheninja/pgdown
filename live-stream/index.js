'use strict'

// const util = require('../util')

const liveStream = module.exports = function (db, opts) {

  // if !db._listener ...
  // const client = db._listener = util.createConnection(db._config)
  // client.connect()
  // client.query('LISTEN pgdown.table_name')
  // client.on('notification', function (data) {
  //   // ...
  // })

  // also, client.query('UNLISTEN pgdown_changes.table_name')
}

module.exports.install = function (db) {
  db.methods = db.methods || {}
  db.methods['liveStream'] =
  db.methods['createLiveStream'] = { type: 'readable' }

  db.createLiveStream = db.liveStream = function (opts) {
    return liveStream(db, opts)
  }
}
