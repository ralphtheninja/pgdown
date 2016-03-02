const inherits = require('inherits')
const AbstractChainedBatch = require('abstract-leveldown/abstract-chained-batch')
const debug = require('debug')('pgdown')

function PgBatch(db) {
  AbstractChainedBatch.call(this, db)

  this._connecting = true
  db._pool.connect((err, client) => {
    this._connecting = false

    if (err) return this._error = err
    this._client = client

    client.query('BEGIN', (err) => {
      debug('PgBatch - begin transaction, %j', err)
      if (err) throw err
      this._flush()
    })
  })
}

inherits(PgBatch, AbstractChainedBatch)

PgBatch.prototype._flush = function () {
  if (this._error) throw this._error

  const qname = this._db.qname
  const client = this._client
  if (client) {
    var op
    while (op = this._operations.shift()) {
      const type = op.type || (op.value == null ? 'del' : 'put')
      const command = PgBatch._commands[type]
      if (!command) throw new Error('Unknown operation in batch: ' + type)
      command(client, qname, op, (err) => {
        if (err) this._error = err
      })
    }
  }
}

PgBatch.prototype._put = function (key, value) {
  this._operations.push({ key: key })
  this._flush()
}

PgBatch.prototype._del = function (key) {
  this._operations.push({ key: key })
  this._flush()
}

PgBatch.prototype._clear = function () {
  if (this._error) throw this._error

  if (!this._client) return 

  // roll back and begin a new transaction
  this._client.query('ROLLBACK; BEGIN', (err) => {
    if (err) this._error = err
  })
}

PgBatch.prototype._write = function (cb) {
  this._flush()

  if (!this._client) {
    // nothing to do
    process.nextTick(cb)
  } else {
    // commit transaction
    client.query('COMMIT', (err) => {
      cb(err || null)
    })
  }
}

function _putSql (qname, op) {
  const INSERT = `INSERT INTO ${qname} (key,value) VALUES($1,$2)`
  const UPSERT = INSERT + ' ON CONFLICT (key) DO UPDATE SET value=excluded.value'
  // const UPDATE = `UPDATE ${qname} SET value=($2) WHERE key=($1)'`

  // always do an upsert for now
  return UPSERT
}

PgBatch._commands = {}

PgBatch._commands.put = function (client, qname, key, value, cb) {
  const sql = _putSql(qname, key, value)
  const params = [ key, value ]
  debug('put command sql: %s %j', sql, params)

  client.query(sql, params, function (err) {
    if (err) debug('put command: error: %j', err)
    cb(err || null)
  })
}

PgBatch._commands.del = function (client, qname, key, cb) {
  const sql = `DELETE FROM ${qname} WHERE (key) = $1`
  const params = [ key ]
  debug('del command sql: %s %j', sql, params)

  client.query(sql, params, (err) => {
    if (err) debug('del command: error: %j', err)
    cb(err || null)
  })
}

// PgDOWN.prototype._createWriteStream = function (options) {
//   debug('## _createWriteStream(options = %j)', options)
//   const qname = this._qname
//   var client

//   const ts = through2.obj((op, enc, cb) => {
//     if (client) return push(op, cb)

//     debug('_createWriteStream: initializing write stream')
//     this._connect((err, _client, release) => {
//       if (err) return cb(err)

//       client = _client
//       ts.once('error', (err) => {
//         debug('_createWriteStream: stream err: %j', err)
//         release(err)
//       }).once('end', () => {
//         debug('_createWriteStream: stream ended')
//         release()
//       })

//       client.query('BEGIN', (err) => {
//         debug('_createWriteStream: begin transaction')
//         if (err) return cb(err)
//         push(op, cb)
//       })
//     })
//   }, (cb) => {
//     debug('_createWriteStream: committing batch')
//     submit(null, cb)
//   })

//   const push = (op, cb) => {
//     debug('_createWriteStream: batch command: %j', op)
//     const type = op.type || (op.value == null ? 'del' : 'put')
//     const command = PgBatch._commands[type]

//     if (!command) throw new Error('Unknown batch command: ' + type)

//     // TODO: try/catch around op?
//     command(client, qname, op, cb)
//   }

//   const submit = (err, cb) => {
//     const action = err ? 'ROLLBACK' : 'COMMIT'
//     debug('_createWriteStream: submitting batch for %s', action)

//     // noop if no client, as no batch has been started
//     if (!client) return

//     client.query(action, (dbErr) => {
//       if (dbErr) debug('_createWriteStream: batch %s error: %j', action, dbErr)
//       else debug('_createWriteStream: batch %s successful', action)
//       cb(dbErr || err)
//     })
//   }

//   return ts
// }

module.exports = PgBatch
