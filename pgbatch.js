const inherits = require('inherits')
const AbstractChainedBatch = require('abstract-leveldown/abstract-chained-batch')
const debug = require('debug')('pgdown')

function PgBatch (db) {
  AbstractChainedBatch.call(this, db)

  this._qname = db._qname
  this._connecting = true

  const pool = this._pool = db._pool
  pool.acquire((err, client) => {
    this._connecting = false

    if (err) return this._setError(err)
    this._client = client

    client.query('BEGIN', (err) => {
      debug('PgBatch - begin transaction (err = %j)', err)
      this._setError(err)
      this._flush()
    }).on('error', (err) => {
      this._setError(err)
    })
  })
}

inherits(PgBatch, AbstractChainedBatch)

PgBatch.prototype._flush = function () {
  const qname = this._qname
  const client = this._client
  const ops = this._operations

  debug('# PgBatch flush (ops: %s, client: %s, err: %j)', ops.length, !!client, this._error)

  if (client) {
    const commands = PgBatch._commands
    while (ops.length) {
      const op = ops.shift()
      const command = commands[op.type]
      if (command) {
        command(client, qname, op)
      } else {
        const err = new Error('Unknown operation in batch: ' + op.type)
        return this._setError(err)
      }
    }
  }
}

PgBatch.prototype._put = function (key, value) {
  debug('# PgBatch _put(key = %j, value = %j)', key, value)
  this._operations.push({ type: 'put', key: key, value: value })
  this._flush()
}

PgBatch.prototype._del = function (key) {
  debug('# PgBatch _put(key = %j)', key)
  this._operations.push({ type: 'del', key: key })
  this._flush()
}

PgBatch.prototype._clear = function () {
  debug('# PgBatch _clear()')
  if (!this._client) return

  // TODO: if this._error, destroy and recreate client?

  // roll back and begin a new transaction
  const client = this._client
  client.query('ROLLBACK', (err) => {
    if (err) return this._setError(err)

    client.query('BEGIN', (err) => {
      debug('PgBatch - begin transaction (err = %j)', err)
      this._setError(err)
      this._flush()
    }).on('error', (err) => {
      this._setError(err)
    })
  })
}

PgBatch.prototype._write = function (cb) {
  debug('# PgBatch _write(cb = %s)', !!cb)
  try {
    this._flush()
  } catch (err) {
    debug('WRITE FAIL %j', err)
    return process.nextTick(() => cb(err))
  }

  const client = this._client
  if (client) {
    // commit transaction
    client.query('COMMIT', (err) => {
      this._close(err)
      cb(err || null)
    })
  } else {
    // nothing to do
    process.nextTick(cb)
  }
}

PgBatch.prototype._setError = function (err) {
  if (err) {
    debug('# PgBatch error: %j', err)
    if (!this._error) this._error = err
  }
}

PgBatch.prototype._close = function (err) {
  if (!err && !this._error) {
    this._pool.release(this._client)
  } else {
    this._pool.destroy(this._client)
  }
  return err || this._error
}

PgBatch._commands = {}

PgBatch._commands.put = function (client, qname, op, cb) {
  const INSERT = `INSERT INTO ${qname} (key,value) VALUES($1,$2)`
  const UPSERT = INSERT + ' ON CONFLICT (key) DO UPDATE SET value=excluded.value'
  // const UPDATE = `UPDATE ${qname} SET value=($2) WHERE key=($1)'`

  // always an upsert for now
  const sql = UPSERT
  const params = [ op.key, op.value ]
  debug('put command sql: %s %j', sql, params)

  return client.query(sql, params, cb)
}

PgBatch._commands.del = function (client, qname, op, cb) {
  const sql = `DELETE FROM ${qname} WHERE (key) = $1`
  const params = [ op.key ]
  debug('del command sql: %s %j', sql, params)

  return client.query(sql, params, cb)
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
