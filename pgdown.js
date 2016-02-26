const inherits = require('inherits')
const after = require('after')
const pg = require('pg')
const AbstractLevelDOWN = require('abstract-leveldown').AbstractLevelDOWN
const errors = require('level-errors')
const PgIterator = require('./pgiterator')
const debug = require('debug')('pgdown')

// const SQL = require('pg-template-tag')

function PgDOWN (location) {
  if (!(this instanceof PgDOWN)) {
    return new PgDOWN(location)
  }

  debug('location %s', location)

  const parts = location.split('/')
  if (location[0] === '/') {
    // it a location begins with a slash it specifies dbname
    parts.shift()
    this._database = parts.shift()
  }

  // the last component of location specifies a table name
  this._table = parts.pop()
  if (!this._table) throw new Error('location must specify a table name')

  // remaining components represent schema namespace
  this._schema = parts.join('__') || null

  debug('this.pg: %j', this.pg)

  // TODO: fix the sql to allow us to use extra path parts for schema name
  if (this._schema) throw new Error('schema paths NYI')

  AbstractLevelDOWN.call(this, location)
}

inherits(PgDOWN, AbstractLevelDOWN)

PgDOWN.prototype._connect = function (cb) {
  debug('_connect: connecting: %j', this.pg.config)
  pg.connect(this.pg.config, (err, client, release) => {
    if (err) {
      release(client)
      return cb(err)
    }

    client.on('error', (err) => {
      debug('CLIENT ERROR EVENT: %j', err)
      // TODO: is this necessary?
      // release(client)
    })

    cb(null, client, release)
  })
}

// TODO: binary? parseInt8?
const PG_KEYS = [
  'user',
  'password',
  'host',
  'port',
  'ssl',
  'rows',
  'poolSize',
  'poolIdleTimeout',
  'reapIntervalMillis'
]

PgDOWN.prototype._open = function (options, cb) {
  this.pg = {}
  const config = this.pg.config = {}

  // throw if database specified in options
  // TODO: InitializationError?
  if (options.database) throw new Error('database specified as db location')
  config.database = this._database

  // TODO: escapement
  if (this._schema) {
    this.pg.schema = `"${this._schema}"`
    this.pg.table = `"${this._schema}"."${this._table}"`
  } else {
    this.pg.table = `"${this._table}"`
  }

  // copy over pg other options
  PG_KEYS.forEach((key) => {
    if (options[key] !== undefined) config[key] = options[key]
  })

  // create a unique id to isolate connection pool to this specific db instance
  // TODO: something less shite
  config.poolId = config.poolId || Math.random()

  debug('_open: pg config: %j', config)

  const errorIfExists = options.errorIfExists

  const sql = (() => {
    if (options.createIfMissing) {
      const ifNotExists = errorIfExists ? '' : 'IF NOT EXISTS '
      const schemaSql = this.pg.schema && `CREATE SCHEMA ${ifNotExists}${this.pg.schema};`

      // key text CONSTRAINT idx_key PRIMARY KEY,
      const tableSql = `
        CREATE TABLE ${ifNotExists}${this.pg.table} (
          key text PRIMARY KEY,
          value jsonb
        );
      `

      // create table and associated schema, if specified
      return (schemaSql || '') + tableSql
    }

    // asserts table existence
    if (errorIfExists) return `SELECT COUNT(*) from ${this.pg.table} LIMIT 1`
  })()

  this._connect((err, client, release) => {
    if (err) return cb(err)

    const done = (err) => {
      if (err) debug('_open: client.query error %j', err)
      release(err)
      cb(err)
    }

    debug('_open: client.query sql: %j', sql)
    sql ? client.query(sql, done) : done()
  })
}

PgDOWN.prototype._close = function (cb) {
  debug('_close: ending client')

  if (this._closed) return process.nextTick(cb)

  // clean up pool
  const pool = pg.pools.getOrCreate(this.pg.config)
  debug('_close: draining pool: %j', pool)

  pool.drain(() => {
    debug('_close: destroying all pooled resources')
    pool.destroyAllNow()
    debug('_close: pool destroyed')
    this._closed = true
    cb()
  })
}

function _putSql (table, op) {
  const INSERT = `INSERT INTO ${table} (key,value) VALUES($1,$2)`
  const UPSERT = INSERT + ' ON CONFLICT (key) DO UPDATE SET value=excluded.value'
  // const UPDATE = `UPDATE ${table} SET value=($2) WHERE key=($1)'`

  // always do an upsert for now
  return UPSERT

  // TODO: the below is a total shitshow -- probably not even worth bothering w/
  // this tries to squeeze INSERT and UPDATE semantics from existing level opts
  // would errorIfMissing be more sensical?

  // if errorIfExists == false:
    // if createIfMissing == true:
      // INSERT
    // else:
      // UPDATE
  // else if errorIfExists == true:
    // if createIfMissing == false:
      // error: bad params
    // else:
      // INSERT
  // else:
    // if createIfMissing == false:
      // UPDATE
    // else:
      // UPSERT
}

PgDOWN.operation = {}

PgDOWN.operation.put = function (client, table, op, cb) {
  const sql = _putSql(table, op)
  const args = [ op.key, op.value ]
  debug('put sql: %s %j', sql, args)

  client.query(sql, args, function (err) {
    if (err) debug('put error: %j', err)
    // TODO: errors.WriteError?
    cb(err || null)
  })
}

PgDOWN.operation.del = function (client, table, op, cb) {
  const sql = `DELETE FROM ${table} WHERE key = $1`
  const args = [ op.key ]
  debug('del sql: %s %j', sql, args)

  client.query(sql, [ op.key ], (err, result) => {
    // TODO: reflect whether or not a row was deleted? errorIfMissing?
    //   if (op.errorIfMissing && !result.rows.length) throw ...

    if (err) debug('del error: %j', err)
    // TODO: errors.WriteError?
    cb(err || null)
  })
}

PgDOWN.prototype._put = function (key, value, opts, cb) {
  const table = this.pg.table
  const op = { type: 'put', key: key, value: value }
  // TODO: merge op, opts?

  this._connect((err, client, release) => {
    if (err) return cb(err)

    PgDOWN.operation.put(client, table, op, (err) => {
      release()
      cb(err)
    })
  })
}

PgDOWN.prototype._del = function (key, opts, cb) {
  const table = this.pg.table
  const op = { type: 'del', key: key }
  // TODO: merge op, opts?

  this._connect((err, client, release) => {
    if (err) return cb(err)

    PgDOWN.operation.del(client, table, op, (err) => {
      release()
      cb(err)
    })
  })
}

PgDOWN.prototype._get = function (key, opts, cb) {
  const table = this.pg.table
  // TODO: most efficient way to disable jsonb field parsing in pg lib?
  const sql = `SELECT value::text FROM ${table} WHERE (key)=$1`
  const args = [ key ]
  debug('get sql: %s %j', sql, args)

  this._connect((err, client, release) => {
    if (err) return cb(err)

    client.query(sql, args, (err, result) => {
      release()
      if (err) cb(err)
      else if (result.rows.length) cb(null, result.rows[0].value)
      else cb(new errors.NotFoundError('key: ' + key)) // TODO: better message
    })
  })
}

PgDOWN.prototype._chainedBatch = function () {
  throw new Error('Not Yet Implemented')
}

PgDOWN.prototype._batch = function (ops, options, cb) {
  const table = this.pg.table

  // TODO: // grab a fresh client from the pool for batch ops
  this._connect((err, client, release) => {
    if (err) return cb(err)

    const done = after(ops.length, (err) => {
      if (err) {
        debug('batch commit error: %j', err)
        client.query('ROLLBACK', (txErr) => {
          release(txErr)

          // if rollback fails something's really screwed
          if (txErr) debug('transaction rollback error: %j', txErr)
          else debug('transaction rollback successful')

          cb(err || null)
        })
      } else {
        debug('committing batch')
        client.query('COMMIT', (txErr) => {
          release(txErr)

          if (txErr) debug('transaction commit error: %j', txErr)
          else debug('transaction commit successful')

          cb(txErr || null)
        })
      }
    })

    client.query('BEGIN', (err) => {
      if (err) return done(err)

      // generate statement sql for each batch op
      for (var i = 0, len = ops.length; i < len; i++) {
        const op = ops[i]
        if (!op) continue

        // TODO: merge op w/ options
        const command = PgDOWN.operation[op.type]
        if (command) {
          command(client, table, op, done)
        } else {
          return done(new Error('unknown operation type: ' + op.type))
        }
      }
    })
  })
}

PgDOWN.prototype._iterator = function (options) {
  return new PgIterator(this, options)
}

PgDOWN.prototype.drop = function (options, cb) {
  if (typeof options === 'function') {
    cb = options
    options = {}
  }

  this._connect((err, client, release) => {
    if (err) return cb(err)

    // const ifExists = options.errorIfExists ? '' : ' IF EXISTS'
    const ifExists = ''

    client.query(`DROP TABLE ${ifExists}${this.pg.table}`, (err) => {
      release()
      cb(err || null)
    })
  })
}

module.exports = PgDOWN
