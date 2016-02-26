const inherits = require('inherits')
const through2 = require('through2')
const pg = require('pg')
const QueryStream = require('pg-query-stream')
const AbstractLevelDOWN = require('abstract-stream-leveldown').AbstractStreamLevelDOWN
const errors = require('level-errors')
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

    // TODO: is this necessary?
    // client.on('error', (err) => {
    //   debug('CLIENT ERROR EVENT: %j', err)
    //   release(client)
    // })

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

      // key text CONSTRAINT idx_key PRIMARY KEY
      // TODO: `value jsonb` for 'json' valueEncoding
      const tableSql = `
        CREATE TABLE ${ifNotExists}${this.pg.table} (
          key text PRIMARY KEY,
          value text
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

PgDOWN.prototype._createWriteStream = function (options) {
  const table = this.pg.table
  var client

  const ts = through2.obj((op, enc, cb) => {
    if (client) write(op, cb)

    debug('initializing write stream')
    this._connect((err, _client, release) => {
      if (err) return cb(err)

      client = _client
      ts.once('error', release).once('end', release)

      client.query('BEGIN', (err) => {
        if (err) return cb(err)
        write(op, cb)
      })
    })
  }, (cb) => {
    debug('committing batch')
    submit(null, cb)
  })

  const write = (op, cb) => {
    // TODO: merge op w/ options
    const type = op.type || (op.value == null ? 'del' : 'put')
    const command = PgDOWN.operation[type]
    try {
      command(client, table, op, cb)
    } catch (err) {
      cb(err)
    }
  }

  const submit = (err, cb) => {
    const action = err ? 'ROLLBACK' : 'COMMIT'
    debug('batch submit action: %s', action)

    client.query(action, (dbErr) => {
      if (dbErr) debug('batch %s error: %j', action, dbErr)
      else debug('batch %s successful', action)
      cb(dbErr || err)
    })
  }

  return ts
}

PgDOWN.comparator = {
  lt: '<',
  lte: '<=',
  gte: '>=',
  gt: '>',
  eq: '=',
  ne: '<>'
}

// TODO: sanitization
function formatConstraints (constraints) {
  // handle `or` clauses
  if (Array.isArray(constraints)) {
    return '(' + constraints.map(formatConstraints).join(') OR (') + ')'
  }

  const clauses = []
  const operators = PgDOWN.comparator
  for (var k in constraints) {
    const v = constraints[k]
    const op = operators[k]
    if (op) {
      clauses.push(`key${op}(${v})`)
    } else if (op === 'or') {
      // TODO: just being lazy, but should fix up extra array wrapping cruft
      clauses.push(formatConstraints([ constraints[k] ]))
    }
  }

  return clauses.filter(Boolean).join(' AND ')
}

PgDOWN.prototype._createReadStream = function (options) {
  options = options || {}

  this._count = 0
  this._limit = isNaN(options.limit) ? -1 : options.limit
  this._reverse = !!options.reverse
  this._constraints = formatConstraints(options)

  const clauses = []
  const args = []

  clauses.push(`SELECT key, value::text FROM ${this.pg.table}`)

  if (this._constraints) {
    args.push(this._constraints)
    clauses.push('WHERE $' + args.length)
  }

  clauses.push('ORDER BY key ' + (this._reverse ? 'DESC' : 'ASC'))

  if (this._limit.limit >= 0) {
    args.push(this._limit)
    clauses.push('LIMIT $' + args.length)
  }

  // TODO: any reason not to add this?
  // if (options.offset > 0) {
  //   args.push(options.offset)
  //   clauses.push('OFFSET $' + args.length)
  // }

  const sql = clauses.join(' ')
  debug('query stream sql: %s %j', sql, args)

  const query = new QueryStream(sql, args)
  const ts = through2.obj()

  this._connect((err, client, release) => {
    if (err) return ts.destroy(err)
    // create stream, release the client when stream is finished
    client.query(query).on('error', release).on('end', release).pipe(ts)
  })

  return ts
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
