const inherits = require('inherits')
const through2 = require('through2')
const pg = require('pg')
const QueryStream = require('pg-query-stream')
const NotFoundError = require('level-errors').NotFoundError
const ASL = require('abstract-stream-leveldown')
const AbstractStreamLevelDOWN = ASL.AbstractStreamLevelDOWN
const AbstractStreamChainedBatch = ASL.AbstractStreamChainedBatch
const debug = require('debug')('pgdown')

// const SQL = require('pg-template-tag')

const escapedName = (name) => '"' + name.replace(/\"/g, '""') + '"'

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

  AbstractStreamLevelDOWN.call(this, location)
}

inherits(PgDOWN, AbstractStreamLevelDOWN)

PgDOWN.prototype._connect = function (cb) {
  debug('## connect: connecting: %j', this.pg.config)
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

// PgDOWN.prototype._getOptions

PgDOWN.prototype._open = function (options, cb) {
  debug('## open(options=%j)', options)

  this.pg = {}
  const config = this.pg.config = {}

  // throw if database specified in options
  // TODO: InitializationError?
  if (options.database) throw new Error('database specified as db location')
  config.database = this._database

  this.pg.table = escapedName(this._table)
  if (this._schema) {
    this.pg.table = escapedName(this.pg._schema) + '.' + this.pg.table
  }

  // copy over pg other options
  PG_KEYS.forEach((key) => {
    if (options[key] !== undefined) config[key] = options[key]
  })

  // create a unique id to isolate connection pool to this specific db instance
  // TODO: something less shite
  config.poolId = config.poolId || Math.random()

  debug('## open: pg config: %j', config)

  const errorIfExists = options.errorIfExists

  const sql = (() => {
    if (options.createIfMissing) {
      const ifNotExists = errorIfExists ? '' : 'IF NOT EXISTS '
      const schemaSql = this.pg.schema && `CREATE SCHEMA ${ifNotExists}${this.pg.schema};`

      // TODO: jsonb, bytea
      const tableSql = `
        CREATE TABLE ${ifNotExists}${this.pg.table} (
          key bytea PRIMARY KEY,
          value bytea
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
  debug('## close(cb=%s)', !!cb)

  if (this._closed) return process.nextTick(cb)
  debug('_close: ending client')

  const pool = pg.pools.getOrCreate(this.pg.config)
  debug('_close: draining pool: %j', pool)

  pool.drain(() => {
    debug('_close: destroying all pooled resources')
    pool.destroyAllNow()
    debug('_close: pool destroyed')
    this._closed = true
    cb && cb()
  })
}

function _putSql (table, op) {
  const INSERT = `INSERT INTO ${table} (key,value) VALUES($1,$2)`
  const UPSERT = INSERT + ' ON CONFLICT (key) DO UPDATE SET value=excluded.value'
  // const UPDATE = `UPDATE ${table} SET value=($2) WHERE key=($1)'`

  // always do an upsert for now
  return UPSERT
}

// NB: expects string values from pg for now
const decodeKey = (source, options) => {
  debug('decodeKey: %j, options: %j', source, options)

  if (!options || (options.asBuffer !== false && options.keyAsBuffer !== false)) {
    return source
  }
  return String(source || '')
}

const decodeValue = (source, options) => {
  debug('decodeValue: %j, options: %j', source, options)

  if (!options || (options.asBuffer !== false && options.valueAsBuffer !== false)) {
    return source
  }
  return String(source || '')
}

const isBuffer = PgDOWN.prototype._isBuffer

// NB: stringify everything going into pg for now
const encode = (source, options, batchOptions) => {
  debug('encode: %j options: %j %j', source, options, batchOptions)

  // if (source == null) return source

  if (isBuffer(source)) return source
  else return new Buffer(source || '', 'utf8')
}

PgDOWN.prototype._get = function (key, options, cb) {
  debug('## get(key=%j, options=%j, cb=%s)', key, options, !!cb)

  const table = this.pg.table
  // TODO: most efficient way to disable jsonb field parsing in pg lib?
  const sql = `SELECT value::bytea FROM ${table} WHERE (key::bytea)=$1`
  const args = [ encode(key, options) ]
  debug('_get: sql %s %j', sql, args)

  this._connect((err, client, release) => {
    if (err) return cb(err)

    client.query(sql, args, (err, result) => {
      release()
      if (err) cb(err)
      else if (result.rows.length) cb(null, decodeValue(result.rows[0].value, options))
      else cb(new NotFoundError('not found: ' + key)) // TODO: better message
    })
  })
}

PgDOWN.operation = {}

PgDOWN.operation.put = function (client, table, op, options, cb) {
  const sql = _putSql(table, op)
  const args = [ encode(op.key, op, options), encode(op.value, op, options) ]
  debug('put operation sql: %s %j', sql, args)

  client.query(sql, args, function (err) {
    if (err) debug('put operation: error: %j', err)
    cb(err || null)
  })
}

PgDOWN.operation.del = function (client, table, op, options, cb) {
  const sql = `DELETE FROM ${table} WHERE (key::bytea) = $1`
  const args = [ encode(op.key, op, options) ]
  debug('del operation sql: %s %j', sql, args)

  client.query(sql, args, (err, result) => {
    // TODO: reflect whether or not a row was deleted? errorIfMissing?
    //   if (op.errorIfMissing && !result.rows.length) throw ...

    if (err) debug('del operation: error: %j', err)
    cb(err || null)
  })
}

PgDOWN.prototype._put = function (key, value, options, cb) {
  debug('## put(key=%j, value=%j, options=%j, cb=%s)', key, value, options, !!cb)

  if (typeof cb !== 'function') {
    throw new Error('put() requires a callback argument')
  }

  const table = this.pg.table
  const op = { type: 'put', key: key, value: value, options: options }

  this._connect((err, client, release) => {
    if (err) return cb(err)

    PgDOWN.operation.put(client, table, op, null, (err) => {
      release()
      cb(err)
    })
  })
}

PgDOWN.prototype._del = function (key, options, cb) {
  debug('## del(key=%j, options=%j, cb=%s)', key, options, !!cb)

  if (typeof cb !== 'function') {
    throw new Error('del() requires a callback argument')
  }

  const table = this.pg.table
  const op = { type: 'del', key: key, options: options }

  this._connect((err, client, release) => {
    if (err) return cb(err)

    PgDOWN.operation.del(client, table, op, null, (err) => {
      release()
      cb(err)
    })
  })
}

PgDOWN.prototype._createWriteStream = function (options) {
  debug('## createWriteStream(options=%j)', options)
  const table = this.pg.table
  var client

  const ts = through2.obj((op, enc, cb) => {
    if (client) return push(op, cb)

    debug('_createWriteStream: initializing write stream')
    this._connect((err, _client, release) => {
      if (err) return cb(err)

      client = _client
      ts.on('error', (err) => {
        debug('_createWriteStream: stream err: %j', err)
        release(err)
      })
      .on('end', () => {
        debug('_createWriteStream: stream ended')
        release()
      })

      client.query('BEGIN', (err) => {
        debug('_createWriteStream: begin transaction')
        if (err) return cb(err)
        push(op, cb)
      })
    })
  }, (cb) => {
    debug('_createWriteStream: committing batch')
    submit(null, cb)
  })

  const push = (op, cb) => {
    debug('_createWriteStream: write batch op: %j', op)
    const type = op.type || (op.value == null ? 'del' : 'put')
    const command = PgDOWN.operation[type]

    if (!command) throw new Error('Unknown batch operation type: ' + type)

    // TODO: try/catch around op?
    command(client, table, op, options, cb)
  }

  const submit = (err, cb) => {
    const action = err ? 'ROLLBACK' : 'COMMIT'
    debug('_createWriteStream: submitting batch for %s', action)

    // noop if no client, as no batch has been started
    if (!client) return

    client.query(action, (dbErr) => {
      if (dbErr) debug('_createWriteStream: batch %s error: %j', action, dbErr)
      else debug('_createWriteStream: batch %s successful', action)
      cb(dbErr || err)
    })
  }

  return ts
}

// reenable support for 'clear' batch op on chained batch
AbstractStreamChainedBatch.prototype._clear = function () {
  debug('_createWriteStream: clearing batch')
  // TODO

  // // signal that commit has been cleared
  // this.emit('clear')
}

PgDOWN.prototype._chainedBatch = function () {
  // patch ASL's chained batch to add _db property
  const batch = AbstractStreamLevelDOWN.prototype._chainedBatch.call(this)

  // add reference to db expected by AbstractLevelDOWN
  batch._db = this

  return batch
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
      clauses.push(`(key::bytea) ${op} (${v})`)
    } else if (op === 'or') {
      // TODO: just being lazy, but should fix up extra array wrapping cruft
      clauses.push(formatConstraints([ constraints[k] ]))
    }
  }

  return clauses.filter(Boolean).join(' AND ')
}

PgDOWN.prototype._createReadStream = function (options) {
  debug('## createReadStream(options=%j)', options)

  this._options = options = options || {}

  this._count = 0
  this._limit = isNaN(options.limit) ? -1 : options.limit
  this._reverse = !!options.reverse
  this._constraints = formatConstraints(options)

  const clauses = []
  const args = []

  clauses.push(`SELECT key::bytea, value::bytea FROM ${this.pg.table}`)

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
  const ts = through2.obj((d, enc, cb) => {
    d.key = decodeKey(d.key, options)
    d.value = decodeValue(d.value, options)
    cb(null, d)
  })

  this._connect((err, client, release) => {
    if (err) return ts.destroy(err)
    // create stream, release the client when stream is finished
    client.query(query).on('error', release).on('end', release).pipe(ts)
  })

  return ts
}

// TODO: 'clear' operation?
PgDOWN.prototype.drop = function (options, cb) {
  debug('## drop(options=%j, cb=%s)', options, !!cb)
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
