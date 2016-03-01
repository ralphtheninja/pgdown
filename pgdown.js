const inherits = require('inherits')
const NotFoundError = require('level-errors').NotFoundError
const pglib = require('pg')
const QueryStream = require('pg-query-stream')
const through2 = require('through2')

const ASL = require('abstract-stream-leveldown')
const AbstractStreamLevelDOWN = ASL.AbstractStreamLevelDOWN
const AbstractStreamChainedBatch = ASL.AbstractStreamChainedBatch

// const SQL = require('pg-template-tag')

const debug = require('debug')('pgdown')

function PgDOWN (location) {
  if (!(this instanceof PgDOWN)) {
    return new PgDOWN(location)
  }

  AbstractStreamLevelDOWN.call(this, location)

  debug('# constructor(location = %j)', location)
  const parts = location.split('/')

  // last component of location specifies table name
  const table = this._table = parts.pop()
  if (!table) throw new Error('location must specify table name')

  const defaults = pglib.defaults
  const pg = this.pg = {}

  // if location begins with a slash it specifies database name
  if (location[0] === '/') {
    parts.shift()
    pg.database = parts.shift()
  }

  pg.database = pg.database || defaults.database || 'postgres'

  // NB: this will eventually allow us to support subleveling natively
  // TODO: use extra path parts for schema name
  if (parts.length) throw new Error('schema paths NYI')

  // remaining components represent schema namespace
  // this._schema = parts.length ? this._escape(parts.join('__')) : ''

  // set qualified name
  this._qname = this._escape(this._table)
  // TODO: if (this._schema) qname = this._schema + '.' + this._table, escaped

  // TODO: surface default `public` schema in opts?

  debug('pg options: %j, defaults: %j', pg, defaults)
}

inherits(PgDOWN, AbstractStreamLevelDOWN)

// TODO: investigate this...
PgDOWN.prototype._escape = function (name) {
  return '"' + name.replace(/\"/g, '""') + '"'
}

PgDOWN.prototype._connect = function (cb) {
  debug('_connect: pg options: %j', this.pg)
  pglib.connect(this.pg, (err, client, release) => {
    if (err) {
      release(client)
      return cb(err)
    }

    // TODO: is this necessary?
    client.on('error', (err) => {
      debug('CLIENT ERROR EVENT: %j', err)
      // release(client)
      // NB: rethrowing for now, to sniff out when this gets invoked
      throw err
    })

    cb(null, client, release)
  })
}

// TODO: binary? parseInt8?
const PG_CONFIG_KEYS = [
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
  debug('## _open(options = %j, cb = %s)', options, !!cb)

  const pg = this.pg

  // verify that database name in options matches the one we're connecting to
  if (options.database && options.database !== pg.databasae) {
    throw new Error('specified database does not match db location')
  }

  // copy over pg other options
  PG_CONFIG_KEYS.forEach((key) => {
    if (options[key] !== undefined) pg[key] = options[key]
  })

  // create a unique id to isolate connection pool to this specific db instance
  // TODO: something less shite
  pg.poolId = pg.poolId || ('' + Math.random()).slice(2)

  debug('_open: pg config: %j', pg)

  const errorIfExists = options.errorIfExists

  const sql = (() => {
    if (options.createIfMissing) {
      const ifNotExists = errorIfExists ? '' : 'IF NOT EXISTS '

      // TODO: support for jsonb, bytea using _serialize[Key|Value]
      const kType = 'bytea'
      const vType = 'bytea'
      const table = this._escape(this._table)
      const tableSql = `
        CREATE TABLE ${ifNotExists}${table} (
          key ${kType} PRIMARY KEY,
          value ${vType}
        );
      `

      if (!this._schema) return tableSql

      // create associated schema along w/ table, if specified
      const schema = this._escape(schema)
      return `CREATE SCHEMA ${ifNotExists}${schema}; ` + tableSql
    }

    // asserts table existence
    if (errorIfExists) return `SELECT COUNT(*) from ${this._qname} LIMIT 1;`
  })()

  this._connect((err, client, release) => {
    if (err) return cb(err)

    const done = (err) => {
      if (err) debug('_open: pg client error: %j', err)
      release(err)
      cb(err)
    }

    debug('_open: sql: %j', sql)
    sql ? client.query(sql, done) : done()
  })
}

function _putSql (qname, op) {
  const INSERT = `INSERT INTO ${qname} (key,value) VALUES($1,$2)`
  const UPSERT = INSERT + ' ON CONFLICT (key) DO UPDATE SET value=excluded.value'
  // const UPDATE = `UPDATE ${qname} SET value=($2) WHERE key=($1)'`

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
  debug('## _get(key = %j, options = %j, cb = %s)', key, options, !!cb)

  // TODO: most efficient way to disable jsonb field parsing in pg lib?
  const sql = `SELECT value::bytea FROM ${this._qname} WHERE (key::bytea)=$1`
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

PgDOWN.operation.put = function (client, qname, op, options, cb) {
  const sql = _putSql(qname, op)
  const args = [ encode(op.key, op, options), encode(op.value, op, options) ]
  debug('put operation sql: %s %j', sql, args)

  client.query(sql, args, function (err) {
    if (err) debug('put operation: error: %j', err)
    cb(err || null)
  })
}

PgDOWN.operation.del = function (client, qname, op, options, cb) {
  const sql = `DELETE FROM ${qname} WHERE (key::bytea) = $1`
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
  debug('## _put(key = %j, value = %j, options = %j, cb = %s)', key, value, options, !!cb)

  if (typeof cb !== 'function') {
    throw new Error('put() requires a callback argument')
  }

  const op = { type: 'put', key: key, value: value, options: options }

  this._connect((err, client, release) => {
    if (err) return cb(err)

    PgDOWN.operation.put(client, this._qname, op, null, (err) => {
      release()
      cb(err)
    })
  })
}

PgDOWN.prototype._del = function (key, options, cb) {
  debug('## _del(key = %j, options = %j, cb = %s)', key, options, !!cb)

  if (typeof cb !== 'function') {
    throw new Error('del() requires a callback argument')
  }

  const op = { type: 'del', key: key, options: options }

  this._connect((err, client, release) => {
    if (err) return cb(err)

    PgDOWN.operation.del(client, this._qname, op, null, (err) => {
      release()
      cb(err)
    })
  })
}

PgDOWN.prototype._createWriteStream = function (options) {
  debug('## _createWriteStream(options = %j)', options)
  const qname = this._qname
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
    command(client, qname, op, options, cb)
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
  debug('## _chainedBatch # _clear: clearing batch')
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
  debug('## _createReadStream(options = %j)', options)

  this._options = options = options || {}

  this._count = 0
  this._limit = isNaN(options.limit) ? -1 : options.limit
  this._reverse = !!options.reverse
  this._constraints = formatConstraints(options)

  const clauses = []
  const args = []

  clauses.push(`SELECT key::bytea, value::bytea FROM ${this._qname}`)

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

PgDOWN.prototype._close = function (cb) {
  debug('## _close(cb = %s)', !!cb)

  if (this._closed) return process.nextTick(cb)

  const pool = pglib.pools.getOrCreate(this.pg)

  debug('_close: destroying all pooled resources')

  // TODO: try/catch?
  pool.destroyAllNow()
  debug('_close: pool destroyed')

  this._closed = true
  cb && cb()
}

// TODO: 'clear' operation?
PgDOWN.prototype._drop = function (options, cb) {
  debug('## _drop(options = %j, cb = %s)', options, !!cb)
  if (typeof options === 'function') {
    cb = options
    options = {}
  }

  this._connect((err, client, release) => {
    if (err) return cb(err)

    // const ifExists = options.errorIfExists ? '' : ' IF EXISTS'
    const ifExists = ''

    client.query(`DROP TABLE ${ifExists}${this._qname}`, (err) => {
      release()
      cb(err || null)
    })
  })
}

module.exports = PgDOWN
