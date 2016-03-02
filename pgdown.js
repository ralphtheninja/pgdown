const inherits = require('inherits')
const pglib = require('pg')
const AbstractLevelDOWN = require('abstract-leveldown/abstract-leveldown')
const NotFoundError = require('level-errors').NotFoundError
const PgIterator = require('./pgiterator')
const PgBatch = require('./pgbatch')
const debug = require('debug')('pgdown')

AbstractLevelDOWN.prototype._serializeKey = function (key) {
  return this._isBuffer(key) ? key
    : key == null ? '' : String(key)
}

AbstractLevelDOWN.prototype._serializeValue = function (value) {
  return this._isBuffer(value) || process.browser ? value
    : value == null ? '' : String(value)
}

function PgDOWN (location) {
  if (!(this instanceof PgDOWN)) {
    return new PgDOWN(location)
  }

  AbstractLevelDOWN.call(this, location)

  debug('# PgDOWN (location = %j)', location)
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
  // this._schema = parts.length ? PgDOWN._escape(parts.join('__')) : ''

  // set qualified name
  this._qname = PgDOWN._escape(this._table)
  // TODO: if (this._schema) qname = this._schema + '.' + this._table, escaped

  // TODO: surface default `public` schema in opts?

  // create a unique id to isolate connection pool to this specific db instance
  // TODO: something less shite
  pg.poolId = pg.poolId || ('' + Math.random()).slice(2)

  debug('pg options: %j, defaults: %j', pg, defaults)

  this._pool = pglib.pools.getOrCreate(pg)

  // this._pool.on('error', (err) => {
  //   debug('WTF POOL ERROR', err)
  // })
}

inherits(PgDOWN, AbstractLevelDOWN)

// TODO: proper plsql ident formatting
PgDOWN._escape = (name) => '"' + name.replace(/\"/g, '""') + '"'

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

PgDOWN.prototype._open = function (options, cb) {
  debug('## _open (options = %j, cb = %s)', options, !!cb)

  const pg = this.pg

  // verify that database name in options matches the one we're connecting to
  if (options.database && options.database !== pg.databasae) {
    throw new Error('specified database does not match db location')
  }

  // copy over pg other options
  PG_CONFIG_KEYS.forEach((key) => {
    if (options[key] !== undefined) pg[key] = options[key]
  })

  debug('_open: pg config: %j', pg)

  const createIfMissing = options.createIfMissing
  const errorIfExists = options.errorIfExists
  const IF_NOT_EXISTS = errorIfExists ? '' : 'IF NOT EXISTS'
  const qname = this._qname
  var sql = ''

  if (errorIfExists || !createIfMissing) {
    // TODO: find a cleaner way to do this
    sql += `
      SELECT COUNT(*) from ${qname} LIMIT 1;
    `
  }

  // create associated schema along w/ table, if specified
  if (createIfMissing && this._schema) {
    sql += `
      CREATE SCHEMA ${IF_NOT_EXISTS} ${PgDOWN._escape(this._schema)};
    `
  }

  if (createIfMissing) {
    // TODO: support for jsonb, bytea using _serialize[Key|Value]
    const kType = 'bytea'
    const vType = 'bytea'
    sql += `
      CREATE TABLE ${IF_NOT_EXISTS} ${qname} (
        key ${kType} PRIMARY KEY,
        value ${vType}
      );
    `
  }

  const pool = this._pool
  pool.acquire((err, client) => {
    if (err) return cb(err)

    debug('_open: sql: %s', sql)
    client.query(sql, (err, result) => {
      debug('_open: pg client result: %j, %j', err, result)
      err ? pool.destroy(client) : pool.release(client)

      if (!err && !createIfMissing && errorIfExists) {
        err = new Error('table exists: ' + qname)
      }

      cb(err || null)
    })
  })
}

PgDOWN.prototype._get = function (key, options, cb) {
  debug('## _get (key = %j, options = %j, cb = %s)', key, options, !!cb)

  // TODO: most efficient way to disable jsonb field parsing in pg lib?
  const sql = `SELECT value FROM ${this._qname} WHERE (key)=$1`
  const params = [ key ]
  debug('_get: sql %s %j', sql, params)

  const pool = this._pool
  pool.acquire((err, client) => {
    if (err) return cb(err)

    client.query(sql, params, (err, result) => {
      debug('get result: %j, %j', err, result)
      err ? pool.destroy(client) : pool.release(client)

      if (err) {
        cb(err)
      } else if (result.rowCount) {
        cb(null, PgIterator._deserialize(result.rows[0].value, options.asBuffer))
      } else {
        // TODO: better error message?
        cb(new NotFoundError('not found: ' + key))
      }
    })
  })
}

PgDOWN.prototype._put = function (key, value, options, cb) {
  debug('## _put (key = %j, value = %j, options = %j, cb = %s)', key, value, options, !!cb)

  const pool = this._pool
  pool.acquire((err, client) => {
    if (err) return cb(err)

    PgBatch._commands.put(client, this._qname, key, value, (err) => {
      err ? pool.destroy(client) : pool.release(client)
      cb(err || null)
    })
  })
}

PgDOWN.prototype._del = function (key, options, cb) {
  debug('## _del (key = %j, options = %j, cb = %s)', key, options, !!cb)

  const pool = this._pool
  pool.acquire((err, client) => {
    if (err) return cb(err)

    PgBatch._commands.del(client, this._qname, key, (err) => {
      err ? pool.destroy(client) : pool.release(client)
      cb(err || null)
    })
  })
}

PgDOWN.prototype._chainedBatch = function () {
  debug('## _chainedBatch ()')
  return new PgBatch(this)
}

PgDOWN.prototype._iterator = function (options) {
  debug('## _iterator (options = %j)', options)
  return new PgIterator(this, options)
}

PgDOWN.prototype._close = function (cb) {
  debug('## _close (cb = %s)', !!cb)

  const pool = this._pool
  // debug('_close: draining client pool: %j', pool)
  // pool.drain(() => { ...
  debug('_close: destroying pool resources')
  pool.destroyAllNow(cb)
}

PgDOWN.prototype._drop = function (cb) {
  debug('## _drop (cb = %s)', !!cb)

  const pool = this._pool
  pool.acquire((err, client) => {
    if (err) return cb(err)

    client.query(`DROP TABLE ${this._qname}`, (err) => {
      err ? pool.destroy(client) : pool.release(client)
      cb && cb(err || null)
    })
  })
}

module.exports = PgDOWN
