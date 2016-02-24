const url = require('url')
const inherits = require('inherits')
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

  const parsed = url.parse(location)
  debug('parsed %j', parsed)

  const path = parsed.pathname.split('/')
  debug('path %j', path)

  const pgOpts = this.pg = {}

  const dbName = path[1]
  debug('dbName %j', dbName)
  // TODO: errors.InitializationError?
  if (!dbName) throw new Error('location must include a valid dbname')

  const tableName = path.pop() || 'pgdown'
  const schemaName = path.slice(2).join('__')

  // TODO: escapement
  pgOpts.table = `"${tableName}"`
  if (schemaName) {
    pgOpts.schema = `"${schemaName}"`
    pgOpts.id = pgOpts.schema + '.' + pgOpts.table
  } else {
    pgOpts.id = pgOpts.table
  }
  debug('this.pg: %j', pgOpts)

  // TODO: fix the sql to allow us to use extra path parts for schema name
  if (schemaName) throw new Error('schema names NYI')

  // TODO: parse into connection obj
  // {
  //   user: ...,
  //   password: '',
  //   host: 'localhost',
  //   port: 5432,
  //   database: '',
  // }

  // remove schema, table name from uri
  parsed.pathname = '/' + dbName
  debug('uri path', parsed.pathname)

  const uri = url.format(parsed)
  debug('creating pg client with uri', uri)

  // TODO: use pg.pools?
  this.pg.client = new pg.Client(uri)

  AbstractLevelDOWN.call(this, location)
}

inherits(PgDOWN, AbstractLevelDOWN)

PgDOWN.prototype._open = function (options, cb) {
  const client = this.pg.client

  const errorIfExists = options.errorIfExists
  const createIfMissing = options.createIfMissing

  if (createIfMissing) {
    const ifNotExists = errorIfExists ? '' : ' IF NOT EXISTS'

    const schemaSql = this.pg.schema && `
      CREATE SCHEMA${ifNotExists} ${this.pg.schema};
    `

    const tableSql = `
      CREATE TABLE${ifNotExists} ${this.pg.table} (
        key text PRIMARY KEY,
        value jsonb
      );
    `

    const createSql = (schemaSql || '') + tableSql
    debug('_open: createIfMissing sql', createSql)

    client.query(createSql)
  } else if (errorIfExists) {
    // test for table existence
    const existsSql = `SELECT COUNT(*) from ${this.pg.id} LIMIT 1`
    debug('_open: errorIfExists sql', existsSql)

    client.query(existsSql)
  }

  debug('_open: client connecting')
  client.connect((err) => {
    // TODO: errors.InitializationError?
    if (err) debug('_open: client.connect error %j', err)
    cb(err || null)
  })
}

PgDOWN.prototype._close = function (cb) {
  debug('_close: ending client')
  // TODO: can this ever throw?
  try {
    this.pg.client.end()
    process.nextTick(cb)
  } catch (err) {
    debug('_close: error %j', err)
    process.nextTick(function () {
      cb(err)
    })
  }
}

function putCommandSql (db, key, value, options) {
  const INSERT = `INSERT INTO ${db.pg.id} (key,value) VALUES($1,$2)`
  const UPSERT = INSERT + ' ON CONFLICT (key) DO UPDATE SET value=excluded.value'
  // const UPDATE = `UPDATE ${this.pg.id} SET value=($2) WHERE key=($1)'`

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

function delCommandSql (db, key) {
  return `DELETE FROM ${db.pg.id} WHERE key = $1`
}

PgDOWN.prototype._put = function (key, value, options, cb) {
  const sql = putCommandSql(this, key, value, options)
  debug('_put: sql', sql)

  this.pg.client.query(sql, [ key, value ], function (err) {
    // TODO: errors.WriteError?
    if (err) debug('_put error %j', err)
    cb(err || null)
  })
}

PgDOWN.prototype._del = function (key, options, cb) {
  const sql = delCommandSql(this, key, options)
  debug('_del: sql', sql)

  this.pg.client.query(sql, [ key ], (err, result) => {
    // TODO: errors.WriteError?
    // TODO: reflect whether or not a row was deleted? errorIfMissing?
    //   if (opts.errorIfMissing && !result.rows.length) throw ...
    if (err) debug('_del: error %j', err)
    cb(err || null)
  })
}

PgDOWN.prototype._get = function (key, options, cb) {
  // TODO: most efficient way to disable jsonb field parsing in pg lib?
  const sql = `SELECT value::text FROM ${this.pg.id}`
  debug('_get: sql', sql)

  this.pg.client.query(sql, (err, result) => {
    if (err || result.rows.length === 0) {
      err = err || new errors.NotFoundError('key: ' + key) // TODO: better message
      debug('_get: error %j', err)
      return cb(err)
    }
    cb(null, result.rows[0].value)
  })
}

PgDOWN.prototype._chainedBatch = function () {
  throw new Error('Not Yet Implemented')
}

PgDOWN.prototype._batch = function (operations, options, cb) {
  throw new Error('Not Yet Implemented')
}

PgDOWN.prototype._iterator = function (options) {
  return new PgIterator(this, options)
}

module.exports = PgDOWN
