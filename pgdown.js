const url = require('url')
const inherits = require('inherits')
const pg = require('pg')
const AbstractLevelDOWN = require('abstract-leveldown').AbstractLevelDOWN
const errors = require('level-errors')
const debug = require('debug')('pgdown')

function PostgresDOWN (location) {
  if (!(this instanceof PostgresDOWN)) {
    return new PostgresDOWN(location)
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

inherits(PostgresDOWN, AbstractLevelDOWN)

PostgresDOWN.prototype._open = function (options, cb) {
  const client = this.pg.client

  const errorIfExists = options.errorIfExists
  const createIfMissing = options.createIfMissing

  if (createIfMissing) {
    const ifNotExists = errorIfExists ? '' : ' IF NOT EXISTS'

    const SCHEMA_SQL = this.pg.schema && `
      CREATE SCHEMA${ifNotExists} ${this.pg.schema};
    `

    const TABLE_SQL = `
      CREATE TABLE${ifNotExists} ${this.pg.table} (
        key text PRIMARY KEY,
        value jsonb
      );
    `

    const CREATE_SQL = (SCHEMA_SQL || '') + TABLE_SQL
    debug('_open: createIfMissing sql', CREATE_SQL)

    client.query(CREATE_SQL)
  } else if (errorIfExists) {
    // test for table existence
    const EXISTS_SQL = `SELECT COUNT(*) from ${this.pg.id} LIMIT 1`
    debug('_open: errorIfExists sql', EXISTS_SQL)

    client.query(EXISTS_SQL)
  }

  debug('_open: client connecting')
  client.connect((err) => {
    // TODO: errors.InitializationError?
    debug('_open: client.connect error', err)
    cb(err)
  })
}

PostgresDOWN.prototype._close = function (cb) {
  debug('_close: ending client')
  // TODO: can this ever throw?
  try {
    this.pg.client.end()
    process.nextTick(cb)
  } catch (err) {
    process.nextTick(function () {
      cb(err)
    })
  }
}

PostgresDOWN.prototype._put = function (key, value, options, cb) {
  const INSERT = `INSERT INTO ${this.pg.id} (key,value) VALUES($1,$2)`
  const UPSERT = INSERT + ' ON CONFLICT (key) DO UPDATE SET value=excluded.value'
  // const UPDATE = `UPDATE ${this.pg.id} SET value=($2) WHERE key=($1)'`

  // just an upsert for now
  const SQL = UPSERT
  debug('_put: sql', SQL)

  this.pg.client.query(SQL, [ key, value ], function (err) {
    // TODO: errors.WriteError?
    debug('_put error', err)
    if (err) return cb(err)

    cb()
  })

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

PostgresDOWN.prototype._get = function (key, options, cb) {
  // TODO: most efficient way to disable jsonb field parsing in pg lib?
  const SQL = `SELECT value::text FROM ${this.pg.id}`
  debug('_get: sql', SQL)

  this.pg.client.query(SQL, (err, result) => {
    if (err) cb(err)
    else if (result.rows.length) cb(null, result.rows[0].value)
    else cb(new errors.NotFoundError('key: ' + key)) // TODO: better message
  })
}

PostgresDOWN.prototype._del = function (key, options, cb) {
  const SQL = `DELETE FROM ${this.pg.id} WHERE key = $1`
  debug('_del: sql', SQL)

  this.pg.client.query(SQL, [ key ], (err, result) => {
    // TODO: errors.WriteError?
    // TODO: reflect whether or not a row was deleted? errorIfMissing?
    //   if (opts.errorIfMissing && !result.rows.length) throw ...
    cb(err || null)
  })
}

PostgresDOWN.prototype._chainedBatch = function () {
  throw new Error('Not Yet Implemented')
}

PostgresDOWN.prototype._batch = function (operations, options, cb) {
  throw new Error('Not Yet Implemented')
}

module.exports = PostgresDOWN
