const url = require('url')
const inherits = require('inherits')
const pg = require('pg')
const AbstractLevelDOWN = require('abstract-leveldown').AbstractLevelDOWN
const errors = require('level-errors')

function PostgresDOWN (location) {
  if (!(this instanceof PostgresDOWN)) {
    return new PostgresDOWN(location)
  }

  const parsed = url.parse(location)
  const path = parsed.path.split('/')
  this.database = path[1].toLowerCase()

  const tableName = this.tableName = path.pop() || 'p_d'
  const schemaName = this.schemaName = path.slice(2).join('__') || 'p_d'
  this.path = `"${schemaName}"."${tableName}"`

  // TODO: parse into connection obj
  // {
  //   user: ...,
  //   password: '',
  //   host: 'localhost',
  //   port: 5432,
  //   database: '',
  // }

  parsed.path = '/' + this.database
  const uri = url.format(parsed)

  // TODO: use pg.pools
  this.client = new pg.Client(uri)

  AbstractLevelDOWN.call(this, location)
}

inherits(PostgresDOWN, AbstractLevelDOWN)

PostgresDOWN.prototype._open = function (options, cb) {
  const client = this.client
  const db = this

  function create (cb) {
    const sql = `
      CREATE SCHEMA IF NOT EXISTS "${db.schemaName}";
      CREATE TABLE IF NOT EXISTS ${db.path} (
        key text PRIMARY KEY,
        value jsonb
      );`

    client.query(sql, cb)
  }

  client.connect(function (err) {
    if (err) return cb(err)

    const errorIfExists = options.errorIfExists
    const createIfMissing = options.createIfMissing

    if (createIfMissing && !errorIfExists) return create(cb)

    // TODO ...
    // // check if rel path exists
    // client.query(`SELECT to_regclass(${db.path});`, function (err, result) {
    //   if (err) return cb(err)

    //   if (result) {
    //     if (errorIfExists) {
    //       cb(new InitializationError('`' + db.path + '` already exists'))
    //     } else {
    //       cb()
    //     }
    //   } else {
    //     createIfMissing ? create(cb) : cb()
    //   }
    // })
  })
}

PostgresDOWN.prototype._close = function (cb) {
  try {
    this.client.end()
    process.nextTick(cb)
  } catch (err) {
    process.nextTick(function () {
      cb(err)
    })
  }
}

PostgresDOWN.prototype._put = function (key, value, options, cb) {
  const INSERT = `INSERT INTO ${this.path} (key,value) VALUES($1,$2)`
  const UPSERT = INSERT + ' ON CONFLICT (key) DO UPDATE SET value = excluded.value'
  // const UPDATE = `UPDATE ${this.path} SET value = $2 WHERE key = $1'`

  // just an upsert for now
  this.client.query(UPSERT, [ key, value ], function (err) {
    if (err) return cb(err) // TODO: errors.WriteError?

    cb()
  })

  // TODO: the below is a total shitshow -- probably not even worth bothering w/
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
  this.client.query(`SELECT value::text FROM ${this.path}`, function (err, result) {
    if (err) return cb(err)
    if (result.rows.length) {
      cb(null, result.rows[0].value)
    }
    else {
      cb(new errors.NotFoundError('key: ' + key))
    }
  })
}

PostgresDOWN.prototype._del = function (key, options, cb) {
  this.client.query(`DELETE FROM ${this.path} WHERE key = $1`, [ key ], function (err, result) {
    if (err) return cb(err) // TODO: errors.WriteError?

    // TODO: reflect whether or not a row was deleted? errorIfMissing?
    cb()
  })
}

PostgresDOWN.prototype._chainedBatch = function () {
  throw new Error('Not Yet Implemented')
}

PostgresDOWN.prototype._batch = function (operations, options, cb) {
  throw new Error('Not Yet Implemented')
}

module.exports = PostgresDOWN
