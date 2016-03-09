# pgdown [![Build Status](https://travis-ci.org/ralphtheninja/pgdown.svg?branch=master)](https://travis-ci.org/ralphtheninja/pgdown)

**Experimental and WIP. Do not use.**

[`PostgreSQL`](http://www.postgresql.org/) backed [`abstract-leveldown`](https://github.com/Level/abstract-leveldown).

## Usage

Use together with [`levelup`](https://github.com/Level/levelup) to get a `PostgreSQL` backed storage.

```js
const levelup = require('levelup')
const pgdown = require('pgdown')

const uri = 'postgres://postgres:@localhost:5432/postgres'
const db = levelup(uri, {
  db: pgdown,
  keyEncoding: 'utf8',
  valueEncoding: 'json'
})

db.put('foo', { bar: 'baz' }, (err) => {
  db.get('foo', (err, result) => {
    console.log('result %j', result)
  })
})
```

## Api

#### `pgdown(location)`

Creates a `PgDOWN` object with `location` which can take the following forms:

* `postgres://<user>:<password>@<host>:<port>/<database>/<table>`
* `/<database>/<table>`

An `options` object is created based on the location and passed to [`pg.Client`](https://github.com/brianc/node-postgres/wiki/Client#new-clientobject-config--client). However, `pgdown` respects the _default_ environment variables used by [`PostgreSQL`](http://www.postgresql.org/docs/9.5/static/libpq-envars.html) in favor of the ones used in `pg`.

To summarize we have the following properties and their default values:

* `database` from `location` _or_ `$PGDATABASE` _or_ `'postgres'`
* `host` from `location` _or_ `$PGHOSTADDR` _or_ `'localhost'`
* `port` from `location` _or_ `$PGPORT` _or_ `5432`
* `user` from `location` _or_ `$PGUSER` _or_ `$USERNAME` (win32) _or_ `$USER`
* `password` from `location` _or_ `$PGPASSWORD` _or_ `null`

## License

MIT
