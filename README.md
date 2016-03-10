# pgdown [![Build Status](https://travis-ci.org/ralphtheninja/pgdown.svg?branch=master)](https://travis-ci.org/ralphtheninja/pgdown)

**Experimental and WIP. Do not use.**

[`PostgreSQL`](http://www.postgresql.org/) backed [`abstract-leveldown`](https://github.com/Level/abstract-leveldown).

## Usage

Use together with [`levelup`](https://github.com/Level/levelup) to get a `PostgreSQL` backed storage.

```js
const levelup = require('levelup')
const PgDOWN = require('pgdown')

const uri = 'postgres://postgres:@localhost:5432/postgres'
const db = levelup(uri, {
  db: PgDOWN,
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

#### `const PgDOWN = require('pgdown')`

Constructor.

#### `const down = PgDOWN(location)`

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

## ES6

`pgdown` mainly uses arrow functions and template strings from ES6 which are very useful when constructing SQL query strings. It primarily targets node `4+` but should work well with `0.10` and `0.12` together with [`babel-register`](https://www.npmjs.com/package/babel-register) _or_ [`babel-cli`](https://www.npmjs.com/package/babel-cli/) and [`babel-preset-es2015`](https://www.npmjs.com/package/babel-preset-es2015).

## PostgreSQL

**Note** `pgdown` requires at least version `9.5` of `PostgreSQL`.

If you're hacking on `pgdown` or just want to setup `PostgreSQL` locally the easiest way is probably to use docker. We can highly recommend [`clkao/postgres-plv8`](https://hub.docker.com/r/clkao/postgres-plv8/) which is based on the official `PostgreSQL` docker image but with support for [`plv8`](https://github.com/plv8/plv8).

```
$ docker pull clkao/postgres-plv8:9.5
$ docker run -d -p 5432:5432 -v /tmp/data:/var/lib/postgresql/data clkao/postgres-plv8:9.5
```

Check out the [wiki](https://github.com/ralphtheninja/pgdown/wiki/PostgreSQL-and-Docker) for more information.

## License

MIT
