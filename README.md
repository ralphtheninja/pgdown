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

## Config

Database config can be specified as a `postgres://` uri in the `location` string passed to the `PgDOWN` factory function. Otherwise, it will be pulled from the `PG*` environment variables (TODO: document this), falling back to the defaults in the `pg` client lib.


## License
MIT
