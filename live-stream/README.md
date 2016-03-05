# `pgdown/live-stream`

Use this module to get a stream of changes over any `pg-level` instance with a [`level-live-stream`](https://github.com/dominictarr/level-live-stream#readme)-compatible API. All writes into the underlying postgres table will be emitted as data events on this stream -- or if a range is specified, only those writes within this range.

```js
var LiveStream = require('pgdown/live-stream')

// attach the live-stream methods using `install`
var liveStream = LiveStream(db)

liveStream
  .on('data', console.log)

setInterval(function () {
  db.put('time', new Date())
}, 1000)
```

## `install`

Use the `install` method on a `pg-level` instance to add a `createLiveStream` method to your db (also available as with the `liveStream` alias).

For compatibility with [`multilevel`](https://github.com/juliangruber/multilevel#readme), `install` attaches the necessary manifest metadata, allowing remote `multilevel` clients to use `db.createLiveStream()` too.

```js
var LiveStream = require('pgdown/live-stream')

// attach the live-stream methods using `install`
LiveStream.install(db)

// then invoke the method using the `db` instance
db.createLiveStream()
  .on('data', console.dir)
```
