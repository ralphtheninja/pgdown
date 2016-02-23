const inherits = require('inherits')
const AbstractLevelDOWN = require('abstract-leveldown').AbstractLevelDOWN

function PostgresDOWN () {
  if (!(this instanceof PostgresDOWN)) {
    return new PostgresDOWN()
  }

  AbstractLevelDOWN.call(this)
}

inherits(PostgresDOWN, AbstractLevelDOWN)

PostgresDOWN.prototype._open = function (options, cb) {
  throw new Error('Not Yet Implemented')
}

PostgresDOWN.prototype._close = function (cb) {
  throw new Error('Not Yet Implemented')
}

PostgresDOWN.prototype._put = function (key, value, options, cb) {
  throw new Error('Not Yet Implemented')
}

PostgresDOWN.prototype._get = function (key, options, cb) {
  throw new Error('Not Yet Implemented')
}

PostgresDOWN.prototype._del = function (key, options, cb) {
  throw new Error('Not Yet Implemented')
}

PostgresDOWN.prototype._chainedBatch = function () {
  throw new Error('Not Yet Implemented')
}

PostgresDOWN.prototype._batch = function (operations, options, cb) {
  throw new Error('Not Yet Implemented')
}
