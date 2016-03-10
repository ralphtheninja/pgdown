var major = process.version.slice(1).split('.').shift()

if (major < 4) {
  require('babel-register')
}

require('./pgdown')
require('./encoding')
require('./abstract-leveldown')
