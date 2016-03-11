var major = process.version.slice(1).split('.').shift()

// transpile with babel to tests in older node versions
if (major < 4) require('babel-register')

require('./pgdown')
require('./encoding')
require('./abstract-leveldown')
