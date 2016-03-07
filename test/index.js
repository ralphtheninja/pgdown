require('./pgdown')
require('./pgdown-json')
require('./abstract-leveldown')

// TODO: figure out WTF is up w/ the shitty `pg` pooling
require('tape').onFinish(function () {
  console.warn('failing:', this.fail)
  setTimeout(() => process.exit(this.fail ? 1 : 0))
})
