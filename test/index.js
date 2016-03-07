require('./pgdown')
require('./pgdown-json')
require('./abstract-leveldown')

// TODO: figure out WTF is up w/ the shitty `pg` pooling
require('tape').test('exit', (t) => {
  t.end()
  setTimeout(process.exit, 1000)
})
