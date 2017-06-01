process.env.SERVICE_NETCALL_PORT = 9975

const service = require('./index')


service.handle('test0', () => {
  console.log('this is such a strong test ! wow !')
})

service.handle({
  test1: console.log,
  test2: console.log,
})

service
  .onload(() => process.exit(0))
  .catch(() => process.exit(1))

