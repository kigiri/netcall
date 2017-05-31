const service = require('./index')

service.handle('test', () => {
  console.log('this is such a strong test ! wow !')
})

service
  .onload(() => process.exit(0))
  .catch(() => process.exit(1))