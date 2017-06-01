process.env.SERVICE_A_PORT = 5487
process.env.SERVICE_B_PORT = 5486

const service = require('../index')

service.handle('lol')

service.onload(emit => {
  //console.log(service)
  setInterval(() => emit.lol('yo'), 1000)
})