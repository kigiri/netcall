process.env.SERVICE_A_PORT = 5487
process.env.SERVICE_B_PORT = 5486

const service = require('../index')

service.onload(() => {
  service.b.lol(console.log)
}).catch(console.error)
