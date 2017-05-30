'use strict'
const net = require('net')
const path = require('path')

const config = require(process.env.CONF_PATH || './config')
const cwd = process.cwd()
const pkg = require(path.join(cwd, 'package.json'))
const serviceName = cwd.split(/service-([a-z0-9_]+)$/i)[1]
  || cwd.split('/').pop()
const services = Object.create(null)
const routeHandlers = []

const BINARY_TYPE = 0
const UNDEFINED_TYPE = 1
const ERROR_TYPE = 2
const STRING_TYPE = 3
const NUMBER_TYPE = 4
const BOOLEAN_TYPE = 5
const DATE_TYPE = 6
const FUNCTION_TYPE = 7
const JSON_TYPE = 8

const getType = val => {
  if (val == null) return UNDEFINED_TYPE
  switch (typeof val) {
    case 'string': return STRING_TYPE
    case 'number': return NUMBER_TYPE
    case 'boolean': return BOOLEAN_TYPE
    case 'function': return FUNCTION_TYPE
    default: {
      switch (val.constructor) {
        case Buffer: return BINARY_TYPE
        case Error: return ERROR_TYPE
        case Date: return DATE_TYPE
        default: return JSON_TYPE
      }
    }
  }
}

const pass = _ => _
const encodeNumber = num => {
  const buf = Buffer.allocUnsafe(8)
  buf.writeDoubleLE(num, 0)
  return buf
}

const handlers = {
  [UNDEFINED_TYPE]: {
    encode: _ => Buffer(0),
    decode: _ => {},
  },
  [STRING_TYPE]: {
    encode: str => Buffer(str),
    decode: buf => buf.toString(),
  },
  [NUMBER_TYPE]: {
    encode: encodeNumber,
    decode: buf => buf.readDoubleLE(0),
  },
  [BOOLEAN_TYPE]: {
    encode: bool => Buffer(Number(bool)),
    decode: buf => Boolean(buf[0]),
  },
  [FUNCTION_TYPE]: {
    encode: fn => Buffer(fn.toString()),
    decode: buf => eval(buf.toString()),
  },
  [BINARY_TYPE]: {
    encode: pass,
    decode: pass,
  },
  [DATE_TYPE]: {
    encode: date => encodeNumber(date.getTime()),
    decode: buf => new Date(buf.readDoubleLE(0)),
  },
  [ERROR_TYPE]: {
    encode: err => Buffer(err.message),
    decode: buf => Error(buf.toString()),
  },
  [JSON_TYPE]: {
    encode: obj => Buffer(JSON.stringify(obj)),
    decode: buf => JSON.parse(buf.toString()),
  },
}

const genId = () => {
  const tmp = Buffer.allocUnsafe(8)
  tmp.writeDoubleLE(Math.random(), 0)
  const id = tmp.readUInt32LE(0)
  // 0xFF is a reserved range for routes id, they should not colide
  return (id < 0xFF) ? genId() : id
}

const buildMessage = (route, rawData, id) => {
  const type = getType(rawData)
  const data = handlers[type].encode(rawData)
  const header = Buffer.allocUnsafe(18)

  header.writeUInt32LE(data.byteLength, 0)
  header.writeUInt32LE(id || genId(), 8)
  header.writeUInt8(type, 16)
  header.writeUInt8(route, 17)

  console.log('sending:', { size: data.byteLength, id, type, route })
  return Buffer.concat([ header, data ])
}

const decode = (type, buf) => {
  const handler = handlers[type]
  if (!handler) return console.log('unhandled type', type)
  try { return handler.decode(buf) }
  catch (err) { console.log(err, '\nparsing failed', { type, buf }) }
}

const handleSocket = (socket, broadcasters) => {
  let size, dataBuffer

  const handleMessage = rawData => {
    const id = rawData.readUInt32LE(8)
    const route = rawData.readUInt8(17)
    const type = rawData.readUInt8(16)

    console.log('recieved:', { size, id, type, route })

    const broadcast = broadcasters[id] || broadcasters[route]
    if (!broadcast) return

    const data = decode(type, rawData.slice(18, size))
    broadcast({ id, route, socket, type, data })

    // clearing the dataBuffer before each call to signal
    // handleNextData know the message has been handled
    dataBuffer = undefined
    rawData.byteLength > size && handleNextData(rawData.slice(size))
  }

  const handleNextData = data => {
    if (dataBuffer) {
      dataBuffer = Buffer.concat([ dataBuffer, data ])
    } else {
      size = data.readUInt32LE(0) + 18
      dataBuffer = data
    }

    dataBuffer.byteLength >= size && handleMessage(dataBuffer)
  }

  socket.on('data', handleNextData)
}

const buildRPCHanlder = (broadcasters, index, socket) => function handler(d) {
  return new Promise((s, f) => {
    const id = genId()
    if (broadcasters[id]) return handler(d)

    const timeout = setTimeout(() => {
      broadcasters[id] = undefined
      const err = Error(`timeout`)
      err.code = 'ETIME'
      f(err)
    }, 15000)

    broadcasters[id] = ({ type, data }) => {
      clearTimeout(timeout)
      broadcasters[id] = undefined
      return type === ERROR_TYPE ? f(data) : s(data)
    }

    socket.write(buildMessage(index, d, id))
  })
}

const getClientSocket = (port, count) => new Promise((s, f) => {
  const socket = new net.Socket
  socket.setNoDelay(true)
  socket.on('error', pass)
  socket.on('close', () => setTimeout(f, 250 * (count + 1)))
  socket.connect(port, () => s(socket))
})

const open = (port, count = 0) => getClientSocket(port, count)
  .catch(() => (++count > 30)
    ? process.exit(1)
    : (console.log(`connection on port ${port} retry #${count} / 30`),
      open(port, count)))

const connect = (port, client = Object.create(null), prevRoutes) => open(port)
  .then(socket => {
    const broadcasters = Object.create(null)

    handleSocket(socket, broadcasters)

    return buildRPCHanlder(broadcasters, 0, socket)()
      .then(routes => ({ socket, routes, broadcasters }))
  })
  .then(({ routes, socket, broadcasters }) => {
    const ret = Object.create(null)
    // check if we need to reboot in case of api changes
    if (prevRoutes && routes.join() !== prevRoutes) return process.exit(0)

    routes.forEach((name, index) => {
      client[name] = buildRPCHanlder(broadcasters, index + 1, socket)
      ret[name] = arg => client[name](arg)
    })

    socket.on('error', console.error)
    socket.on('close', () =>
      setTimeout(() => connect(port, client, routes.join()), 1000))

    return ret
  })

const tryCall = (fn, arg, handler) => {
  try {
    const ret = fn(arg, services)
    if (typeof ret.then === 'function') return ret.then(handler, handler)
    handler(ret)
  } catch (err) { handler(err) }
}

const handleAnswer = (broadcasters, index, fn) =>
  broadcasters[index] = ({ socket, data, id }) =>
    tryCall(fn, data, answer => socket.write(buildMessage(index, answer, id)))


console.log(`Starting service ${serviceName}:${config.service[serviceName]}...`)

const q = Promise.all((pkg.service || []).map((name =>
    connect(config.service[name]).then(client => services[name] = client))))
  .then(() => {
    const broadcasters = Object.create(null)
    const routeNames = routeHandlers
      .sort((a, b) => a.name - b.name)
      .map(a => a.name)

    if (!routeNames.length) return console.log(`no routes exposed`)

    const tcpServer = net.createServer(socket =>
      handleSocket(socket, broadcasters))


    tcpServer.listen(config.service[serviceName], () =>
      console.log(`serving routes:\n  ${routeNames.join('\n  ')}`))

    tcpServer.on('error', err => {
      console.error('TCP Server ERROR:')
      console.error(err)
      setTimeout(() => process.exit(1))
    })

    routeHandlers.unshift({ name: '__getRoutes__', handler: () => routeNames })

    routeHandlers.forEach(({ name, handler }, index) =>
      handleAnswer(broadcasters, index, handler))
  })

services.onload = fn => q.then(fn)
services.onerror = fn => q.catch(fn)
services.config = config
services.handle = (name, handler) => routeHandlers.push({ name, handler })

module.exports = services
