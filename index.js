'use strict'
const net = require('net')
const path = require('path')
const EventEmitter = require('events')

const cwd = process.cwd()
const pkg = require(path.join(cwd, 'package.json'))
const serviceName = cwd.split('/').pop().replace('service-', '')
const getPort = name => process.env[`SERVICE_${name.toUpperCase()}_PORT`]
const servicePort = getPort(serviceName)

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
      if (val instanceof Buffer) return BINARY_TYPE
      if (val instanceof Error) return ERROR_TYPE
      if (val instanceof Date) return DATE_TYPE
      return JSON_TYPE
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

  return Buffer.concat([ header, data ])
}

const decode = (type, buf) => {
  const handler = handlers[type]
  if (!handler) return console.error('unhandled type', type)
  try { return handler.decode(buf) }
  catch (err) { console.error(err, '\nparsing failed', { type, buf }) }
}

const handleSocket = (socket, broadcasters) => {
  let size, dataBuffer

  const handleMessage = rawData => {
    const id = rawData.readUInt32LE(8)
    const route = rawData.readUInt8(17)
    const type = rawData.readUInt8(16)

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

const cleanup = new WeakMap()
const connect = (port, client, prevRoutes, ev) => open(port)
  .then(socket => {
    const broadcasters = Object.create(null)

    handleSocket(socket, broadcasters)

    return buildRPCHanlder(broadcasters, 0, socket)()
      .then(routes => ({ socket, broadcasters, routes }))
  })
  .then(({ routes, socket, broadcasters }) => {
    const routeNames = Object.keys(routes)
    const ret = Object.create(null)
    client || (client = Object.create(null))

    // check if we need to reboot in case of api changes
    if (prevRoutes && routeNames.join() !== prevRoutes) return process.exit(0)

    routeNames.forEach(name => {
      const { index, isEvent } = routes[name]
      if (!isEvent) {
        client[name] = buildRPCHanlder(broadcasters, index, socket)
        return ret[name] = arg => client[name](arg)
      }

      if (prevRoutes) {
        broadcasters[index] = ({ data }) => ev.emit(name, data)
        return socket.write(buildMessage(1, name, 0))
      }

      // Since event listenners are lazy,
      // We have to check that the event exist and keep track of which events
      // I'm subscribed to, in order to recover subs after connection lost.
      let evMethod = (...args) => {
        if (!ev) {
          ev = new EventEmitter
          ev.resumeRoutes = new Set
          socket.write(buildMessage(1, name, 0))
          broadcasters[index] = ({ data }) => ev.emit(name, data)
        }

        ev.resumeRoutes.add(name)

        const methodHandler = (method, fn) => {
          ev[method](name, fn)
          return () => ev.removeListener(fn)
        }

        // so that is why re-write this method to avoid checking every time
        return (evMethod = methodHandler)(...args)
      }

      ret[name] = fn => evMethod('on', fn)
      ret[name].once = fn => evMethod('once', fn)
      ret[name].removeAllListeners = () => ev.removeAllListeners(name)
    })

    socket.on('error', console.error)
    socket.on('close', () =>
      setTimeout(() => connect(port, client, routeNames.join(), ev), 1000))

    cleanup.set(ret, () => {
      socket.removeAllListeners()
      socket.end()
    })

    return ret
  })

const tryCall = (fn, arg, handler) => {
  try {
    const ret = fn(arg, services)
    if (ret && typeof ret.then !== 'function') return handler(ret)
    ret.then(handler, handler)
  } catch (err) { handler(err) }
}

const handleAnswer = (broadcasters, index, fn) =>
  broadcasters[index] = ({ socket, data, id }) =>
    tryCall(fn, data, answer => socket.write(buildMessage(index, answer, id)))

console.log(`Starting service ${serviceName}:${servicePort}...`)

let q
const init = serviceList => q = Promise.all(serviceList.map((name =>
    connect(getPort(name)).then(client => services[name] = client))))
  .then(() => new Promise((s,f) => {
    const broadcasters = Object.create(null)
    const listenners = Object.create(null)
    const emit = services.emit = Object.create(null)

    const routeNames = routeHandlers
      .sort((a, b) => a.name - b.name)
      .map(a => a.name)

    if (!routeNames.length) {
      console.log(`no routes exposed`)
      return s(emit)
    }

    const tcpServer = net.createServer(socket =>
      handleSocket(socket, broadcasters))

    tcpServer.listen(servicePort, () => {
      console.log(`serving routes:\n  ${routeNames.join('\n  ')}`)
      s(emit)
      f = undefined
    })

    tcpServer.on('error', err => {
      if (f) return f(err)
      console.error('TCP Server ERROR:')
      console.error(err)
      setTimeout(() => process.exit(1))
    })

    const routeList = routeHandlers.map(({ name, handler }, index) => {
      // pad index because the 2 first routes are reserved
      index = index + 2
      if (handler) {
        handleAnswer(broadcasters, index, handler)
        return { [name]: { index } }
      }
      const list = listenners[name] = []
      emit[name] = data => {
        const msg = buildMessage(index, data, 0)
        for (let socket of list) {
          socket.write(msg)
        }
      }
      return { [name]: { index, isEvent: true } }
    })

    const routeBufferMessage = buildMessage(0, Object.assign(...routeList))
    broadcasters[0] = ({ socket, id }) => {
      const msg = Buffer.from(routeBufferMessage)
      msg.writeUInt32LE(id, 8)
      socket.write(msg)
    }

    broadcasters[1] = ({ data, socket }) => {
      const list = listenners[data]
      list.push(socket)
      socket.on('close', () => list.splice(list.indexOf(socket), 1))
    }
  }))

init(pkg.service || [])

services.onload = fn => q.then(fn)
services.onerror = fn => q.catch(fn)
services.handle = (name, handler) => typeof name === "string"
  ? routeHandlers.push({ name, handler })
  : Object.keys(name).map(key =>
      routeHandlers.push({ name: key, handler: name[key] }))

services.kill = name => {
  const clear = cleanup.get(services[key])
  return clear && clear()
}
services.init = init
services.killAll = () => Object.keys(services).forEach(services.kill)

module.exports = services
