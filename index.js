const http = require('http')
const mongo = require('./lib/mongo')
const ping = require('./lib/ping')
const status = require('./lib/status')
const execute = require('./lib/execute')
const winston = require('winston')
const path = require('path')
const fs = require('fs')

try {
  fs.mkdirSync(path.join(__dirname, 'logs'))
} catch (e) {

}

winston.remove(winston.transports.Console)
winston.add(winston.transports.Console, {
  level: 'info'
})
winston.add(winston.transports.File, {
  filename: path.join(__dirname, 'logs', 'workers.log'),
  json: false,
  maxsize: 50000000,
  maxFiles: 5,
  timestamp: true,
  level: 'debug'
})

winston.info('starting workers')

const error = (err, res) => {
  winston.error(err)
  res.statusCode = 500
  res.setHeader('Content-Type', 'text/plain')
  return res.end(err.stack)
}

mongo().then(() => {
  return ping()
}).then(() => {
  return status()
}).then(() => {
  const server = http.createServer((req, res) => {
    const lastHardError = execute.lastHardError()

    if (req.method === 'GET') {
      if (lastHardError && req.url === '/status') {
        res.statusCode = 500
        res.setHeader('Content-Type', 'text/plain')
        return res.end(lastHardError.stack)
      }

      res.statusCode = 200
      res.setHeader('Content-Type', 'text/plain')
      return res.end('ok')
    }

    var data = ''
    req.on('data', function (chunk) {
      data += chunk.toString()
    })

    req.on('end', function () {
      execute(JSON.parse(data)).then((stream) => {
        stream.on('error', (e) => {
          error(e, res)
        }).pipe(res)
      }).catch((err) => error(err, res))
    })
  })

  server.listen(1000)
}).catch((e) => {
  winston.error(e)
  process.exit(1)
})






