const http = require('http')
const mongo = require('./lib/mongo')
const ping = require('./lib/ping')
const status = require('./lib/status')
const execute = require('./lib/execute')

const error = (err, res) => {
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
  console.error(e)
  process.exit(1)
})






