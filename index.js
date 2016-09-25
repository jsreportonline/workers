const http = require('http')
const mongo = require('./lib/mongo')
const ping = require('./lib/ping')
const status = require('./lib/status')
const process = require('./lib/process')

mongo().then(() => {
  return ping()
}).then(() => {
  return status()
}).then(() => {
  const server = http.createServer((req, res) => {
    if (req.method === 'GET') {
      res.statusCode = 200
      res.setHeader('Content-Type', 'text/plain')
      return res.end('OK')
    }
    var data = ''
    req.on('data', function (chunk) {
      data += chunk.toString()
    })

    req.on('end', function () {
      process(JSON.parse(data), res).catch((e) => {
        console.log('should response error', e.stack)
        res.statusCode = 500
        res.setHeader('Content-Type', 'text/plain')
        return res.end(e.stack)
      })
    })
  })

  server.listen(1000)
}).catch((e) => {
  console.error(e)
  process.exit(1)
})






