const http = require('http')
const mongo = require('./lib/mongo')
const ping = require('./lib/ping')
const status = require('./lib/status')
const execute = require('./lib/execute')

const error = (err, res) => {
  console.log(err)
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
      execute(JSON.parse(data)).then((stream) => {
        stream.on('error', (e) => {
          console.log('error')
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






