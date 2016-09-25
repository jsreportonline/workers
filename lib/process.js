const status = require('./status')
const request = require('request')
const db = require('./mongo').db

const workers = {
  t: [],
  w: [],
  p: [],
  f: []
}

const mapEnvToWorkers = (type) => {
  workers[type] = process.env['workerUrls:' + type].split(' ').map((u) => ({
    url: u,
    lastUsed: new Date()
  }))
}

mapEnvToWorkers('t')
mapEnvToWorkers('w')
mapEnvToWorkers('p')
mapEnvToWorkers('f')

module.exports = (body, res) => {
  return db().collection('tenants').findOneAsync({ name: body.tenant }, { server: 1 }).then((t) => {
    if (t.serverIp && t.serverIp !== process.env.ip && status(t.serverIp)) {
      console.log(`posting to external node ${t.serverIp}`)
      return request.post({
        url: `http://${t.serverIp}:1000`,
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(body)
      })
    }

    if (!t.serverIp || t.serverIp === process.env.ip || !status(t.serverIp)) {
      return db().collection('tenants').updateAsync({ name: body.tenant }, { $set: { serverIp: process.env.ip } }).then(() => {
        console.log(`Executing worker message for ${body.tenant} in container ${body.containerType}`)
        var worker = workers[body.containerType].find((w) => w.tenant === body.tenant)

        if (!worker || worker === -1) {
          console.log('Assigned worker not found, searching by LRU')
          worker = workers[body.containerType].reduce((prev, current) => (prev.lastUsed < current.lastUsed) ? prev : current)
        }

        console.log(`Worker url for message ${worker.url}`)

        worker.tenant = body.tenant
        worker.lastUsed = new Date()

        request.post({
          url: worker.url,
          headers: {
            'Content-Type': 'application/json'
          },
          body: JSON.stringify(body.data)
        }).on('error', (e) => {
          console.log('should response error', e.stack)
          res.statusCode = 500
          res.setHeader('Content-Type', 'text/plain')
          return res.end(e.stack)
        }).pipe(res)
      })
    }
  })
}


