const status = require('./status').status
const db = require('./mongo').db
const Promise = require('bluebird')
const request = require('request')
const exec = Promise.promisify(require('child_process').exec)
const url = require('url')

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

const waitForPing = (url, retries) => {
  return new Promise((resolve, reject) => {
    const retry = () => {
      request.get(url, (err, resp) => {
        if (!err && resp.statusCode === 200) {
          return resolve()
        }

        console.log(`ping ${retries}`)

        if (--retries === 0) {
          return reject(new Error(`Unable to ping ${url}`))
        }

        setTimeout(retry, 5)
      })
    }

    retry()
  })
}

const restartContainer = (worker) => {
  if (process.env.containerRestartPolicy === 'no') {
    console.log('Skipping container restart')
    return Promise.resolve(worker)
  }

  const container = url.parse(worker.url).hostname
  console.log(`Restarting container ${container}`)

  return exec(`docker restart -t 0 ${container}`)
    .then(() => waitForPing(worker.url, 1000))
    .then(() => worker)
    .catch((e) => {
      worker.isBusy = false
      throw e
    })
}

const findWorker = (tenant, containerType) => {
  var worker = workers[containerType].find((w) => w.tenant === tenant)

  if (worker && worker !== -1) {
    worker.tenant = tenant
    worker.lastUsed = new Date()
    worker.isBusy = true

    return Promise.resolve(worker)
  }

  console.log('Assigned worker not found, searching by LRU')
  worker = workers[containerType].reduce((prev, current) => (prev.lastUsed < current.lastUsed) ? prev : current)

  if (worker.isBusy) {
    const err = new Error('Too many requests')
    err.code = 'TOO_MANY'
    throw err
  }

  worker.tenant = tenant
  worker.lastUsed = new Date()
  worker.isBusy = true

  if (!worker.tenant) {
    console.log('No need to restart new worker')
    return Promise.resolve(worker)
  }

  return restartContainer(worker)
}

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
      status(t.serverIp)
      return db().collection('tenants').updateAsync({ name: body.tenant }, { $set: { serverIp: process.env.ip } }).then(() => {
        console.log(`Executing worker message for ${body.tenant} in container ${body.containerType}`)

        return findWorker(body.tenant, body.containerType).then((worker) => {
          console.log(`Worker url for message ${worker.url}`)

          request.post({
            url: worker.url,
            headers: {
              'Content-Type': 'application/json'
            },
            body: JSON.stringify(body.data)
          }).on('error', (e) => {
            worker.isBusy = false
            res.statusCode = 500
            res.setHeader('Content-Type', 'text/plain')
            return res.end(e.stack)
          }).on('end', () => {
            worker.isBusy = false
          }).pipe(res)
        })
      })
    }
  }).catch((e) => {
    console.error('Error when processing request', e.stack)
    res.statusCode = 500
    res.setHeader('Content-Type', 'text/plain')
    return res.end(e.stack)
  })
}