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
    lastUsed: new Date(),
    numberOfRequests: 0
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

        if (--retries === 0) {
          return reject(new Error(`Unable to ping ${url}`))
        }

        setTimeout(retry, 10)
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
    .then(() => (worker.numberOfRequests = 1))
    .then(() => worker)
    .catch((e) => {
      worker.numberOfRequests = 0
      throw e
    })
}

const findWorker = (tenant, containerType) => {
  console.log(`Searching to worker ${tenant}:${containerType}`)
  var worker = workers[containerType].find((w) => w.tenant === tenant)

  if (worker && worker !== -1) {
    console.log('Already associated')
    worker.tenant = tenant
    worker.lastUsed = new Date()
    worker.numberOfRequests++

    return Promise.resolve(worker)
  }

  console.log('Assigned worker not found, searching by LRU')
  worker = workers[containerType].reduce((prev, current) => (prev.lastUsed < current.lastUsed) ? prev : current)

  if (worker.numberOfRequests > 0) {
    const err = new Error('Too many requests')
    err.code = 'TOO_MANY'
    throw err
  }

  if (!worker.tenant) {
    worker.tenant = tenant
    worker.lastUsed = new Date()
    worker.numberOfRequests++
    console.log('No need to restart new worker')
    return Promise.resolve(worker)
  }

  worker.tenant = tenant
  worker.lastUsed = new Date()
  return restartContainer(worker)
}

module.exports = (body, res) => {
  console.log(`Processing request ${body.tenant}:${body.containerType}`)
  return db().collection('tenants').findOneAsync({ name: body.tenant }, { serverIp: 1 }).then((t) => {
    console.log(`${body.tenant}:${t.serverIp}`)

    if (t.serverIp) {
      console.log(`status:${status(t.serverIp)}`)
    }

    if (t.serverIp && t.serverIp !== process.env.ip && status(t.serverIp)) {
      console.log(`posting to external node ${t.serverIp}`)
      request.post({
        url: `http://${t.serverIp}:1000`,
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(body)
      }).on('error', (e) => {
        res.statusCode = 500
        res.setHeader('Content-Type', 'text/plain')
        return res.end(e.stack)
      }).pipe(res)
    }

    return db().collection('tenants').updateAsync({ name: body.tenant }, { $set: { serverIp: process.env.ip } }).then(() => {
      console.log(`Executing local worker  ${body.tenant} in container ${body.containerType}`)

      return findWorker(body.tenant, body.containerType).then((worker) => {
        console.log(`Worker url for message ${worker.url}:${worker.numberOfRequests}`)

        request.post({
          url: worker.url,
          headers: {
            'Content-Type': 'application/json'
          },
          body: JSON.stringify(body.data)
        }).on('error', (e) => {
          worker.numberOfRequests--
          res.statusCode = 500
          res.setHeader('Content-Type', 'text/plain')
          return res.end(e.stack)
        }).on('end', () => {
          worker.numberOfRequests--
        }).pipe(res)
      })
    })
  }).catch((e) => {
    console.error('Error when processing request', e.stack)
    res.statusCode = 500
    res.setHeader('Content-Type', 'text/plain')
    return res.end(e.stack)
  })
}