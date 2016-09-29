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

const timeoutWaitingForWorker = process.env.timeoutWaitingForWorker = 10000

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

process.on('SIGTERM', () => {
  console.log('quiting worker...')
  const query = {}
  query[`workerIp.${process.env.stack}`] = process.env.ip
  const update = { '$unset': {} }
  update.$unset[`workerIp.${process.env.stack}`] = ''

  db().collection('tenants').updateAsync(query, update, { multi: true }).finally(() => {
    process.exit()
  })
})

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

        setTimeout(retry, 100)
      })
    }

    retry()
  })
}

const busyQueue = []

const restartContainer = (worker) => {
  if (process.env.containerRestartPolicy === 'no') {
    worker.numberOfRequests = 1
    console.log('Skipping container restart')
    return Promise.resolve(worker)
  }

  const container = url.parse(worker.url).hostname
  console.log(`Restarting container ${container}`)

  worker.numberOfRequests = 1

  return exec(`docker restart -t 0 ${container}`)
    .then(() => waitForPing(worker.url, 1000))
    .then(() => worker)
    .catch((e) => {
      worker.numberOfRequests = 0
      throw e
    })
}

const findWorker = (tenant, containerType, body, res) => {
  console.log(`Searching to worker ${tenant}:${containerType}`)
  var worker = workers[containerType].find((w) => w.tenant === tenant)

  if (worker && worker !== -1) {
    console.log('Already associated')
    worker.tenant = tenant
    worker.lastUsed = new Date()
    worker.numberOfRequests++

    return Promise.resolve(worker.restartPromise).then(() => worker)
  }

  console.log('Assigned worker not found, searching by LRU')
  worker = workers[containerType].reduce((prev, current) => (prev.lastUsed < current.lastUsed) ? prev : current)

  if (worker.numberOfRequests > 0) {
    console.log('All workers are busy, queuing')
    busyQueue.push({
      submittedOn: new Date().getTime(),
      body: body,
      res: res
    })
    return Promise.resolve(null)
  }

  const originalTenant = worker.tenant
  worker.tenant = tenant
  worker.lastUsed = new Date()

  if (!originalTenant) {
    worker.numberOfRequests++
    console.log('No need to restart new worker')
    return Promise.resolve(worker)
  }

  const update = { $set: {} }
  update.$set[`workerIp.${process.env.stack}`] = undefined

  worker.restartPromise = db().collection('tenants').updateAsync({ name: originalTenant }, update).then(() => restartContainer(worker))
  return worker.restartPromise
}

const flushBusyQueue = () => {
  const item = busyQueue.shift()
  if (item) {
    if (item.submittedOn < (new Date().getTime() - timeoutWaitingForWorker)) {
      item.res.statusCode = 500
      item.res.setHeader('Content-Type', 'text/plain')
      item.res.end('Timeout when waiting for worker')

      return flushBusyQueue()
    }

    console.log(`queue length: ${busyQueue.length}`)
    run(item.body, item.res)
  }
}

const run = (body, res) => {
  console.log(`Processing request ${body.tenant}:${body.containerType}`)

  return db().collection('tenants').findOneAsync({ name: body.tenant }, { workerIp: 1 }).then((t) => {
    if (t.workerIp && t.workerIp[process.env.stack] && t.workerIp[process.env.stack] !== process.env.ip && status(t.workerIp)) {
      console.log(`posting to external node ${t.workerIp[process.env.stack]}`)
      return request.post({
        url: `http://${t.workerIp[process.env.stack]}:1000`,
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

    const update = { $set: {} }
    update.$set[`workerIp.${process.env.stack}`] = process.env.ip

    return db().collection('tenants').updateAsync({ name: body.tenant }, update).then(() => {
      console.log(`Executing local worker  ${body.tenant} in container ${body.containerType}`)

      return findWorker(body.tenant, body.containerType, body, res).then((worker) => {
        if (!worker) {
          return
        }

        console.log(`Worker url for message ${worker.url}:${worker.numberOfRequests}`)

        request.post({
          url: worker.url,
          headers: {
            'Content-Type': 'application/json'
          },
          body: JSON.stringify(body.data)
        }).on('error', (e) => {
          worker.numberOfRequests--
          flushBusyQueue()
          res.statusCode = 500
          res.setHeader('Content-Type', 'text/plain')
          return res.end(e.stack)
        }).on('end', () => {
          worker.numberOfRequests--
          flushBusyQueue()
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

module.exports = (body, res) => {
  run(body, res)
}