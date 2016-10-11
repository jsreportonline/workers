const status = require('./status').status
const db = require('./mongo').db
const Promise = require('bluebird')
const request = require('request')
const lworkers = require('./workers')
const lbusyQueue = require('./busyQueue')

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

module.exports = (body, post = request.post, stat = status.status, workers = lworkers, busyQueue = lbusyQueue) => {
  const findWorker = (tenant, containerType, body) => {
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
      const promise = new Promise((resolve, reject) => {})
      busyQueue.push(body, promise)
      return promise
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

    worker.restartPromise = db().collection('tenants').updateAsync({ name: originalTenant }, update).then(() => worker.restart())
    return worker.restartPromise
  }

  const run = (body) => {
    console.log(`Processing request ${body.tenant}:${body.containerType}`)

    return db().collection('tenants').findOneAsync({ name: body.tenant }, { workerIp: 1 }).then((t) => {
      if (t.workerIp && t.workerIp[process.env.stack] && t.workerIp[process.env.stack] !== process.env.ip && stat(t.workerIp)) {
        console.log(`posting to external node ${t.workerIp[process.env.stack]}`)
        return new Promise((resolve, reject) => resolve(post({
          url: `http://${t.workerIp[process.env.stack]}:1000`,
          headers: {
            'Content-Type': 'application/json'
          },
          body: JSON.stringify(body)
        })))
      }

      const update = { $set: {} }
      update.$set[`workerIp.${process.env.stack}`] = process.env.ip

      return db().collection('tenants').updateAsync({ name: body.tenant }, update).then(() => {
        console.log(`Executing local worker  ${body.tenant} in container ${body.containerType}`)

        return findWorker(body.tenant, body.containerType, body).then((worker) => {
          console.log(`Worker url for message ${worker.url}:${worker.numberOfRequests}`)

          return new Promise((resolve, reject) => resolve(post({
            url: worker.url,
            headers: {
              'Content-Type': 'application/json'
            },
            body: JSON.stringify(body.data)
          }).on('error', (e) => {
            worker.numberOfRequests--
            busyQueue.flush(run)
          }).on('end', () => {
            worker.numberOfRequests--
            busyQueue.flush(run)
          })))
        })
      })
    })
  }

  return run(body)
}
