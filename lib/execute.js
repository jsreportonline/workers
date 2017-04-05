const status = require('./status').status
const db = require('./mongo').db
const Promise = require('bluebird')
const request = require('request')
const lworkers = require('./workers')
const lbusyQueue = require('./busyQueue')
const winston = require('winston')
const findWinServer = require('./win').findWinServer

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

module.exports = (body, post = request.post, stat = status, workers = lworkers, busyQueue = lbusyQueue) => {
  const unregisterAndRestartWorker = (worker, originalTenant) => {
    const update = { $set: {} }
    update.$set[`workerIp.${process.env.stack}`] = undefined

    worker.numberOfRequests = 1
    worker.restartPromise = db().collection('tenants').updateAsync({ name: originalTenant }, update).then(() => worker.restart()).catch((e) => {
      winston.error(`restarting container ${worker.url} failed ${e.stack}`)
      lastHardError = e
      return Promise.resolve(worker)
    }).then(() => {
      winston.info(`restarting container ${worker.url} done`)
      worker.numberOfRequests = 0
      busyQueue.flush(run)
      return worker
    })
    return worker.restartPromise
  }

  const findWorker = (tenant, containerType, body) => {
    winston.info(`Searching to worker ${tenant}:${containerType}`)
    var worker = workers[containerType].find((w) => w.tenant === tenant)

    if (worker && worker !== -1) {
      winston.info('Already associated')
      worker.tenant = tenant
      worker.lastUsed = new Date()
      worker.numberOfRequests++

      return Promise.resolve(worker.restartPromise).then(() => ({ worker: worker }))
    }

    winston.info('Assigned worker not found, searching by LRU')
    worker = workers[containerType].reduce((prev, current) => (prev.lastUsed < current.lastUsed) ? prev : current)
    if (worker.numberOfRequests > 0) {
      winston.info('All workers are busy, queuing')
      return new Promise((resolve, reject) => {
        busyQueue.push(body, resolve, reject)
      })
    }

    const originalTenant = worker.tenant
    worker.tenant = tenant
    worker.lastUsed = new Date()

    if (!originalTenant) {
      worker.numberOfRequests++
      winston.info('No need to restart new worker')
      return Promise.resolve(worker.restartPromise).then(() => ({ worker: worker }))
    }

    return unregisterAndRestartWorker(worker, originalTenant).then(() => {
      worker.numberOfRequests = 1
      return { worker: worker }
    })
  }

  const warmup = (containerType) => {
    const worker = workers[containerType].reduce((prev, current) => (prev.lastUsed < current.lastUsed) ? prev : current)

    if (worker.numberOfRequests > 0) {
      return
    }

    if (!worker.tenant) {
      return
    }

    winston.info(`warming up ${worker.url}`)

    const originalTenant = worker.tenant
    worker.tenant = undefined
    return unregisterAndRestartWorker(worker, originalTenant)
  }

  const run = (body) => {
    winston.info(`Processing request ${body.tenant}:${body.containerType}`)

    if (body.isWin) {
      const winServer = findWinServer()
      if (!winServer) {
        winston.error('Unable to find availible windows worker server')
        return Promise.reject(new Error('Unable to find availible windows worker server'))
      }

      winston.info('posting to windows node ' + winServer)
      return new Promise((resolve, reject) => resolve(post({
        url: winServer,
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(body)
      })))
    }

    return db().collection('tenants').findOneAsync({ name: body.tenant }, { workerIp: 1 }).then((t) => {
      if (t.workerIp && t.workerIp[process.env.stack] && t.workerIp[process.env.stack] !== process.env.ip && stat(t.workerIp[process.env.stack])) {
        winston.info(`posting to external node ${t.workerIp[process.env.stack]}`)
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
        winston.info(`Executing local worker  ${body.tenant} in container ${body.containerType}`)

        return findWorker(body.tenant, body.containerType, body).then((output) => {
          if (output.result) {
            return output.result
          }

          const worker = output.worker
          winston.info(`Worker url for message ${worker.url}:${worker.numberOfRequests}`)

          return new Promise((resolve, reject) => resolve(post({
            url: worker.url,
            timeout: process.env.workerContainerTimeout || 50000,
            headers: {
              'Content-Type': 'application/json'
            },
            body: JSON.stringify(body.data)
          }).on('error', (e) => {
            winston.debug(`${body.tenant} failed request: ${JSON.stringify(body)}`)
            winston.error(e)
            unregisterAndRestartWorker(worker, body.tenant).then(() => {
              busyQueue.flush(run)
            })
          }).on('end', () => {
            worker.numberOfRequests--
            busyQueue.flush(run)
            warmup(body.containerType)
          })))
        })
      })
    })
  }

  return run(body)
}

let lastHardError
module.exports.lastHardError = () => {
  const lastError = lastHardError
  lastHardError = null
  return lastError
}
