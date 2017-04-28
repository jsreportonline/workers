const Promise = require('bluebird')
const request = require('request')
const exec = Promise.promisify(require('child_process').exec)
const url = require('url')
const winston = require('winston')

const workers = {
  t: [],
  w: [],
  p: [],
  f: [],
  h: [],
  e: []
}

class Worker {
  constructor (url) {
    this.url = url
    this.lastUsed = new Date()
    this.numberOfRequests = 0
  }

  waitForPing () {
    let finished = false
    return new Promise((resolve, reject) => {
      const retry = () => {
        request.get(this.url, (err, resp) => {
          if (finished) {
            return
          }

          if (!err && resp.statusCode === 200) {
            finished = true
            return resolve()
          }

          setTimeout(retry, 100)
        })
      }

      retry()

      setTimeout(() => {
        if (!finished) {
          finished = true
          reject(new Error(`Unable to ping ${this.url}`))
        }
      }, 10000)
    })
  }

  restart () {
    const container = url.parse(this.url).hostname
    winston.info(`Restarting container ${container}`)

    const restartPromise = process.env.containerRestartPolicy === 'no'
      ? Promise.resolve(this) : exec(`timeout 5s docker restart -t 0 ${container}`)

    return restartPromise
      .then(() => this.waitForPing())
      .then(() => this)
  }
}

const mapEnvToWorkers = (type) => {
  let worker = (process.env['workerUrls:' + type] || '').split(' ').map((u) => new Worker(u))
  workers[type] = worker
}

mapEnvToWorkers('t')
mapEnvToWorkers('w')
mapEnvToWorkers('p')
mapEnvToWorkers('f')
mapEnvToWorkers('h')
mapEnvToWorkers('e')

module.exports = workers
