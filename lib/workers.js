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

class Worker {

  constructor (url) {
    this.url = url
    this.lastUsed = new Date()
    this.numberOfRequests = 0
  }

  waitForPing () {
    let retries = 1000

    return new Promise((resolve, reject) => {
      const retry = () => {
        request.get(this.url, (err, resp) => {
          if (!err && resp.statusCode === 200) {
            return resolve()
          }

          if (--retries === 0) {
            return reject(new Error(`Unable to ping ${this.url}`))
          }

          setTimeout(retry, 100)
        })
      }

      retry()
    })
  }

  restart () {
    const container = url.parse(this.url).hostname
    console.log(`Restarting container ${container}`)

    const restartPromise = process.env.containerRestartPolicy === 'no'
      ? Promise.resolve(this) : exec(`docker restart -t 0 ${container}`)

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

module.exports = workers


