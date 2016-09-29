const db = require('./mongo').db

var cache = []

const status = () => {
  return db().collection('servers').find({ stack: process.env.stack }).toArrayAsync().then((res) => {
    cache = res
  })
}

module.exports = () => {
  setInterval(status, 5000)
  return status()
}

module.exports.status = (workerIp) => {
  const server = cache.find((s) => s.ip === workerIp.ip)

  return server && server.ping > new Date(Date.now() - 20000)
}
