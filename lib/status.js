const db = require('./mongo').db

var cache = []

const status = () => {
  return db().collection('servers').find({}).toArrayAsync().then((res) => {
    cache = res
  })
}

module.exports = () => {
  setInterval(status, 5000)
  return status()
}

module.exports.status = (ip) => {
  const server = cache.find(ip)

  return server && server.ping > new Date(Date.now() - 10000)
}
