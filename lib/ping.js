const db = require('./mongo').db

const ping = () => {
  return db().collection('servers').updateAsync({ ip: process.env.ip, stack: process.env.stack }, { $set: { ping: new Date(), stack: process.env.stack } }, { upsert: true })
}

module.exports = (cb) => {
  setInterval(ping, 5000)
  return ping()
}
