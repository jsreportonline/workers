const db = require('./mongo').db

const ping = () => {
  return db().collection('servers').updateAsync({ ip: process.env.ip }, { $set: { ping: new Date() } }, { upsert: true })
}

module.exports = (cb) => {
  setInterval(ping, 5000)
  return ping()
}