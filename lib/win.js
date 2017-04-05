
const Promise = require('bluebird')
const request = require('request')

let registeredServers
let healthyServers
let lastIndex = 0

const ping = () => {
  let refreshedHealthyServers = []
  return Promise.all(registeredServers.map((s) => new Promise((resolve, reject) => {
    request({
      url: s,
      timeout: 5000,
      method: 'GET'
    }, (err, res, body) => {
      if (!err && body === 'OK') {
        refreshedHealthyServers.push(s)
      }
      resolve()
    })
  }))).then(() => (healthyServers = refreshedHealthyServers.sort()))
}

module.exports = () => {
  registeredServers = process.env['workerUrls:win'].split(' ')
  setInterval(ping, 2000)
  return ping()
}

module.exports.findWinServer = () => {
  console.log(healthyServers)
  if (healthyServers.length === 0) {
    return null
  }

  // rotate servers
  lastIndex = (lastIndex + 1 >= healthyServers.length) ? 0 : lastIndex + 1
  return healthyServers[lastIndex]
}
