const MongoDB = require('mongodb')
const Promise = require('bluebird')
Promise.promisifyAll(MongoDB)
var db

module.exports = (cb) => {
  const connectionString = process.env['connectionString:uri']

  var options = {
    server: {
      auto_reconnect: true,
      socketOptions: {
        keepAlive: 1,
        connectTimeoutMS: 10000,
        socketTimeoutMS: 60000
      }
    },
    replSet: {
      auto_reconnect: true,
      socketOptions: {
        keepAlive: 1,
        connectTimeoutMS: 10000,
        socketTimeoutMS: 60000
      }
    }
  }

  return MongoDB.MongoClient.connectAsync(connectionString, options).then((adb) => {
    db = adb
  })
}

module.exports.db = () => db.db('multitenant-root')
