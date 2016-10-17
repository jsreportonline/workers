const timeoutWaitingForWorker = process.env.timeoutWaitingForWorker = 10000

class BusyQueue {

  constructor () {
    this.busyQueue = []
  }

  push (item, resolve, reject) {
    this.busyQueue.push({
      submittedOn: new Date().getTime(),
      item: item,
      resolve: resolve,
      reject: reject
    })
  }

  get length () {
    return this.busyQueue.length
  }

  flush (runHandler) {
    const item = this.busyQueue.shift()
    if (item) {
      if (item.submittedOn < (new Date().getTime() - timeoutWaitingForWorker)) {
        console.log('Timeout when waiting for worker')
        item.reject(new Error('Timeout when waiting for worker'))

        return this.flush(runHandler)
      }

      console.log(`queue length: ${this.busyQueue.length}`)
      runHandler(item.item).then((res) => item.resolve({ result: res })).catch(item.reject)
    }
  }
}

module.exports = new BusyQueue()
