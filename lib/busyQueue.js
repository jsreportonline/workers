const timeoutWaitingForWorker = process.env.timeoutWaitingForWorker = 10000

class BusyQueue {

  constructor () {
    this.busyQueue = []
  }

  push (item, promise) {
    this.busyQueue.push({
      submittedOn: new Date().getTime(),
      item: item,
      promise: promise
    })
  }

  get length () {
    return this.busyQueue.length
  }

  flush (runHandler) {
    const item = this.busyQueue.shift()
    if (item) {
      if (item.submittedOn < (new Date().getTime() - timeoutWaitingForWorker)) {
        item.promise.reject(new Error('Timeout when waiting for worker'))

        return this.flush(runHandler)
      }

      console.log(`queue length: ${this.busyQueue.length}`)
      runHandler(item.item)
    }
  }
}

module.exports = new BusyQueue()