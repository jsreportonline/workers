process.env['connectionString:uri'] = 'mongodb://localhost:27017/workers-test'
process.env['connectionString:database'] = 'workers-test'

process.env.stack = 'test'
process.env.ip = '0.0.0.0'

const should = require('should')
const mongo = require('../lib/mongo')
const execute = require('../lib/execute')
const stream = require('stream')

describe('execute', () => {
  let tenants
  let workers

  beforeEach(() => {
    workers = {
      t: [{
        url: 'http://localhost:2000'
      }]
    }

    return mongo().then(() => {
      tenants = mongo.db().collection('tenants')
      return tenants.removeAsync({})
    })
  })

  it('should proxy request when tenant has active worker', (done) => {
    const post = (opts) => {
      opts.url.should.be.eql('http://1.1.1.1:1000')
      done()
      return new stream.Readable()
    }

    const status = () => true

    tenants.insert({
      name: 'test',
      workerIp: {
        'test': '1.1.1.1'
      }
    }).then(() => execute({
      tenant: 'test',
      containerType: 't'
    }, post, status, workers)).catch(done)
  })

  it('should process request when tenant worker but it is not active', (done) => {
    const post = (opts) => {
      opts.url.should.be.eql('http://localhost:2000')
      done()
      return new stream.Readable()
    }

    const status = () => false

    tenants.insert({
      name: 'test',
      workerIp: {
        'test': '1.1.1.1'
      }
    }).then(() => execute({
      tenant: 'test',
      containerType: 't'
    }, post, status, workers)).catch(done)
  })

  it('should process request when tenant does not have a worker', (done) => {
    const post = (opts) => {
      opts.url.should.be.eql('http://localhost:2000')
      done()
      return new stream.Readable()
    }

    tenants.insert({
      name: 'test'
    }).then(() => execute({
      tenant: 'test',
      containerType: 't'
    }, post, () => true, workers)).catch(done)
  })

  it('should use already associated worker', (done) => {
    workers.t = [{
      url: 'http://localhost:2000',
      tenant: 'foo'
    }, {
      url: 'http://localhost:2001',
      tenant: 'test'
    }]

    const post = (opts) => {
      opts.url.should.be.eql('http://localhost:2001')
      done()
      return new stream.Readable()
    }

    tenants.insert({
      name: 'test'
    }).then(() => execute({
      tenant: 'test',
      containerType: 't'
    }, post, () => true, workers)).catch(done)
  })

  it('should find LRU worker', (done) => {
    workers.t = [{
      url: 'http://localhost:2000',
      lastUsed: 2
    }, {
      url: 'http://localhost:2001',
      lastUsed: 1
    }]

    const post = (opts) => {
      opts.url.should.be.eql('http://localhost:2001')
      done()
      return new stream.Readable()
    }

    tenants.insert({
      name: 'test'
    }).then(() => execute({
      tenant: 'test',
      containerType: 't'
    }, post, () => true, workers)).catch(done)
  })

  it('should unset old tenant worker ip in mongo', () => {
    workers.t = [{
      url: 'http://localhost:2000',
      tenant: 'a'
    }]
    workers.t[0].restart = () => workers.t[0]

    const post = (opts) => {
      const res = new stream.Readable()
      res.push(null)
      res.resume()
      return res
    }

    return tenants.insert({
      name: 'test'
    }).then(() => tenants.insert({
      name: 'a',
      workerIp: {
        'test': '0.0.0.0'
      }
    })).then(() => execute({
      tenant: 'test',
      containerType: 't'
    }, post, () => true, workers)).then(() => tenants.findOneAsync({ name: 'a' }).then((t) => {
      should(t.workerIp.test).not.be.ok()
    }))
  })

  it('should queue request when all workers are busy', (done) => {
    workers.t = [{
      url: 'http://localhost:2000',
      numberOfRequests: 1,
      tenant: 'a'
    }, {
      url: 'http://localhost:2001',
      numberOfRequests: 1,
      tenant: 'b'
    }]

    let queue = {
      push: () => done()
    }

    tenants.insert({
      name: 'test'
    }).then(() => execute({
      tenant: 'test',
      containerType: 't'
    }, () => true, () => true, workers, queue)).catch(done)
  })

  it('should restart worker before switching from other tenant', (done) => {
    let worker = {
      url: 'http://localhost:2000',
      tenant: 'a'
    }
    worker.restart = () => {
      done()
      return worker
    }

    workers.t = [worker]

    const post = (opts) => {
      opts.url.should.be.eql('http://localhost:2000')
      return new stream.Readable()
    }

    tenants.insert({
      name: 'test'
    }).then(() => execute({
      tenant: 'test',
      containerType: 't'
    }, post, () => true, workers)).catch(done)
  })

  it('should restart last used worker after process', (done) => {
    workers.t = [{
      url: 'http://localhost:2000',
      tenant: 'test'
    }, {
      url: 'http://localhost:2001',
      lastUsed: 1,
      tenant: 'a'
    }]

    workers.t[1].restart = () => {
      done()
      return workers.t[1]
    }

    const post = (opts) => {
      const res = new stream.Readable()
      res.push(null)
      res.resume()
      return res
    }

    tenants.insert({
      name: 'test'
    }).then(() => execute({
      tenant: 'test',
      containerType: 't'
    }, post, () => true, workers)).catch(done)
  })
})
