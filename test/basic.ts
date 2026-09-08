import { strictEqual, deepStrictEqual, match, throws } from 'assert'
import { ProxyWithCircuitBreaker } from "../src/circuit-breaker-proxy.ts"
import { stub, restore, useFakeTimers, assert as _assert } from 'sinon'
import { ConsecutiveBreaker } from 'cockatiel'

interface DataClient {
  getData: ReturnType<typeof stub>
}

describe('ProxyWithCircuitBreaker', () => {
  let clientA: DataClient, clientB: DataClient, clientC: DataClient, handleWhen: ReturnType<typeof stub>, clients: DataClient[]

  const defaultOpts = () => ({
      halfOpenAfter: 10000,
      breaker: new ConsecutiveBreaker(3),
  })

  beforeEach(() => {
    // 1. Create mock clients with a dummy method
    clientA = { getData: stub() }
    clientB = { getData: stub() }
    clientC = { getData: stub() }

    clients = [clientA, clientB]

    // 2. Define a condition: handle errors where message is 'retry-me'
    handleWhen = stub().callsFake((err) => err.message === 'retry-me')
  })

  afterEach(() => {
    restore()
  })

  it('should successfully return data from the first client', (done) => {
    const proxy = ProxyWithCircuitBreaker.create(clients, defaultOpts, { handleWhenCondition: handleWhen })
    const expectedData = { id: 1 }

    // Setup: clientA succeeds
    clientA.getData.callsArgWith(1, null, expectedData)

    proxy.getData('param1', (err, result) => {
      try {
        strictEqual(err, null)
        deepStrictEqual(result, expectedData)
        strictEqual(clientA.getData.calledOnce, true)
        done()
      } catch (e) {
        done(e)
      }
    })
  })

  it('should rotate clients (Round Robin) on consecutive calls', (done) => {
    const proxy = ProxyWithCircuitBreaker.create(clients, defaultOpts, { handleWhenCondition: handleWhen })
    clientA.getData.callsArgWith(0, null, 'res1')
    clientB.getData.callsArgWith(0, null, 'res2')

    // First call uses clientA (index 0)
    proxy.getData((err1, res1) => {
      // Second call should use clientB (index 1)
      proxy.getData((err2, res2) => {
        try {
          strictEqual(res1, 'res1')
          strictEqual(res2, 'res2')
          strictEqual(clientA.getData.calledOnce, true)
          strictEqual(clientB.getData.calledOnce, true)
          done()
        } catch (e) {
          done(e)
        }
      })
    })
  })

  it('should failover to the next client if the first returns a handleable error', (done) => {
    const proxy = ProxyWithCircuitBreaker.create(clients, defaultOpts, { handleWhenCondition: handleWhen })
    const retryError = new Error('retry-me')
    const successData = 'recovered'

    // clientA fails with a handleable error, clientB succeeds
    clientA.getData.callsArgWith(0, retryError)
    clientB.getData.callsArgWith(0, null, successData)

    proxy.getData((err, result) => {
      try {
        strictEqual(result, successData)
        strictEqual(clientA.getData.calledOnce, true)
        strictEqual(clientB.getData.calledOnce, true)
        done()
      } catch (e) {
        done(e)
      }
    })
  })

  it('should throw immediately if the error is NOT handleable', (done) => {
    const proxy = ProxyWithCircuitBreaker.create(clients, defaultOpts, { handleWhenCondition: handleWhen })
    const fatalError = new Error('fatal-error')

    clientA.getData.callsArgWith(0, fatalError)

    proxy.getData((err, result) => {
      try {
        strictEqual(err, fatalError)
        strictEqual(clientB.getData.called, false, 'Should not have tried clientB')
        done()
      } catch (e) {
        done(e)
      }
    })
  })

  it('should trip the circuit breaker and reset after the timeout', async () => {
    const clock = useFakeTimers()

    const halfOpenAfter = 1000
    const proxy = ProxyWithCircuitBreaker.create([clientA], () => ({
      halfOpenAfter,
      breaker: new ConsecutiveBreaker(3),
    }), { handleWhenCondition: handleWhen })

    const retryError = new Error('retry-me')
    clientA.getData.callsArgWith(0, retryError)

    // Trip the breaker (3 failures)
    const callProxy = () => new Promise(resolve => proxy.getData(resolve))
    await callProxy()
    await callProxy()
    await callProxy()

    // Verify it is OPEN
    clientA.getData.resetHistory()
    await new Promise((resolve) => {
      proxy.getData(err => {
        strictEqual(clientA.getData.called, false, 'Should not call client when OPEN')
        match(err.message, /All clients unavailable/, 'Should return the "unavailable" error')
        resolve(null)
      })
    })

    // TELEPORT: Move time forward by 1001ms
    clock.tick(halfOpenAfter+1)

    // Verify it is now HALF-OPEN (allows a call)
    clientA.getData.resetBehavior()
    clientA.getData.callsArgWith(0, null, 'success-after-reset')

    await new Promise((resolve, reject) => {
      proxy.getData((err, result) => {
        try {
          strictEqual(err, null)
          strictEqual(result, 'success-after-reset')
          strictEqual(clientA.getData.calledOnce, true)
          resolve(null)
        } catch (e) {
          reject(e)
        }
      })
    })

    clock.restore()
  })

  it('should work with complex scenario - 3 clients and errors', async () => {
    const clock = useFakeTimers()

    const halfOpenAfter = 1000
    const proxy = ProxyWithCircuitBreaker.create([clientA, clientB, clientC], () => ({
      halfOpenAfter,
      breaker: new ConsecutiveBreaker(3),
    }), { handleWhenCondition: handleWhen })

    const retryError = new Error('retry-me')
    clientA.getData.callsArgWith(0, retryError)
    clientB.getData.callsArgWith(0, null, 'success')
    clientC.getData.callsArgWith(0, null, 'success')

    // Trip the breaker (3 failures)
    const callProxy = () => new Promise(resolve => proxy.getData(resolve))
    for(let i = 0; i < 12; i++) {
      await callProxy()
    }

    // Call number:     1   2  3   4   5  6   7   8  9  10 11 12
    // Called clients: A,B; B; C; A,B; B; C; A,B; B; C; B; B; C
    _assert.callCount(clientA.getData, 3)
    _assert.callCount(clientB.getData, 8)
    _assert.callCount(clientC.getData, 4)

    // Move clock after halfOpen interval
    clock.tick(halfOpenAfter+1)

    clientA.getData.resetHistory()
    clientB.getData.resetHistory()
    clientC.getData.resetHistory()

    for(let i = 0; i < 6; i++) {
      await callProxy()
    }

    // clientA shall be called once before removed again due to halfOpen state
    // Call number:     1   2  3  4  5  6 
    // Called clients: A,B; B; C; B; B; C
    _assert.callCount(clientA.getData, 1)
    _assert.callCount(clientB.getData, 4)
    _assert.callCount(clientC.getData, 2)

    // Move clock after halfOpen interval
    clock.tick(halfOpenAfter+1)
    clientA.getData.resetHistory()
    clientB.getData.resetHistory()
    clientC.getData.resetHistory()

    clientA.getData.resetBehavior()
    // Make the first call succeed to get into breaker.Open state
    clientA.getData.onCall(0).callsArgWith(0, null, 'success').callsArgWith(0, retryError)

    for(let i = 0; i < 15; i++) {
      await callProxy()
    }

    // clientA is again called 3 times before opening the circuit breaker
    // Call number:     1  2  3   4   5  6   7   8  9   10  11 12 13 14 15
    // Called clients:  A; B; C; A,B; B; C; A,B; B; C; A,B; B; C; B; B; C
    _assert.callCount(clientA.getData, 4)
    _assert.callCount(clientB.getData, 9)
    _assert.callCount(clientC.getData, 5)

    clock.restore()
  })

  it('should throw error if the last argument is not a function', () => {
    const proxy = ProxyWithCircuitBreaker.create([clientA], defaultOpts, { handleWhenCondition: handleWhen })
    throws(() => {
      proxy.getData('no-callback-here')
    }, /Method getData expected a callback function/)
  })
})
