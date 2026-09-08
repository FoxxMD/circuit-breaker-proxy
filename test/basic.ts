import { strictEqual, deepStrictEqual, match, rejects } from 'assert'
import { ProxyWithCircuitBreaker, type CircuitBreakerProxy } from "../src/circuit-breaker-proxy.ts"
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

  it('should successfully return data from the first client', async () => {
    const proxy = ProxyWithCircuitBreaker.create(clients, defaultOpts, { handleWhenCondition: handleWhen })
    const expectedData = { id: 1 }

    // Setup: clientA succeeds
    clientA.getData.resolves(expectedData)

    const result = await proxy.getData('param1')
    deepStrictEqual(result, expectedData)
    strictEqual(clientA.getData.calledOnceWith('param1'), true)
  })

  it('should rotate clients (Round Robin) on consecutive calls', async () => {
    const proxy = ProxyWithCircuitBreaker.create(clients, defaultOpts, { handleWhenCondition: handleWhen })
    clientA.getData.resolves('res1')
    clientB.getData.resolves('res2')

    // First call uses clientA (index 0)
    const res1 = await proxy.getData()
    // Second call should use clientB (index 1)
    const res2 = await proxy.getData()

    strictEqual(res1, 'res1')
    strictEqual(res2, 'res2')
    strictEqual(clientA.getData.calledOnce, true)
    strictEqual(clientB.getData.calledOnce, true)
  })

  it('should failover to the next client if the first returns a handleable error', async () => {
    const proxy = ProxyWithCircuitBreaker.create(clients, defaultOpts, { handleWhenCondition: handleWhen })
    const retryError = new Error('retry-me')
    const successData = 'recovered'

    // clientA fails with a handleable error, clientB succeeds
    clientA.getData.rejects(retryError)
    clientB.getData.resolves(successData)

    const result = await proxy.getData()
    strictEqual(result, successData)
    strictEqual(clientA.getData.calledOnce, true)
    strictEqual(clientB.getData.calledOnce, true)
  })

  it('should throw immediately if the error is NOT handleable', async () => {
    const proxy = ProxyWithCircuitBreaker.create(clients, defaultOpts, { handleWhenCondition: handleWhen })
    const fatalError = new Error('fatal-error')

    clientA.getData.rejects(fatalError)

    await rejects(proxy.getData(), fatalError)
    strictEqual(clientB.getData.called, false, 'Should not have tried clientB')
  })

  it('should trip the circuit breaker and reset after the timeout', async () => {
    const clock = useFakeTimers()

    const halfOpenAfter = 1000
    const proxy = ProxyWithCircuitBreaker.create([clientA], () => ({
      halfOpenAfter,
      breaker: new ConsecutiveBreaker(3),
    }), { handleWhenCondition: handleWhen })

    const retryError = new Error('retry-me')
    clientA.getData.rejects(retryError)

    // Trip the breaker (3 failures)
    const callProxy = () => proxy.getData().catch((e: Error) => e)
    await callProxy()
    await callProxy()
    await callProxy()

    // Verify it is OPEN
    clientA.getData.resetHistory()
    const openErr = await callProxy() as Error
    strictEqual(clientA.getData.called, false, 'Should not call client when OPEN')
    match(openErr.message, /All clients unavailable/, 'Should return the "unavailable" error')

    // TELEPORT: Move time forward by 1001ms
    clock.tick(halfOpenAfter+1)

    // Verify it is now HALF-OPEN (allows a call)
    clientA.getData.resetBehavior()
    clientA.getData.resolves('success-after-reset')

    const result = await proxy.getData()
    strictEqual(result, 'success-after-reset')
    strictEqual(clientA.getData.calledOnce, true)

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
    clientA.getData.rejects(retryError)
    clientB.getData.resolves('success')
    clientC.getData.resolves('success')

    // Trip the breaker (3 failures)
    const callProxy = () => proxy.getData().catch(() => undefined)
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
    clientA.getData.rejects(retryError)
    // Make the first call succeed to get into breaker.Open state
    clientA.getData.onCall(0).resolves('success')

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

  it('should support synchronous methods (still returns a Promise)', async () => {
    class SyncClient {
      add(a: number, b: number) {
        return a + b
      }
    }

    const proxy = ProxyWithCircuitBreaker.create([new SyncClient()], defaultOpts)
    const result = proxy.add(2, 3)
    strictEqual(result instanceof Promise, true)
    strictEqual(await result, 5)
  })

  it('should pass through any number/shape of arguments unchanged', async () => {
    const spyClient = { call: stub().resolves('ok') }
    const proxy = ProxyWithCircuitBreaker.create([spyClient], defaultOpts)

    const objArg = { nested: { value: 1 } }
    const fnArg = () => 'callback-shaped-arg'

    const result = await proxy.call('a', 2, objArg, fnArg)
    strictEqual(result, 'ok')
    strictEqual(spyClient.call.calledOnceWith('a', 2, objArg, fnArg), true)
  })
})
