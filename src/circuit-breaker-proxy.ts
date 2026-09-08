import { circuitBreaker, handleWhen, isBrokenCircuitError, type CircuitBreakerPolicy, type ICircuitBreakerOptions } from 'cockatiel';
import { promisify } from 'node:util';

const EXECUTE_WITH_CALLBACK = Symbol('executeWithCallback')
const EXECUTE_RESILIENTLY = Symbol('executeResiliently')

interface ClientEntry<T> {
  client: T
  breaker: CircuitBreakerPolicy
}

interface ProxyState<T> {
  registry: ClientEntry<T>[]
  currentIndex: number
  handleWhenCondition: (error: Error) => boolean
}

const stateMap = new WeakMap<object, ProxyState<any>>()

/**
 * A proxy that round-robins calls across a set of callback-based clients, applying
 * circuit breaker logic (via cockatiel) to each client individually.
 *
 * The returned instance is also typed as `T`, so any method available on a client
 * is available (and callable with the same callback signature) on the proxy.
 */
export class ProxyWithCircuitBreaker<T extends object = object> {
  /**
   * @param clients An array of client instances accepting a callback in functions.
   * @param handleWhenCondition Return true if the error should trigger a retry against the next client.
   * @param circuitBreakerOpts Factory for the cockatiel circuit breaker options, invoked once per client.
   */
  static create<T extends object>(
    clients: T[],
    handleWhenCondition: (error: Error) => boolean,
    circuitBreakerOpts: () => ICircuitBreakerOptions
  ): ProxyWithCircuitBreaker<T> & T {
    return new ProxyWithCircuitBreaker<T>(clients, handleWhenCondition, circuitBreakerOpts) as ProxyWithCircuitBreaker<T> & T
  }

  /**
   * @param clients An array of client instances accepting a callback in functions.
   * @param handleWhenCondition Return true if the error should trigger a retry against the next client.
   * @param circuitBreakerOpts Factory for the cockatiel circuit breaker options, invoked once per client.
   */
  constructor(clients: T[], handleWhenCondition: (error: Error) => boolean, circuitBreakerOpts: () => ICircuitBreakerOptions) {
    const registry: ClientEntry<T>[] = clients.map(client => ({
      client,
      breaker: circuitBreaker(handleWhen(handleWhenCondition), circuitBreakerOpts())
    }))

    // Store everything in the WeakMap to avoid instance collisions
    stateMap.set(this, {
      registry,
      currentIndex: 0,
      handleWhenCondition
    })

    return new Proxy(this, {
      get: (target, prop) => {
        // Prioritize Internal Symbols (for logic inside the class)
        if (typeof prop === 'symbol') {
          return (target as any)[prop]
        }

        return (...args: any[]) => (target as any)[EXECUTE_WITH_CALLBACK](prop, args)
      }
    }) as this
  }

  private [EXECUTE_WITH_CALLBACK](methodName: string, args: any[]): void {
    const callback = args.pop()

    if (typeof callback !== 'function') {
      throw new Error(`Method ${methodName} expected a callback function.`)
    }

    this[EXECUTE_RESILIENTLY](methodName, args)
      .then(result => callback(null, result))
      .catch(err => callback(err))
  }

  private async [EXECUTE_RESILIENTLY](methodName: string, args: any[]): Promise<any> {
    const state = stateMap.get(this)!
    const len = state.registry.length

    // ATOMIC START: Grab our starting slot and move the global pointer once.
    // This ensures the NEXT request starts somewhere else.
    const startingIndex = state.currentIndex
    state.currentIndex = (startingIndex + 1) % len

    let attempts = 0
    let lastError: Error | null = null

    while (attempts < len) {
      // LOCAL ROTATION: We calculate our index based on our starting slot.
      // This ensures this specific request tries every client in order.
      const index = (startingIndex + attempts) % len;
      const { client, breaker } = state.registry[index];

      try {
        const method = (client as any)[methodName]
        if (typeof method !== 'function') {
          throw new Error(`Method ${methodName} not found on client`)
        }

        const fn = promisify(method).bind(client)
        const res = await breaker.execute(() => fn(...args))
        return res
      } catch (error) {
        if (isBrokenCircuitError(error)) {
          attempts++
          continue
        }

        // Only retry if it's a "handleable" error
        if (!state.handleWhenCondition(error as Error)) throw error

        lastError = error as Error
        attempts++
      }
    }

    throw lastError || new Error("All clients unavailable or failed")
  }
}
