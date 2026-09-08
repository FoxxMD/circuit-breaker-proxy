import { circuitBreaker, handleWhen, isBrokenCircuitError, CircuitState, type CircuitBreakerPolicy, type ICircuitBreakerOptions } from 'cockatiel';
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
  comparer?: (clientA: T, clientB: T) => number | Promise<number>
}

export interface ProxyWithCircuitBreakerOptions<T> {
  /** Return true if the error should trigger a retry against the next client. Defaults to always retrying. */
  handleWhenCondition?: (error: Error) => boolean
  /** Use `Array.sort`'s comparator with provided client instances to determine selection order.
   *  May be sync or async (return a Promise<number>). When omitted, the default persisted
   *  round-robin selection is used instead. */
  comparer?: (clientA: T, clientB: T) => number | Promise<number>
}

const stateMap = new WeakMap<object, ProxyState<any>>()

/** Insertion sort so the (possibly async) comparer can be awaited pairwise; Array.sort requires sync compare fns. */
async function asyncSort<E>(items: E[], comparator: (a: E, b: E) => number | Promise<number>): Promise<E[]> {
  const result = [...items]
  for (let i = 1; i < result.length; i++) {
    const current = result[i]
    let j = i - 1
    while (j >= 0 && (await comparator(result[j], current)) > 0) {
      result[j + 1] = result[j]
      j--
    }
    result[j + 1] = current
  }
  return result
}

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
   * @param circuitBreakerOpts Factory for the cockatiel circuit breaker options, invoked once per client.
   * @param options Optional. `handleWhenCondition` defaults to always retrying when omitted; `comparer`
   *   defaults to the persisted round-robin selection when omitted.
   */
  static create<T extends object>(
    clients: T[],
    circuitBreakerOpts: () => ICircuitBreakerOptions,
    options?: ProxyWithCircuitBreakerOptions<T>
  ): ProxyWithCircuitBreaker<T> & T {
    return new ProxyWithCircuitBreaker<T>(clients, circuitBreakerOpts, options) as ProxyWithCircuitBreaker<T> & T
  }

  /**
   * @param clients An array of client instances accepting a callback in functions.
   * @param circuitBreakerOpts Factory for the cockatiel circuit breaker options, invoked once per client.
   * @param options Optional. `handleWhenCondition` defaults to always retrying when omitted; `comparer`
   *   defaults to the persisted round-robin selection when omitted.
   */
  constructor(
    clients: T[],
    circuitBreakerOpts: () => ICircuitBreakerOptions,
    options?: ProxyWithCircuitBreakerOptions<T>
  ) {
    const handleWhenCondition = options?.handleWhenCondition ?? (() => true)

    const registry: ClientEntry<T>[] = clients.map(client => ({
      client,
      breaker: circuitBreaker(handleWhen(handleWhenCondition), circuitBreakerOpts())
    }))

    // Store everything in the WeakMap to avoid instance collisions
    stateMap.set(this, {
      registry,
      currentIndex: 0,
      handleWhenCondition,
      comparer: options?.comparer
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

    let order: ClientEntry<T>[]
    if (state.comparer) {
      // COMPARER ORDER: User-supplied ordering (sync or async), fixed for the whole call.
      // Does not touch the round-robin pointer.
      order = await asyncSort(state.registry, (a, b) => state.comparer!(a.client, b.client))
    } else {
      // ATOMIC START: Grab our starting slot and move the global pointer once.
      // This ensures the NEXT request starts somewhere else.
      const startingIndex = state.currentIndex
      state.currentIndex = (startingIndex + 1) % len

      // LOCAL ROTATION: Rotate from our starting slot so this specific
      // request tries every client in order.
      order = Array.from({ length: len }, (_, i) => state.registry[(startingIndex + i) % len])
    }

    let attempts = 0
    let lastError: Error | null = null

    while (attempts < len) {
      const { client, breaker } = order[attempts];

      try {
        const method = (client as any)[methodName]
        if (typeof method !== 'function') {
          throw new Error(`Method ${methodName} not found on client`)
        }

        if(breaker.state === CircuitState.Open) {
          // not taking requests right now
          attempts++;
          continue;
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
