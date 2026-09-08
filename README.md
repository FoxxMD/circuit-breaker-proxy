# Circuit break proxy

A Node.js proxy (not an HTTP proxy) which supports applying circuit breaker logic across a set of generic clients.
You provide a list of generic clients and this package routes calls to the clients in a round-robin fashion.
If any of the clients fails to process a function call, the call is retried with the next client.
If a client continuously fails to respond to function calls, it can be temporarily removed from the list.

- Non-function properties on a client are passed straight through, read from the first client.
- Uses [cockatiel](https://github.com/connor4312/cockatiel) to handle circuit breaker logic

## Example
A good example is with a `node-etcd` client across many different hosts where each host is expected to provide the same response:

```ts
import { ProxyWithCircuitBreaker } from '@ceecko/circuit-breaker-proxy';
import { Etcd } from 'node-etcd';

const proxy = ProxyWithCircuitBreaker.create([
  new Etcd('host1.example.com'),
  new Etcd('host2.example.com'),
  new Etcd('host3.example.com'),
], () => ({
    // If a client fails 3 times in a row, remove it for 10 seconds
    halfOpenAfter: 10000,
    breaker: new ConsecutiveBreaker(3),
}), {
  handleWhenCondition: err => {
    // If the error is related to network, continue with the next client.
    if(err instanceof NetworkError) return true

    // If it's non-network related error, return it straightaway
    return false
  }
})

const data = await proxy.get('key')
// ... process data
```

## BYO Order

Provide a `comparer` function to determine the client selection order at function call time.

This comparison is done *once* when the function is initially called and the order is then set for the duration of the function call/attempts.

`comparer` may be sync or async (return a `Promise<number>`) — either way it is awaited before continuing.

```ts
import { ProxyWithCircuitBreaker } from '@ceecko/circuit-breaker-proxy';
import {MyCoolClient} from './myModule';

const proxy = ProxyWithCircuitBreaker.create([
  new MyCoolClient('a'),
  new MyCoolClient('b'),
], ({
    // If a client fails 3 times in a row, remove it for 10 seconds
    halfOpenAfter: 10000,
    breaker: new ConsecutiveBreaker(3),
}),
{
  comparer: (clientA, clientB) => {
    const aPrior = clientA.resolvePriority(); // 2
    const bPRior = clientB.resolvePriority(); // 4
    // MyCoolClient('b') will be ordered first
    return aPrior - bPRior;
  }
});

proxy.doSomething(); // first call uses MyCoolClient('b')
```