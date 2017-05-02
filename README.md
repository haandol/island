# island v1.5

An opinionated, full-stacked Microservices framework for [node](http://nodejs.org), powered by [TypeScript](https://github.com/microsoft/typescript).

[![Build Status][travis-image]][travis-url]
[![NPM version][npm-image]][npm-url]
[![Dependency Status][david-image]][david-url]
[![Coverage Status][coveralls-image]][coveralls-url]
[![Test Coverage][codeclimate-coverage]][codeclimate-url]
[![Code Climate][codeclimate-gpa]][codeclimate-url]
[![Issue Count][codeclimate-issue]][codeclimate-url]


```typescript
import * as island from 'island';
import * as keeper from 'island-keeper';
import { EndpointController } from './controller/endpoint.controller';
import { EventController } from './controller/event.controller';

const serviceName = 'hawaii';

class HawaiiIslet extends island.Islet {
  main() {
    const islandKeeper = keeper.IslandKeeper.getInst();
    islandKeeper.init('consul', 'island');
    islandKeeper.setServiceName(serviceName);

    const amqpChannelPoolAdapter = new island.AmqpChannelPoolAdapter({url: 'amqp://rabbitmq:5672'});
    this.registerAdapter('amqpChannelPool', amqpChannelPoolAdapter);
    const rpcAdapter = new island.RPCAdapter({amqpChannelPoolAdapter, serviceName});
    rpcAdapter.registerController(EndpointController);
    this.registerAdapter('rpc', rpcAdapter);

    const eventAdapter = new island.EventAdapter({amqpChannelPoolAdapter, serviceName});
    eventAdapter.registerController(EventController);
    this.registerAdapter('event', eventAdapter);
  }
}

island.Islet.run(HawaiiIslet);
```


## Table of Contents

  - [Installation](#installation)
  - [Features](#features)
  - [v1.0](#v1.0)
  - [Building](#building)
  - [Tests](#tests)
  - [Environment Variables](#environment+variables)
  - [Milestones](#milestones)
  - [People](#people)
  - [License](#license)


## Installation

```
$ npm install island --save
```


## Features

  - Free from service discovery
  - Support various types of communication
    - RPC(strong link between islands)
    - Event(weak link between islands)
    - Push messaging(to user) via `socket.io`
  - Ensure that each island gets proper parameters
  - Track communications per each request
  - Chain contexts with UUID per each request

## v1.2

### Changes

  - Support to expand langid from property name for @validate @sanitize [#69](https://github.com/spearhead-ea/island/issues/68)
  - Fix singleton bug [#64](https://github.com/spearhead-ea/island/pull/67)

## v1.0

### Changes

  - `Loggers` is no longer a part of `island` -> [island-loggers](https://github.com/spearhead-ea/island-loggers) [#14](https://github.com/spearhead-ea/island/issues/14)
  - `Di` is no longer a part of `island` -> [island-di](https://github.com/spearhead-ea/island-di) [#16](https://github.com/spearhead-ea/island/issues/16)
  - `@endpoint` decorator now provides 4 more methods [#28](https://github.com/spearhead-ea/island/issues/28)
    - `@endpoint('GET /test')` still works
    - `@endpoint.get('/test')` - You can omit the GET method
    - `@endpoint.post('/test')` - You can omit the POST method
    - `@endpoint.put('/test')` - You can omit the PUT method
    - `@endpoint.del('/test')` - You can omit the DEL method


### Breaking Changes

  - Require TypeScript@2.x
    - `strictNullChecks`


## Building

In order to build the island, ensure that you have [Git](http://git-scm.com/downloads) and [Node.js](http://nodejs.org/) installed.

Clone a copy of the repo:

```
$ git clone https://github.com/spearhead-ea/island.git
```

Change to the island directory:

```
$ cd island
```

Install prerequisites and dev dependencies:

```
$ npm install -g gulp typescript
$ npm install
```


## Tests

  To run the test suite, first install the dependencies, then run `npm test`:

```bash
$ npm install
$ RABBITMQ_HOST=localhost npm test
```


## Environment Variables

| Environment                  | Notes                                                             |
| ---------------------------- | ----------------------------------------------------------------- |
| `NODE_ENV`                   | When `development`, allows APIs which has options.developmentOnly |
| `HOSTNAME`                   | TraceLog uses this as a name of node                              |
| `ISLAND_RPC_EXEC_TIMEOUT_MS` | Timeout during execution (Defaults to 25000)                      |
| `ISLAND_RPC_WAIT_TIMEOUT_MS` | Timeout during call (Defaults to 60000)                           |
| `ISLAND_SERVICE_LOAD_TIME_MS`| Time to load service (Defaults to 60000)                          |
| `ISLAND_LOGGER_LEVEL`        | Logger level of category `island`                                 |
| `ISLAND_TRACEMQ_HOST`        | MQ(formatted by amqp URI) for TraceLog. If omitted it doesn't log |
| `ISLAND_TRACEMQ_QUEUE`       | A queue name to log TraceLog                                      |
| `RPC_NO_REVIVER`             | When `true`, RpcResponse not use JSON.parse reviver               |



## Milestones

For details on our planned features and future direction please refer to our [milestones](https://github.com/spearhead-ea/island/milestones)



## People

The original author of `island` is [Wonshik Kim](https://github.com/wokim)

The current lead maintainer is [Kei Son](https://github.com/heycalmdown)

[List of all contributors](https://github.com/spearhead-ea/island/graphs/contributors)



## License

  [MIT](LICENSE)


[travis-image]: https://api.travis-ci.org/spearhead-ea/island.svg?branch=release-1.0
[travis-url]: https://travis-ci.org/spearhead-ea/island
[npm-image]: https://badge.fury.io/js/island.svg
[npm-url]: http://badge.fury.io/js/island
[david-image]: https://david-dm.org/spearhead-ea/island/status.svg
[david-url]: https://david-dm.org/spearhead-ea/island
[coveralls-image]: https://coveralls.io/repos/github/spearhead-ea/island/badge.svg?branch=master
[coveralls-url]: https://coveralls.io/github/spearhead-ea/island?branch=master
[codeclimate-coverage]: https://codeclimate.com/github/spearhead-ea/island/badges/coverage.svg
[codeclimate-gpa]: https://codeclimate.com/github/spearhead-ea/island/badges/gpa.svg
[codeclimate-issue]: https://codeclimate.com/github/spearhead-ea/island/badges/issue_count.svg
[codeclimate-url]: https://codeclimate.com/github/spearhead-ea/island/coverage
