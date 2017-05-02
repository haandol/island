import * as cls from 'continuation-local-storage';

import * as amqp from 'amqplib';
import * as Bluebird from 'bluebird';
import * as _ from 'lodash';
import * as os from 'os';
import uuid = require('uuid');

import { RpcOptions } from '../controllers/rpc-decorator';
import { sanitize, validate } from '../middleware/schema.middleware';
import { AbstractError, AbstractFatalError, AbstractLogicError, FatalError, ISLAND, LogicError } from '../utils/error';
import { logger } from '../utils/logger';
import reviver from '../utils/reviver';
import { TraceLog } from '../utils/tracelog';
import { AmqpChannelPoolService } from './amqp-channel-pool-service';

const RPC_EXEC_TIMEOUT_MS = parseInt(process.env.ISLAND_RPC_EXEC_TIMEOUT_MS, 10) || 25000;
const RPC_WAIT_TIMEOUT_MS = parseInt(process.env.ISLAND_RPC_WAIT_TIMEOUT_MS, 10) || 60000;
const SERVICE_LOAD_TIME_MS = parseInt(process.env.ISLAND_SERVICE_LOAD_TIME_MS, 10) || 60000;
const RPC_QUEUE_EXPIRES_MS = RPC_WAIT_TIMEOUT_MS + SERVICE_LOAD_TIME_MS;
const NO_REVIVER = process.env.RPC_NO_REVIVER === 'true';

export interface IConsumerInfo {
  channel: amqp.Channel;
  tag: string;
  options?: RpcOptions;
  key: string;
  consumer: (msg: any) => Promise<void>;
  consumerOpts: any;
}

interface Message {
  content: Buffer;
  properties: amqp.Options.Publish;
}

interface IRpcResponse {
  version: number;
  result: boolean;
  body?: AbstractError | any;
}

export interface RpcRequest {
  name: string;
  msg: any;
  options: RpcOptions;
}

class RpcResponse {
  static reviver: ((k, v) => any) | undefined = reviver;
  static encode(body: any, serviceName: string): Buffer {
    const res: IRpcResponse = {
      body,
      result: body instanceof Error ? false : true,
      version: 1
    };

    return new Buffer(JSON.stringify(res, (k, v: AbstractError | number | boolean) => {
      // TODO instanceof Error should AbstractError
      if (v instanceof Error) {
        const e = v as AbstractError;
        return {
          debugMsg: e.debugMsg,
          errorCode: e.errorCode,
          errorKey: e.errorKey,
          errorNumber: e.errorNumber,
          errorType: e.errorType,
          extra: e.extra,
          message: e.message,
          name: e.name,
          occurredIn: serviceName,
          stack: e.stack,
          statusCode: e.statusCode
        };
      }
      return v;
    }), 'utf8');
  }

  static decode(msg: Buffer): IRpcResponse {
    if (!msg) return { version: 0, result: false };
    try {
      const reviver = !NO_REVIVER && RpcResponse.reviver || undefined;
      const res = JSON.parse(msg.toString('utf8'), reviver);
      if (!res.result) res.body = this.getAbstractError(res.body);

      return res;
    } catch (e) {
      logger.notice('[decode error]', e);
      return { version: 0, result: false };
    }
  }

  static getAbstractError(err: AbstractError): AbstractError {
    let result: AbstractError;
    const enumObj = {};
    enumObj[err.errorNumber] = err.errorKey;
    switch (err.errorType) {
      case 'LOGIC':
        result = new AbstractLogicError(err.errorNumber, err.debugMsg, err.occurredIn, enumObj);
        break;
      case 'FATAL':
        result = new AbstractFatalError(err.errorNumber, err.debugMsg, err.occurredIn, enumObj);
        break;
      default:
        result = new AbstractError('ETC', 1, err.message, err.occurredIn, { 1: 'F0001' });
        result.name = 'ETCError';
    }

    result.statusCode = err.statusCode;
    result.stack = err.stack;
    result.extra = err.extra;
    result.occurredIn = err.occurredIn;

    return result;
  }
}

function enterScope(properties: any, func): Promise<any> {
  return new Promise((resolve, reject) => {
    const ns = cls.getNamespace('app');
    ns.run(() => {
      _.each(properties, (value, key: string) => {
        ns.set(key, value);
      });
      Bluebird.try(func).then(resolve).catch(reject);
    });
  });
}

export type RpcHook = (rpc) => Promise<any>;
export enum RpcHookType {
  PRE_ENDPOINT,
  POST_ENDPOINT,
  PRE_RPC,
  POST_RPC,
  PRE_ENDPOINT_ERROR,
  POST_ENDPOINT_ERROR,
  PRE_RPC_ERROR,
  POST_RPC_ERROR
}

export interface InitializeOptions {
  noReviver: boolean;
}

export default class RPCService {
  private consumerInfosMap: { [name: string]: IConsumerInfo } = {};
  private responseQueue: string;
  private responseConsumerInfo: IConsumerInfo;
  private reqExecutors: { [corrId: string]: (msg: Message) => Promise<any> } = {};
  private reqTimeouts: { [corrId: string]: any } = {};
  private channelPool: AmqpChannelPoolService;
  private serviceName: string;
  private hooks: { [key: string]: RpcHook[] };

  constructor(serviceName?: string) {
    this.serviceName = serviceName || 'unknown';
    this.hooks = {};
  }

  public async initialize(channelPool: AmqpChannelPoolService, opts?: InitializeOptions): Promise<any> {
    if (opts && opts.noReviver) {
      RpcResponse.reviver = undefined;
    }
    // NOTE: live docker 환경에서는 같은 hostname + process.pid 조합이 유일하지 않을 수 있다
    // docker 내부의 process id 는 1인 경우가 대부분이며 host=net으로 실행시키는 경우 hostname도 동일할 수 있다.
    this.responseQueue = `rpc.res.${this.serviceName}.${os.hostname()}.${uuid.v4()}`;
    logger.info(`consuming ${this.responseQueue}`);
    const consumer = (msg: Message) => {
      if (!msg) {
        logger.error(`[WARN] msg is null. consume was canceled unexpectedly`);
      }
      const correlationId = msg.properties.correlationId || 'no-correlation-id';
      const reqExecutor = this.reqExecutors[correlationId];
      if (!reqExecutor) {
        // Request timeout이 생겨도 reqExecutor가 없음
        logger.notice(`[RPC-WARNING] invalid correlationId ${correlationId}`);
        return Promise.resolve();
      }
      delete this.reqExecutors[correlationId];
      return reqExecutor(msg);
    };

    await TraceLog.initialize();

    this.channelPool = channelPool;
    await channelPool.usingChannel(channel => channel.assertQueue(this.responseQueue, { exclusive: true }));
    this.responseConsumerInfo = await this._consume(this.responseQueue, consumer, 'RequestExecutors', {});
  }

  public _publish(exchange: any, routingKey: any, content: any, options?: any) {
    return this.channelPool.usingChannel(channel => {
      return Promise.resolve(channel.publish(exchange, routingKey, content, options));
    });
  }

  public purge() {
    // TODO: cancel consume
    this.hooks = {};
    return Promise.resolve();
  }

  public registerHook(type: RpcHookType, hook: RpcHook) {
    this.hooks[type] = this.hooks[type] || [];
    this.hooks[type].push(hook);
  }

  // [TODO] register의 consumer와 _consume의 anonymous function을 하나로 합쳐야 한다.
  // 무척 헷갈림 @kson //2016-08-09
  // [TODO] Endpoint도 동일하게 RpcService.register를 부르는데, rpcOptions는 Endpoint가 아닌 RPC만 전달한다
  // 포함 관계가 잘못됐다. 애매하다. @kson //2016-08-09
  public async register(name: string,
                        handler: (req: any) => Promise<any>,
                        type: 'endpoint' | 'rpc',
                        rpcOptions?: RpcOptions): Promise<void> {
    const consumer = (msg: Message) => {
      if (!msg.properties.replyTo) throw ISLAND.FATAL.F0026_MISSING_REPLYTO_IN_RPC;
      const replyTo = msg.properties.replyTo;
      const headers = msg.properties.headers;
      const tattoo = headers && headers.tattoo;
      const timestamp = msg.properties.timestamp || 0;
      const log = new TraceLog(tattoo, timestamp);
      log.size = msg.content.byteLength;
      log.from = headers.from;
      log.to = { node: process.env.HOSTNAME, context: name, island: this.serviceName, type: 'rpc' };
      return enterScope({ RequestTrackId: tattoo, Context: name, Type: 'rpc' }, () => {
        let content = JSON.parse(msg.content.toString('utf8'), reviver);
        if (rpcOptions) {
          if (_.get(rpcOptions, 'schema.query.sanitization')) {
            content = sanitize(rpcOptions.schema!.query!.sanitization, content);
          }
          if (_.get(rpcOptions, 'schema.query.validation')) {
            if (!validate(rpcOptions.schema!.query!.validation, content)) {
              throw new LogicError(ISLAND.LOGIC.L0002_WRONG_PARAMETER_SCHEMA, `Wrong parameter schema`);
            }
          }
        }

        logger.debug(`Got ${name} with ${JSON.stringify(content)}`);

        // Should wrap with Bluebird.try while handler sometimes returns ES6 Promise which doesn't support timeout.
        const options: amqp.Options.Publish = { correlationId: msg.properties.correlationId, headers };
        return Bluebird.try(async () => {
          const req = content;
          if (type === 'endpoint') {
            return await this.dohook(RpcHookType.PRE_ENDPOINT, req);
          } else { // rpc
            return await this.dohook(RpcHookType.PRE_RPC, req);
          }
        })
          .then(req => handler(req))
          .then(async res => {
            if (type === 'endpoint') {
              return await this.dohook(RpcHookType.POST_ENDPOINT, res);
            } else { // rpc
              return await this.dohook(RpcHookType.POST_RPC, res);
            }
          })
          .then(res => {
            logger.debug(`responses ${JSON.stringify(res)}`);
            log.end();
            if (rpcOptions) {
              if (_.get(rpcOptions, 'schema.result.sanitization')) {
                res = sanitize(rpcOptions.schema!.result!.sanitization, res);
              }
              if (_.get(rpcOptions, 'schema.result.validation')) {
                validate(rpcOptions.schema!.result!.validation, res);
              }
            }
            this.channelPool.usingChannel(channel => {
              return Promise.resolve(channel.sendToQueue(replyTo, RpcResponse.encode(res, this.serviceName), options));
            });
          })
          .timeout(RPC_EXEC_TIMEOUT_MS)
          .catch(async err => {
            if (type === 'endpoint') {
              err = await this.dohook(RpcHookType.PRE_ENDPOINT_ERROR, err);
            } else { // rpc
              err = await this.dohook(RpcHookType.PRE_RPC_ERROR, err);
            }
            log.end(err);
            // 503 오류일 때는 응답을 caller에게 안보내줘야함
            if (err.statusCode && parseInt(err.statusCode, 10) === 503) {
              throw err;
            }
            if (!err.extra) {
              err.extra = { island: this.serviceName, name, req: content };
            }
            const extra = err.extra;
            logger.error(`Got an error during ${extra.island}/${extra.name}` +
              ` with ${JSON.stringify(extra.req)} - ${err.stack}`);
            return this.channelPool.usingChannel(channel => {
              return Promise.resolve(channel.sendToQueue(replyTo, RpcResponse.encode(err, this.serviceName), options));
            }).then(async () => {
              if (type === 'endpoint') {
                err = await this.dohook(RpcHookType.POST_ENDPOINT_ERROR, err);
              } else { // rpc
                err = await this.dohook(RpcHookType.POST_RPC_ERROR, err);
              }
              throw err;
            });
          })
          .finally(() => {
            log.shoot();
          });
      });
    };

    // NOTE: 컨슈머가 0개 이상에서 0개가 될 때 자동으로 삭제된다.
    // 단 한번도 컨슈머가 등록되지 않는다면 영원히 삭제되지 않는데 그런 케이스는 없음
    await this.channelPool.usingChannel(channel => channel.assertQueue(name, {
                arguments : {'x-expires': RPC_QUEUE_EXPIRES_MS},
                durable   : false
    }));
    this.consumerInfosMap[name] = await this._consume(name, consumer, 'SomeoneCallsMe');
  }

  public async pause(name: string) {
    const consumerInfo = this.consumerInfosMap[name];
    if (!consumerInfo) return;
    await consumerInfo.channel.cancel(consumerInfo.tag);
  }

  public async resume(name: string) {
    const consumerInfo = this.consumerInfosMap[name];
    if (!consumerInfo) return;
    await consumerInfo.channel.consume(consumerInfo.key, consumerInfo.consumer);
  }

  public async unregister(name: string) {
    const consumerInfo = this.consumerInfosMap[name];
    if (!consumerInfo) return;

    await this._cancel(consumerInfo);
    delete this.consumerInfosMap[name];
  }

  public async invoke<T, U>(name: string, msg: T, opts?: any): Promise<U>;
  public async invoke(name: string, msg: any, opts?: any): Promise<any>;
  public async invoke(name: string, msg: any, opts?: any): Promise<any> {
    const ns = cls.getNamespace('app');
    const tattoo = ns.get('RequestTrackId');
    const context = ns.get('Context');
    const type = ns.get('Type');
    const correlationId = uuid.v4();
    const headers = {
      tattoo,
      from: { node: process.env.HOSTNAME, context, island: this.serviceName, type }
    };
    const content = new Buffer(JSON.stringify(msg), 'utf8');
    const options: amqp.Options.Publish = {
      correlationId,
      expiration: `${RPC_WAIT_TIMEOUT_MS}`, // [FIXME] https://github.com/louy/typed-amqplib/pull/1
      headers,
      replyTo: this.responseQueue,
      timestamp: +(new Date())
    };
    const p = this.markTattoo(name, correlationId, tattoo, ns, opts)
      .catch(err => {
        err.tattoo = tattoo;
        throw err;
      });

    try {
      await this.channelPool.usingChannel(channel => {
        return Promise.resolve(channel.sendToQueue(name, content, options));
      });
    } catch (e) {
      clearTimeout(this.reqTimeouts[correlationId]);
      delete this.reqTimeouts[correlationId];
      delete this.reqExecutors[correlationId];
      throw e;
    }
    return await p;
  }

  protected async _consume(key: string, handler: (msg) => Promise<any>, tag: string, consumerOpts?: any):
    Promise<IConsumerInfo> {
    const channel = await this.channelPool.acquireChannel();
    await channel.prefetch(+process.env.RPC_PREFETCH || 1000);

    const consumer = async msg => {
      try {
        await handler(msg);
        channel.ack(msg);
      } catch (error) {
        if (error.statusCode && parseInt(error.statusCode, 10) === 503) {
          // Requeue the message when it has a chance
          setTimeout(() => {
            channel.nack(msg);
          }, 1000);
          return;
        }

        // Discard the message
        channel.ack(msg);

        this.channelPool.usingChannel(channel => {
          const content = RpcResponse.encode(error, this.serviceName);
          const headers = msg.properties.headers;
          const correlationId = msg.properties.correlationId;
          const properties: amqp.Options.Publish = { correlationId, headers };
          return Promise.resolve(channel.sendToQueue(msg.properties.replyTo, content, properties));
        });
      }
    };
    const result = await channel.consume(key, consumer, consumerOpts || {});

    return { channel, tag: result.consumerTag, key, consumer, consumerOpts };
  }

  protected async _cancel(consumerInfo: IConsumerInfo): Promise<void> {
    await consumerInfo.channel.cancel(consumerInfo.tag);
    await this.channelPool.releaseChannel(consumerInfo.channel);
  }

  private async dohook(type: RpcHookType, value) {
    if (!this.hooks[type]) return value;
    return Bluebird.reduce(this.hooks[type], async (value, hook) => await hook(value), value);
  }

  private markTattoo(name: string, corrId: any, tattoo: any, ns: any, opts: any): Promise<any> {
    return new Promise((resolve, reject) => {
      // 지저분한데 bluebird .timeout으로 교체할 방법 없을까?
      // @kson //2016-08-11
      this.reqTimeouts[corrId] = setTimeout(() => {
        // Cleanup registered response executors
        delete this.reqTimeouts[corrId];
        delete this.reqExecutors[corrId];

        const err = new FatalError(
          ISLAND.FATAL.F0023_RPC_TIMEOUT,
          `RPC(${name} does not return in ${RPC_WAIT_TIMEOUT_MS} ms`
        );
        err.statusCode = 504;
        return reject(err);
      }, RPC_WAIT_TIMEOUT_MS);

      this.reqExecutors[corrId] = ns.bind((msg: Message) => {
        clearTimeout(this.reqTimeouts[corrId]);
        delete this.reqTimeouts[corrId];

        const res = RpcResponse.decode(msg.content);
        if (!res.result) return reject(res.body);
        if (opts && opts.withRawdata) {
          return resolve({ body: res.body, raw: msg.content });
        }
        return resolve(res.body);
      });
    });
  }
}
