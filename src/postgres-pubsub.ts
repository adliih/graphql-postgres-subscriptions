import { PubSubEngine } from 'graphql-subscriptions';
import { Client, ClientConfig, Notification } from 'pg';
import { EventEmitter } from 'events';

export interface PostgresPubSubOptions extends ClientConfig {
  /**
   * Pre-existing, already-connected PostgreSQL client.
   * When provided, the library uses it as-is and never calls connect() or end() on it.
   * When omitted, a new Client is created from the remaining ClientConfig options.
   */
  client?: Client;

  /**
   * Transform every incoming message payload before it is delivered to subscribers.
   * Defaults to the identity function.
   */
  commonMessageHandler?: <T>(message: T) => unknown;
}

type Listener = (payload: unknown) => void;

function defaultCommonMessageHandler<T>(message: T): T {
  return message;
}

function escapeIdentifier(str: string): string {
  return '"' + str.replace(/"/g, '""') + '"';
}

export class PostgresPubSub extends PubSubEngine {
  private readonly pgClient: Client;
  private readonly ee: EventEmitter;
  private readonly subscriptions: Map<number, [string, Listener]>;
  private readonly channelSubscriberCount: Map<string, number>;
  private readonly commonMessageHandler: <T>(message: T) => unknown;
  private readonly connectPromise: Promise<void>;
  private subIdCounter: number;

  constructor(options: PostgresPubSubOptions = {}) {
    super();

    const { client, commonMessageHandler, ...pgOptions } = options;

    this.pgClient = client ?? new Client(pgOptions);
    this.ee = new EventEmitter();
    this.subscriptions = new Map();
    this.channelSubscriberCount = new Map();
    this.subIdCounter = 0;
    this.commonMessageHandler = commonMessageHandler ?? defaultCommonMessageHandler;

    // Relay pg NOTIFY events to our internal EventEmitter
    this.pgClient.on('notification', (msg: Notification) => {
      let payload: unknown;
      try {
        payload = msg.payload !== undefined ? JSON.parse(msg.payload) : undefined;
      } catch {
        // Payload is not JSON – forward as-is
        payload = msg.payload;
      }
      this.ee.emit(msg.channel, payload);
    });

    if (client) {
      // Caller is responsible for the lifecycle of a provided client
      this.connectPromise = Promise.resolve();
    } else {
      this.connectPromise = this.pgClient.connect().then(() => undefined);
    }
  }

  // ---------------------------------------------------------------------------
  // PubSubEngine abstract methods
  // ---------------------------------------------------------------------------

  async publish(triggerName: string, payload: unknown): Promise<void> {
    await this.connectPromise;
    const serialized = JSON.stringify(payload);
    await this.pgClient.query('SELECT pg_notify($1, $2)', [triggerName, serialized]);
  }

  async subscribe(triggerName: string, onMessage: Listener): Promise<number> {
    await this.connectPromise;

    const callback: Listener = (payload: unknown) => {
      onMessage(this.commonMessageHandler(payload));
    };

    this.ee.on(triggerName, callback);

    const count = (this.channelSubscriberCount.get(triggerName) ?? 0) + 1;
    this.channelSubscriberCount.set(triggerName, count);

    if (count === 1) {
      await this.pgClient.query(`LISTEN ${escapeIdentifier(triggerName)}`);
    }

    this.subIdCounter += 1;
    this.subscriptions.set(this.subIdCounter, [triggerName, callback]);
    return this.subIdCounter;
  }

  unsubscribe(subId: number): void {
    const sub = this.subscriptions.get(subId);
    if (!sub) return;

    const [triggerName, callback] = sub;
    this.subscriptions.delete(subId);
    this.ee.removeListener(triggerName, callback);

    const count = (this.channelSubscriberCount.get(triggerName) ?? 1) - 1;
    if (count <= 0) {
      this.channelSubscriberCount.delete(triggerName);
      this.pgClient
        .query(`UNLISTEN ${escapeIdentifier(triggerName)}`)
        .catch(() => {
          // Ignore UNLISTEN errors (e.g. connection already closed)
        });
    } else {
      this.channelSubscriberCount.set(triggerName, count);
    }
  }

  // ---------------------------------------------------------------------------
  // asyncIterableIterator is provided by PubSubEngine base class and uses
  // our subscribe / unsubscribe implementations above.
  //
  // asyncIterator is kept as a convenience alias for users migrating from
  // graphql-subscriptions v1/v2 where this was the canonical method name.
  // ---------------------------------------------------------------------------

  asyncIterator<T>(triggers: string | readonly string[]): AsyncIterableIterator<T> {
    return this.asyncIterableIterator<T>(triggers);
  }

  // ---------------------------------------------------------------------------
  // Lifecycle
  // ---------------------------------------------------------------------------

  /**
   * UNLISTEN all channels and end the underlying pg connection.
   * Only call this when the library owns the client (i.e. no `client` option
   * was passed to the constructor).
   */
  async close(): Promise<void> {
    await this.connectPromise;
    await this.pgClient.query('UNLISTEN *');
    await this.pgClient.end();
  }
}
