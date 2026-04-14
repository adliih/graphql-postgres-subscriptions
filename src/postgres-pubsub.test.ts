// Adapted from https://github.com/apollographql/graphql-subscriptions/blob/master/src/test/tests.ts
import { Client } from 'pg';
import { PostgresPubSub } from './postgres-pubsub';

/** Matches docker-compose.yml (override with DATABASE_URL). */
const TEST_PG_URL =
  process.env.DATABASE_URL ?? 'postgresql://postgres:postgres@127.0.0.1:5432/postgres';

let client: Client;

function pgConnectionHint(err: unknown): string {
  const code =
    err && typeof err === "object" && "code" in err
      ? String((err as { code?: string }).code)
      : "";
  if (code === "ECONNREFUSED") {
    return (
      "\n\nPostgreSQL is not reachable. Integration tests need a running server.\n" +
      "  docker compose up -d\n" +
      "Or set DATABASE_URL to your instance.\n"
    );
  }
  return "";
}

describe('PostgresPubSub', () => {
  beforeEach(async () => {
    client = new Client({ connectionString: TEST_PG_URL });
    try {
      await client.connect();
    } catch (err) {
      throw new Error(String(err) + pgConnectionHint(err));
    }
  });

  afterEach(async () => {
    await client.end();
  });

  // ---------------------------------------------------------------------------
  // subscribe / publish / unsubscribe
  // ---------------------------------------------------------------------------

  test('can subscribe when instantiated without a client', (done) => {
    const ps = new PostgresPubSub({ connectionString: TEST_PG_URL });
    ps.subscribe('a', (payload) => {
      expect(payload).toEqual('test');
      void ps.close().then(done);
    }).then(() => ps.publish('a', 'test'));
  });

  test('can subscribe and is called when events happen', (done) => {
    const ps = new PostgresPubSub({ client });
    ps.subscribe('a', (payload) => {
      expect(payload).toEqual('test');
      done();
    }).then(() => ps.publish('a', 'test'));
  });

  test('can subscribe when instantiated with connection string', (done) => {
    const ps = new PostgresPubSub({
      connectionString: TEST_PG_URL,
    });
    ps.subscribe('a', (payload) => {
      expect(payload).toEqual('test');
      void ps.close().then(done);
    }).then(() => ps.publish('a', 'test'));
  });

  test('publish sends a pg NOTIFY with JSON-serialized payload', (done) => {
    const ps = new PostgresPubSub({ client });
    // Listen on the underlying pg client directly to verify the wire format.
    client.on('notification', ({ payload }) => {
      // pg delivers the raw JSON string; our library encodes with JSON.stringify.
      expect(JSON.parse(payload ?? 'null')).toEqual('test');
      done();
    });
    ps.subscribe('a', () => {
      // subscribe triggers LISTEN so that pg actually delivers the notification
    }).then(() => ps.publish('a', 'test'));
  });

  test('can unsubscribe', (done) => {
    const ps = new PostgresPubSub({ client });
    ps.subscribe('a', () => {
      throw new Error('should not be called after unsubscribe');
    }).then((subId) => {
      ps.unsubscribe(subId);
      // publish completes without error even when nobody is listening
      ps.publish('a', 'test').then(done);
    });
  });

  test('publish rejects when payload exceeds the PostgreSQL 8000-byte limit', async () => {
    const ps = new PostgresPubSub({ client });
    await expect(ps.publish('a', 'a'.repeat(9000))).rejects.toThrow();
  });

  test('transforms messages using commonMessageHandler in subscribe', (done) => {
    const commonMessageHandler = (msg: unknown) => ({ transformed: msg });
    const ps = new PostgresPubSub({ client, commonMessageHandler });
    ps.subscribe('transform', (payload) => {
      expect(payload).toEqual({ transformed: { test: true } });
      done();
    }).then(() => ps.publish('transform', { test: true }));
  });

  // ---------------------------------------------------------------------------
  // asyncIterableIterator (v3 API) + asyncIterator (v1/v2 compat alias)
  // ---------------------------------------------------------------------------

  test('asyncIterableIterator exposes a valid AsyncIterableIterator', () => {
    const ps = new PostgresPubSub({ client });
    const iterator = ps.asyncIterableIterator('test');
    expect(iterator).toBeDefined();
    expect(typeof iterator[Symbol.asyncIterator]).toBe('function');
    expect(typeof iterator.next).toBe('function');
    void iterator.return?.();
  });

  test('asyncIterator alias exposes a valid AsyncIterableIterator', () => {
    const ps = new PostgresPubSub({ client });
    const iterator = ps.asyncIterator('test');
    expect(iterator).toBeDefined();
    expect(typeof iterator[Symbol.asyncIterator]).toBe('function');
    expect(typeof iterator.next).toBe('function');
    void iterator.return?.();
  });

  test('asyncIterableIterator yields published values', (done) => {
    const ps = new PostgresPubSub({ client });
    const iterator = ps.asyncIterableIterator<{ test: boolean }>('test');

    iterator.next().then((result) => {
      expect(result.value).toEqual({ test: true });
      expect(result.done).toBe(false);
      done();
    });

    // Give the iterator time to subscribe before publishing
    setTimeout(() => ps.publish('test', { test: true }), 10);
  });

  test('asyncIterableIterator does not yield events for other channels', (done) => {
    const ps = new PostgresPubSub({ client });
    const iterator = ps.asyncIterableIterator<{ test: boolean }>('test');
    const spy = jest.fn();

    iterator.next().then(spy);
    // Publish to a different channel — spy must NOT be called synchronously
    ps.publish('test2', { test: true }).then(() => {
      expect(spy).not.toHaveBeenCalled();
      void iterator.return?.();
      done();
    });
  });

  test('asyncIterableIterator subscribes to multiple channels', (done) => {
    const ps = new PostgresPubSub({ client });
    const iterator = ps.asyncIterableIterator<{ test: boolean }>(['test', 'test2']);

    iterator.next().then((result) => {
      expect(result.value).toEqual({ test: true });
      expect(result.done).toBe(false);
      done();
    });

    setTimeout(() => ps.publish('test2', { test: true }), 10);
  });

  test('asyncIterableIterator transforms messages using commonMessageHandler', (done) => {
    const commonMessageHandler = (msg: unknown) => ({ transformed: msg });
    const ps = new PostgresPubSub({ client, commonMessageHandler });
    const iterator = ps.asyncIterableIterator<{ transformed: unknown }>('test');

    iterator.next().then((result) => {
      expect(result.value).toEqual({ transformed: { test: true } });
      expect(result.done).toBe(false);
      done();
    });

    setTimeout(() => ps.publish('test', { test: true }), 10);
  });

  test('asyncIterableIterator returns done after return() is called', async () => {
    const ps = new PostgresPubSub({ client });
    const iterator = ps.asyncIterableIterator<{ test: boolean }>("test");

    const nextPromise = iterator.next();
    // PubSubAsyncIterableIterator only queues pullValue() after subscribeAll()
    // resolves. If return() runs before that, emptyQueue() clears an empty
    // pullQueue and the later pullValue() never gets a done signal (hang).
    await new Promise((r) => setTimeout(r, 50));

    await iterator.return?.();

    const result = await nextPromise;
    expect(result.done).toBe(true);
  });
});
