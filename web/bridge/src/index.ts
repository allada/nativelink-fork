import protobuf from 'protobufjs/minimal';
import { createClient, commandOptions } from 'redis';
import pg from 'pg';

import processLifecycleEvent from './transforms/PublishLifecycleEventRequest';
import processBuildToolEventStream from './transforms/PublishBuildToolEventStreamRequest';

protobuf.util.toJSONOptions = Object.assign({}, protobuf.util.toJSONOptions, {
  longs: Number,
});

type RedisClientType = ReturnType<typeof createClient>;

async function initRedisClients(): Promise<RedisClientType> {
  try {
    const redisClient  = createClient({
      url: process.env.REDIS_URL,
    });

    redisClient.on('error', (err) => {
      console.error('Redis Client Error:', err);
      throw new Error('Failed to connect to Redis.');
    });

    await redisClient.connect();

    console.log('Redis clients successfully connected.');

    return redisClient;
  } catch (error) {
    console.error('Error during Redis client initialization:', error);
    throw new Error('Unable to initialize Redis clients. Please check your connection.');
  }
}

async function initPgClient() {
  const pgClient = new pg.Client({
    user: process.env.PG_USER,
    host: process.env.PG_HOST,
    database: process.env.PG_DATABASE,
    password: process.env.PG_PASSWORD,
    port: parseInt(process.env.PG_PORT as string),
  });

  await pgClient.connect();

  console.log('Postgres client successfully connected.');

  return pgClient;
}

async function redisScan(redisClient: RedisClientType, match: string, handler: (data: Buffer | null) => Promise<void>) {
  let cursor = 0;
  do {
    const {cursor: newCursor, keys} = await redisClient.scan(cursor, { MATCH: match });
    cursor = newCursor;

    const dataPromises = keys.map(key => {
      return redisClient
        .get(commandOptions({ returnBuffers: true }), key)
        .then(data => handler(data))
    });
    await Promise.all(dataPromises);
  } while (cursor !== 0);
}

export async function main() {
  let [redisClient, pgClient] = await Promise.all([initRedisClients(), initPgClient()]);

  let pubSubClient = redisClient.duplicate();
  await pubSubClient.connect();

  const ALREADY_RUNNING = { running: false };
  const maybeRunBepProcessor = async () => {
    if (ALREADY_RUNNING.running) {
      return;
    }
    ALREADY_RUNNING.running = true;
    setTimeout(() => {
      Promise.all([
        redisScan(redisClient, 'BuildToolEventStream:*', data => processBuildToolEventStream(data, pgClient)),
        redisScan(redisClient, 'LifecycleEvent:*', data => processLifecycleEvent(data, pgClient)),
      ])
      .finally(() => {
        ALREADY_RUNNING.running = false;
      });
    }, 10);
  };
  pubSubClient.subscribe(String(process.env.REDIS_SUBSCRIBE_CHANNEL), maybeRunBepProcessor, true);
  // setInterval(maybeRunBepProcessor, 1000);
  await Promise.all([
    redisScan(redisClient, 'BuildToolEventStream:*', data => processBuildToolEventStream(data, pgClient)),
    redisScan(redisClient, 'LifecycleEvent:*', data => processLifecycleEvent(data, pgClient)),
  ]);
  await new Promise(resolve => setTimeout(resolve, 1000));
  process.exit(0);
}
