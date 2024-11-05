import pg from 'pg';
import { google }  from '../protos';

const PublishLifecycleEventRequest = google.devtools.build.v1.PublishLifecycleEventRequest;

async function transform(request: typeof PublishLifecycleEventRequest, pgClient: pg.Client) {
  // console.log('buildId:', request.buildEvent.streamId.buildId);
  // console.log('build:', '???');
  // console.log('time:', '???');
  // console.log('cache_ratio:', '???');
  // console.log('start_time:', '???');
  // console.log('remote_execution:', '???');
  // console.log('status:', '???');
  // console.log();

  let outRequest = request.toJSON();

  await pgClient.query('INSERT INTO "PublishLifecycleEventRequest" (data) VALUES ($1)', [outRequest]);
}

export default async function processLifecycleEvent(data: Buffer | null, pgClient: pg.Client) {
  if (!data) {
    return;
  }

  return transform(PublishLifecycleEventRequest.decode(new Uint8Array(data)), pgClient);
}
