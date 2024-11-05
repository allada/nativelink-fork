import pg from 'pg';
import { google, blaze, build_event_stream }  from '../protos';
import { removeEmptyFields } from '../utils';

const PublishBuildToolEventStreamRequest = google.devtools.build.v1.PublishBuildToolEventStreamRequest;

const BUILDS_TABLE = {
  name: 'build_data',
  id: 'id',
  fields: {
    id: String,
    start_time: Number,
    last_updated: Number,
    finished: Boolean,

    remote_execution: Boolean,
    remote_cache: Boolean,

    cache_misses: Number,
    actions_executed: Number,
    actions_created: Number,
  },
  updateTransforms: {
    last_updated: () => 'last_updated=GREATEST(build_data.last_updated, excluded.last_updated)',
    start_time: () => 'start_time=LEAST(build_data.start_time, excluded.start_time)',
    finished: () => "finished=GREATEST(build_data.finished, excluded.finished)",
    cache_misses: () => 'cache_misses=GREATEST(build_data.cache_misses, excluded.cache_misses)',
    actions_executed: () => 'actions_executed=GREATEST(build_data.actions_executed, excluded.actions_executed)',
    actions_created: () => 'actions_created=GREATEST(build_data.actions_created, excluded.actions_created)',
  }
}

async function transform(request: InstanceType<typeof google.devtools.build.v1.PublishBuildToolEventStreamRequest>, pgClient: pg.Client) {
  let outRequest: any;

  if (request.orderedBuildEvent && request.orderedBuildEvent.event && request.orderedBuildEvent.event.bazelEvent) {
    switch (request.orderedBuildEvent.event.bazelEvent.type_url) {
      case "type.googleapis.com/build_event_stream.BuildEvent":
        outRequest = request.toJSON();
        const bazelEvent = build_event_stream.BuildEvent.decode(request.orderedBuildEvent.event.bazelEvent.value);
        const buildId = request.orderedBuildEvent.streamId.buildId;
        switch (bazelEvent.payload) {
          case 'unstructuredCommandLine':
            processUnstructuredCommandLine(pgClient, buildId, bazelEvent.unstructuredCommandLine, request);
            break;
          case 'buildMetrics':
            processBuildMetrics(pgClient, buildId, bazelEvent.buildMetrics, request);
            break;
        }
        if (bazelEvent.lastMessage) {
          updateInvocation(pgClient, {
            id: buildId,
            last_updated: request.orderedBuildEvent.event.eventTime.seconds.toNumber(),
            start_time: request.orderedBuildEvent.event.eventTime.seconds.toNumber(),
            finished: true,
          });
        }

        outRequest.orderedBuildEvent.event.bazelEvent = bazelEvent.toJSON();
        break;
      default:
        console.log('Unknown build event type:', request.orderedBuildEvent.event.bazelEvent.type_url);
        return;
    }
  }

  removeEmptyFields(outRequest);

  if (!outRequest) {
    return;
  }
  // console.log(outRequest);

  await pgClient.query('INSERT INTO "PublishBuildToolEventStreamRequest" (data) VALUES ($1)', [outRequest]);
}

async function processUnstructuredCommandLine(pgClient: pg.Client, buildId: string, unstructuredCommandLine: any, request: InstanceType<typeof google.devtools.build.v1.PublishBuildToolEventStreamRequest>) {
  let updates = {
    id: buildId,
    last_updated: request.orderedBuildEvent.event.eventTime.seconds.toNumber(),
    start_time: request.orderedBuildEvent.event.eventTime.seconds.toNumber(),
  } as Partial<InvocationData> & { id: string, last_updated: number, start_time: number };
  for (const arg of unstructuredCommandLine.args) {
    if (arg.startsWith('--remote_executor=')) {
      updates.remote_execution = true;
    }
    if (arg.startsWith('--remote_cache=')) {
      updates.remote_cache = true;
    }
  }
  updateInvocation(pgClient, updates);
}

async function processBuildMetrics(pgClient: pg.Client, buildId: string, buildMetrics: any, request: InstanceType<typeof google.devtools.build.v1.PublishBuildToolEventStreamRequest>) {
  let updates = {
    id: buildId,
    last_updated: request.orderedBuildEvent.event.eventTime.seconds.toNumber(),
    start_time: request.orderedBuildEvent.event.eventTime.seconds.toNumber(),
    cache_misses: 0,
    actions_executed: buildMetrics.actionSummary.actionsExecuted.toNumber(),
    actions_created: buildMetrics.actionSummary.actionsCreatedNotIncludingAspects.toNumber(),
  } as Partial<InvocationData> & { id: string, last_updated: number, start_time: number };

  if (buildMetrics.actionSummary.actionCacheStatistics) {
    updates.cache_misses = buildMetrics.actionSummary.actionCacheStatistics.misses;
    for (const details of buildMetrics.actionSummary.actionCacheStatistics.missDetails) {
      console.log(details);
      if (details.reason === blaze.ActionCacheStatistics.MissReason.UNCONDITIONAL_EXECUTION) {
        updates.cache_misses = Math.max(updates.cache_misses! - details.count, 0);
      }
    }
  }
  updateInvocation(pgClient, updates);
}

type RecordId = string;

type InvocationData = {[K in keyof typeof BUILDS_TABLE.fields]?: ReturnType<typeof BUILDS_TABLE.fields[K]>};
type UpdateData = {
  onUpdates: Array<() => Promise<void>>,
  data: InvocationData,
};

const QUEUED_INVOCATION_UPDATES = new Map() as Map<pg.Client, Record<RecordId, UpdateData>>;

function updateInvocation(pgClient: pg.Client, data: { id: RecordId, last_updated: number, start_time: number } & Partial<InvocationData>, onUpdate?: () => Promise<void>) {
  console.assert(data.id !== undefined);
  let record = QUEUED_INVOCATION_UPDATES.get(pgClient)!;
  if (!record) {
    record = {};
    QUEUED_INVOCATION_UPDATES.set(pgClient, record);
  }

  if (!record[data.id]) {
    record[data.id] = {
      onUpdates: [],
      data: {},
    };
  }

  for (const key of Object.keys(data)) {
    record[data.id].data[key] = data[key];
  }

  if (onUpdate) {
    record[data.id].onUpdates.push(onUpdate);
  }
}

async function processQueuedInvocations() {
  if (QUEUED_INVOCATION_UPDATES.size === 0) {
    return;
  }

  const updatePromises: Promise<any>[] = [];
  const processingIds: Array<[Record<string, UpdateData>, string]> = [];
  for (const [pgClient, data] of QUEUED_INVOCATION_UPDATES.entries()) {
    for (let [id, updates] of Object.entries(data)) {
      console.assert(updates.data[BUILDS_TABLE.id] === id);

      const updatedKeys = Object.keys(updates.data).filter(k => k !== BUILDS_TABLE.id);
      const allKeys = Object.keys(BUILDS_TABLE.fields);
      const allValues = allKeys.map(k => updates.data[k]);

      let i = 0;
      const query = [
        'INSERT INTO',
        BUILDS_TABLE.name,
        `(${allKeys.join(',')})`,
        'VALUES',
        `(${allValues.map(v => v === undefined ? 'DEFAULT' : `$${++i}`).join(',')})`,
        'ON CONFLICT',
        `(${BUILDS_TABLE.id})`,
        'DO UPDATE SET',
        `${updatedKeys.map(key => {
          if (BUILDS_TABLE.updateTransforms[key]) {
            return BUILDS_TABLE.updateTransforms[key]();
          }
          return `${key}=excluded.${key}`;
        }).join(',')}`,
      ].join(' ');
      console.log(query, allValues.filter(v => v !== undefined));

      processingIds.push([data, id]);

      // Upsert our data.
      updatePromises.push(
        pgClient
          .query(query, allValues.filter(v => v !== undefined))
          .finally(() => {
            return Promise.all(updates.onUpdates.map(f => f()));
          }),
      );
      if (updatePromises.length >= 100) {
        processingIds.forEach(([data, id]) => delete data[id]);
        await Promise.all(updatePromises);
        updatePromises.length = 0;
      }
    }
  }

  processingIds.forEach(([data, id]) => delete data[id]);
  // Run any remaining updates.
  await Promise.all(updatePromises);

  // Now clean up any postgres clients that are no longer in use.
  outerLoop: for (const [pgClient, data] of QUEUED_INVOCATION_UPDATES.entries()) {
    for (const key in data) {
      if (Object.hasOwn(data, key)) {
        continue outerLoop;
      }
    }
    QUEUED_INVOCATION_UPDATES.delete(pgClient);
  }
}

function doProcessQueuedInvocations() {
  processQueuedInvocations().finally(() => setTimeout(doProcessQueuedInvocations, 250));
}

doProcessQueuedInvocations();

export default async function publishBuildToolEventStream(data: Buffer | null, pgClient: pg.Client) {
  if (!data) {
    return;
  }

  return transform(PublishBuildToolEventStreamRequest.decode(new Uint8Array(data)), pgClient);
}
