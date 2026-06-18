/**
 * Workers poll task queues and execute workflow/activity tasks. They are
 * stateless and scale horizontally. We run several logical worker roles, each
 * on its own task queue, so that:
 *
 *   - provider workers can pin a global rate limit (maxTaskQueueActivitiesPerSecond)
 *   - product workers deploy/version independently
 *   - a noisy product or a throttled provider can't starve the others
 *
 * For local dev this file spins them all up in one process; in production each
 * role is its own deployment.
 */
import { NativeConnection, Worker } from '@temporalio/worker';
import * as activities from './activities';
import { TASK_QUEUES } from './shared/searchAttributes';

const WORKFLOWS_PATH = require.resolve('./workflows');

async function run() {
  const address = process.env.TEMPORAL_ADDRESS ?? 'localhost:7233';
  const namespace = process.env.TEMPORAL_NAMESPACE ?? 'default';
  const connection = await NativeConnection.connect({ address });

  // Orchestrator worker: runs the main workflow + generic activities.
  const orchestrator = await Worker.create({
    connection,
    namespace,
    taskQueue: TASK_QUEUES.ORCHESTRATOR,
    workflowsPath: WORKFLOWS_PATH,
    activities,
  });

  // Product workers: only run their underwriting child workflow.
  const personal = await Worker.create({
    connection,
    namespace,
    taskQueue: TASK_QUEUES.UNDERWRITING_PERSONAL,
    workflowsPath: WORKFLOWS_PATH,
  });
  const auto = await Worker.create({
    connection,
    namespace,
    taskQueue: TASK_QUEUES.UNDERWRITING_AUTO,
    workflowsPath: WORKFLOWS_PATH,
  });
  const debt = await Worker.create({
    connection,
    namespace,
    taskQueue: TASK_QUEUES.UNDERWRITING_DEBT,
    workflowsPath: WORKFLOWS_PATH,
  });

  // Provider workers: enforce a per-provider rate limit at the task-queue level.
  // `maxTaskQueueActivitiesPerSecond` is a GLOBAL limit across all workers on
  // that queue — exactly what you want to respect a vendor's contractual RPS.
  const creditWorker = await Worker.create({
    connection,
    namespace,
    taskQueue: TASK_QUEUES.PROVIDER_CREDIT,
    activities,
    maxTaskQueueActivitiesPerSecond: 20, // bureau allows 20 rps
    maxConcurrentActivityTaskExecutions: 50,
  });
  const identityWorker = await Worker.create({
    connection,
    namespace,
    taskQueue: TASK_QUEUES.PROVIDER_IDENTITY,
    activities,
    maxTaskQueueActivitiesPerSecond: 10,
  });
  const fraudWorker = await Worker.create({
    connection,
    namespace,
    taskQueue: TASK_QUEUES.PROVIDER_FRAUD,
    activities,
    maxTaskQueueActivitiesPerSecond: 30,
  });

  console.log('Workers started. Polling task queues...');
  await Promise.all([
    orchestrator.run(),
    personal.run(),
    auto.run(),
    debt.run(),
    creditWorker.run(),
    identityWorker.run(),
    fraudWorker.run(),
  ]);
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
