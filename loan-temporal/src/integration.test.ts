/**
 * Spins up an ephemeral Temporal dev server in-process, runs all workers, and
 * drives one application of each product to a terminal state — including a
 * human-in-the-loop APPROVE on the referred one.
 */
import { TestWorkflowEnvironment } from '@temporalio/testing';
import { Worker } from '@temporalio/worker';
import * as activities from './activities';
import { TASK_QUEUES, SEARCH_ATTRIBUTE_KEYS } from './shared/searchAttributes';
import { loanApplicationWorkflow, getState, submitHumanDecision } from './workflows/loanApplication.workflow';
import { fromPortal, fromBrokerEmail } from './client/ingestion';

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const WF = require.resolve('./workflows');

async function main() {
  const env = await TestWorkflowEnvironment.createLocal({
    server: { searchAttributes: SEARCH_ATTRIBUTE_KEYS },
  });
  try {
    const { client, nativeConnection, namespace } = env;

    const queues = [
      TASK_QUEUES.ORCHESTRATOR,
      TASK_QUEUES.UNDERWRITING_PERSONAL,
      TASK_QUEUES.UNDERWRITING_AUTO,
      TASK_QUEUES.UNDERWRITING_DEBT,
      TASK_QUEUES.PROVIDER_CREDIT,
      TASK_QUEUES.PROVIDER_IDENTITY,
      TASK_QUEUES.PROVIDER_FRAUD,
    ];
    const workers = await Promise.all(
      queues.map((taskQueue) =>
        Worker.create({ connection: nativeConnection, namespace, taskQueue, workflowsPath: WF, activities }),
      ),
    );
    const runs = workers.map((w) => w.run());

    // happy path: should auto-approve & fund
    const personal = fromPortal({
      product: 'PERSONAL',
      requestedAmount: 12000,
      applicant: { fullName: 'Ada', email: 'ada@x.com', dateOfBirth: '1985-01-01', annualIncome: 90000 },
      documents: [
        { type: 'ID', uri: 's3://id', receivedAt: new Date().toISOString() },
        { type: 'PROOF_OF_INCOME', uri: 's3://inc', receivedAt: new Date().toISOString() },
      ],
    });
    // refer path: amount over auto limit -> human review
    const auto = fromBrokerEmail({
      brokerId: 'b1', messageId: 'm1', product: 'AUTO', requestedAmount: 80000,
      applicant: { fullName: 'Alan', email: 'alan@x.com', dateOfBirth: '1980-01-01', annualIncome: 150000 },
      documents: [
        { type: 'ID', uri: 's3://id', receivedAt: new Date().toISOString() },
        { type: 'PROOF_OF_INCOME', uri: 's3://inc', receivedAt: new Date().toISOString() },
        { type: 'VEHICLE_TITLE', uri: 's3://title', receivedAt: new Date().toISOString() },
      ],
    });

    const hPersonal = await client.workflow.start(loanApplicationWorkflow, {
      args: [personal], taskQueue: TASK_QUEUES.ORCHESTRATOR, workflowId: personal.applicationId,
    });
    const hAuto = await client.workflow.start(loanApplicationWorkflow, {
      args: [auto], taskQueue: TASK_QUEUES.ORCHESTRATOR, workflowId: auto.applicationId,
    });

    // wait for the auto app to reach human review, then approve it
    for (let i = 0; i < 50; i++) {
      await sleep(300);
      const st = await hAuto.query(getState);
      if (st.status === 'PENDING_HUMAN_REVIEW') {
        console.log('[test] auto app awaiting human review:', st.reasons);
        await hAuto.executeUpdate(submitHumanDecision, {
          args: [{ type: 'APPROVE', reviewerId: 'rev-1', notes: 'ok', approvedAmount: 80000, interestRate: 0.08 }],
        });
        console.log('[test] human APPROVE submitted');
        break;
      }
    }

    const personalOut = await hPersonal.result();
    const autoOut = await hAuto.result();
    console.log('[test] PERSONAL outcome:', personalOut.status, personalOut.reasons);
    console.log('[test] AUTO outcome:   ', autoOut.status, autoOut.reasons);

    workers.forEach((w) => w.shutdown());
    await Promise.allSettled(runs);

    const ok = ['FUNDED', 'DECLINED'].includes(personalOut.status) && autoOut.status === 'FUNDED';
    console.log(ok ? '\n✅ end-to-end run succeeded' : '\n❌ unexpected outcome');
    process.exitCode = ok ? 0 : 1;
  } finally {
    await env.teardown();
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
