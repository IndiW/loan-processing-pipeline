/**
 * End-to-end integration test covering all three ingestion channels.
 * Uses TestWorkflowEnvironment.createLocal() with search attributes registered,
 * since upsertSearchAttributes() in the workflow requires SA registration.
 *
 * Scenarios:
 *   - PERSONAL via portal  → auto-approve → FUNDED
 *   - AUTO via broker email → over $60k limit → REFER → human APPROVE → FUNDED
 *   - DEBT_CONSOLIDATION via aggregator batch → auto-approve (via personal rules) → FUNDED
 *   - Aggregator dedup via workflowId idempotency
 *
 * Workers are shared across tests via a delegation pattern (same as workflow.test.ts)
 * to avoid the per-test 7×Worker startup cost.
 */
import { TestWorkflowEnvironment } from '@temporalio/testing';
import { Worker, Runtime, DefaultLogger } from '@temporalio/worker';
import { loanApplicationWorkflow, getState, submitHumanDecision } from './workflows/loanApplication.workflow';
import { TASK_QUEUES, SEARCH_ATTRIBUTE_KEYS } from './shared/searchAttributes';
import { fromPortal, fromBrokerEmail, fromAggregatorRow } from './client/ingestion';
import type { LoanApplication, DocumentRef } from './shared/types';
import { ApplicationFailure } from '@temporalio/common';

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));
const WF = require.resolve('./workflows');

// ── delegation pattern ─────────────────────────────────────────────────────
// All integration tests use the same deterministic mocks; one shared mock object
// is set once and never swapped, so the static delegates approach works here.

const mocks = {
  validateApplication: jest.fn(async (app: LoanApplication) => {
    const required: Record<string, string[]> = {
      PERSONAL: ['ID', 'PROOF_OF_INCOME'],
      AUTO: ['ID', 'PROOF_OF_INCOME', 'VEHICLE_TITLE'],
      DEBT_CONSOLIDATION: ['ID', 'PROOF_OF_INCOME', 'BANK_STATEMENT'],
    };
    const have = new Set(app.documents.map((d: DocumentRef) => d.type));
    const missing = (required[app.product] ?? []).filter((t: string) => !have.has(t as DocumentRef['type']));
    if (app.requestedAmount <= 0) {
      throw ApplicationFailure.create({ message: 'requestedAmount must be positive', type: 'ValidationError', nonRetryable: true });
    }
    return { ok: missing.length === 0, missingDocuments: missing, errors: [] };
  }),
  pullCreditReport: jest.fn(async () => ({
    score: 750, openTradelines: 5, delinquencies: 0, bureau: 'Equifax', pulledAt: new Date().toISOString(),
  })),
  verifyIdentity: jest.fn(async () => ({ verified: true, matchScore: 95, provider: 'Onfido' })),
  screenForFraud: jest.fn(async () => ({ riskScore: 5, signals: [], provider: 'Sift' })),
  disburseFunds: jest.fn(async (input: { applicationId: string }) => ({
    disbursementId: `disb_${input.applicationId}`,
  })),
  recordAudit: jest.fn(async () => {}),
  notifyApplicant: jest.fn(async () => {}),
  createReviewTask: jest.fn(async () => {}),
  closeReviewTask: jest.fn(async () => {}),
  emitOutcome: jest.fn(async () => {}),
};

describe('End-to-end: three ingestion channels', () => {
  let env: TestWorkflowEnvironment;
  let workerInstances: Worker[];
  let workerRuns: Promise<void>[];

  beforeAll(async () => {
    Runtime.install({ logger: new DefaultLogger('WARN') });
    env = await TestWorkflowEnvironment.createLocal({
      server: { searchAttributes: SEARCH_ATTRIBUTE_KEYS },
    });

    workerInstances = await Promise.all(
      Object.values(TASK_QUEUES).map((q) =>
        Worker.create({ connection: env.nativeConnection, namespace: 'default', taskQueue: q, workflowsPath: WF, activities: mocks }),
      ),
    );
    workerRuns = workerInstances.map((w) => w.run());
  }, 60_000);

  afterAll(async () => {
    workerInstances?.forEach((w) => w.shutdown());
    await Promise.allSettled(workerRuns ?? []);
    await env?.teardown();
  }, 120_000);

  it('funds a PERSONAL application from the portal (happy path)', async () => {
    const app = fromPortal({
      product: 'PERSONAL',
      requestedAmount: 15_000,
      applicant: { fullName: 'Ada Lovelace', email: 'ada@example.com', dateOfBirth: '1985-12-10', annualIncome: 90_000 },
      documents: [
        { type: 'ID', uri: 's3://docs/ada-id', receivedAt: new Date().toISOString() },
        { type: 'PROOF_OF_INCOME', uri: 's3://docs/ada-income', receivedAt: new Date().toISOString() },
      ],
    });

    const handle = await env.client.workflow.start(loanApplicationWorkflow, {
      args: [app], taskQueue: TASK_QUEUES.ORCHESTRATOR, workflowId: app.applicationId,
    });

    const result = await handle.result();
    expect(result.status).toBe('FUNDED');
    expect(result.reasons).toContain('auto_approved');
  });

  it('refers AUTO broker-email application then funds after human approval', async () => {
    // $80k AUTO → over $60k auto limit → amount_over_auto_limit → REFER
    const app = fromBrokerEmail({
      brokerId: 'b1', messageId: 'm1', product: 'AUTO', requestedAmount: 80_000,
      applicant: { fullName: 'Alan Turing', email: 'alan@example.com', dateOfBirth: '1980-06-23', annualIncome: 120_000 },
      documents: [
        { type: 'ID', uri: 's3://docs/alan-id', receivedAt: new Date().toISOString() },
        { type: 'PROOF_OF_INCOME', uri: 's3://docs/alan-income', receivedAt: new Date().toISOString() },
        { type: 'VEHICLE_TITLE', uri: 's3://docs/alan-title', receivedAt: new Date().toISOString() },
      ],
    });

    const handle = await env.client.workflow.start(loanApplicationWorkflow, {
      args: [app], taskQueue: TASK_QUEUES.ORCHESTRATOR, workflowId: app.applicationId,
    });

    // Wait for PENDING_HUMAN_REVIEW
    for (let i = 0; i < 100; i++) {
      await sleep(100);
      const st = await handle.query(getState);
      if (st.status === 'PENDING_HUMAN_REVIEW') break;
    }

    const stateBefore = await handle.query(getState);
    expect(stateBefore.status).toBe('PENDING_HUMAN_REVIEW');
    expect(stateBefore.reasons).toContain('amount_over_auto_limit');

    await handle.executeUpdate(submitHumanDecision, {
      args: [{ type: 'APPROVE', reviewerId: 'rev-7', notes: 'approved after review', approvedAmount: 80_000, interestRate: 0.08 }],
    });

    const result = await handle.result();
    expect(result.status).toBe('FUNDED');
    expect(result.reasons).toContain('human_approved');
  });

  it('funds a DEBT_CONSOLIDATION application from the aggregator batch', async () => {
    const app = fromAggregatorRow({
      aggregator: 'lendingtree', externalId: '99887', product: 'DEBT_CONSOLIDATION', requestedAmount: 25_000,
      applicant: { fullName: 'Grace Hopper', email: 'grace@example.com', dateOfBirth: '1975-12-09', annualIncome: 105_000 },
      documents: [
        { type: 'ID', uri: 's3://docs/grace-id', receivedAt: new Date().toISOString() },
        { type: 'PROOF_OF_INCOME', uri: 's3://docs/grace-income', receivedAt: new Date().toISOString() },
        { type: 'BANK_STATEMENT', uri: 's3://docs/grace-bank', receivedAt: new Date().toISOString() },
      ],
    });

    const handle = await env.client.workflow.start(loanApplicationWorkflow, {
      args: [app], taskQueue: TASK_QUEUES.ORCHESTRATOR, workflowId: app.applicationId,
    });

    const result = await handle.result();
    expect(result.status).toBe('FUNDED');
  });

  it('deduplicates aggregator re-deliveries via workflowId (idempotency)', async () => {
    const row = {
      aggregator: 'lending-tree', externalId: 'dedup-42', product: 'PERSONAL' as const,
      requestedAmount: 10_000,
      applicant: { fullName: 'Bob Smith', email: 'bob@example.com', dateOfBirth: '1990-01-01', annualIncome: 60_000 },
      documents: [
        { type: 'ID' as const, uri: 's3://id', receivedAt: new Date().toISOString() },
        { type: 'PROOF_OF_INCOME' as const, uri: 's3://income', receivedAt: new Date().toISOString() },
      ],
    };

    const app1 = fromAggregatorRow(row);
    const app2 = fromAggregatorRow(row); // same row → same applicationId

    expect(app1.applicationId).toBe(app2.applicationId);

    const h1 = await env.client.workflow.start(loanApplicationWorkflow, {
      args: [app1], taskQueue: TASK_QUEUES.ORCHESTRATOR, workflowId: app1.applicationId,
      workflowIdConflictPolicy: 'USE_EXISTING',
    });
    const h2 = await env.client.workflow.start(loanApplicationWorkflow, {
      args: [app2], taskQueue: TASK_QUEUES.ORCHESTRATOR, workflowId: app2.applicationId,
      workflowIdConflictPolicy: 'USE_EXISTING',
    });

    expect(h1.workflowId).toBe(h2.workflowId);
    const r1 = await h1.result();
    expect(r1.status).toBe('FUNDED');
  });
});
