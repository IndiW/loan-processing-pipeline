/**
 * Workflow integration tests.
 * Uses TestWorkflowEnvironment.createLocal() — a real Temporal dev server
 * that supports search attribute registration (required by upsertSearchAttributes).
 *
 * Worker strategy: workers are started once per test file (beforeAll/afterAll)
 * using activity delegates that forward to per-test jest.fn() mocks set in
 * beforeEach. This avoids the 7-worker-per-test startup cost that blows the
 * 60s timeout.
 *
 * Time-dependent tests (doc wait timeout, 48h SLA) pass short durations via
 * WorkflowTimers so they complete in real time without time-skipping.
 */
import { TestWorkflowEnvironment } from '@temporalio/testing';
import { Worker, Runtime, DefaultLogger } from '@temporalio/worker';
import type { WorkflowHandle } from '@temporalio/client';

import {
  loanApplicationWorkflow,
  getState,
  submitHumanDecision,
  submitDocuments,
  withdraw,
} from '../workflows/loanApplication.workflow';
import type { WorkflowTimers } from '../workflows/loanApplication.workflow';
import { TASK_QUEUES, SEARCH_ATTRIBUTE_KEYS } from '../shared/searchAttributes';
import type {
  LoanApplication,
  LoanOutcome,
  AuditEvent,
  HumanDecision,
  DocumentRef,
} from '../shared/types';

// ── types ──────────────────────────────────────────────────────────────────

type Mocks = {
  validateApplication: jest.Mock;
  pullCreditReport: jest.Mock;
  verifyIdentity: jest.Mock;
  screenForFraud: jest.Mock;
  disburseFunds: jest.Mock;
  recordAudit: jest.Mock;
  notifyApplicant: jest.Mock;
  createReviewTask: jest.Mock;
  closeReviewTask: jest.Mock;
  emitOutcome: jest.Mock;
};

// ── helpers ────────────────────────────────────────────────────────────────

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

/**
 * The client wraps an Update validator failure in `WorkflowUpdateFailedError`
 * whose `.message` is just "Workflow Update failed"; the validator's actual
 * `Error('...')` message lives on `.cause.message`. A plain `rejects.toThrow`
 * matches only the wrapper, so we walk the cause chain.
 */
function expectUpdateRejection(p: Promise<unknown>, contains: string): Promise<void> {
  return p.then(
    () => {
      throw new Error(`expected Update to reject with "${contains}" but it resolved`);
    },
    (err: unknown) => {
      const messages: string[] = [];
      let cur: unknown = err;
      for (let i = 0; i < 5 && cur; i++) {
        const e = cur as { message?: string; cause?: unknown };
        if (typeof e.message === 'string') messages.push(e.message);
        cur = e.cause;
      }
      const combined = messages.join(' | ');
      if (!combined.includes(contains)) {
        throw new Error(`expected error chain to contain "${contains}", got: ${combined}`);
      }
    },
  );
}

async function waitForStatus(
  handle: WorkflowHandle<typeof loanApplicationWorkflow>,
  status: string,
  maxAttempts = 200,
): Promise<void> {
  for (let i = 0; i < maxAttempts; i++) {
    await sleep(100);
    const st = await handle.query(getState);
    if (st.status === status) return;
    if (['FUNDED', 'DECLINED', 'WITHDRAWN', 'EXPIRED'].includes(st.status) && st.status !== status) {
      throw new Error(`Workflow reached terminal state ${st.status} before expected ${status}`);
    }
  }
  throw new Error(`Timed out waiting for status ${status}`);
}

// ── fixture apps ───────────────────────────────────────────────────────────

const baseApp: LoanApplication = {
  applicationId: 'PLACEHOLDER',
  channel: 'PORTAL',
  product: 'PERSONAL',
  applicant: { fullName: 'Ada Lovelace', email: 'ada@example.com', dateOfBirth: '1985-12-10', annualIncome: 90_000 },
  requestedAmount: 15_000,
  documents: [
    { type: 'ID', uri: 's3://id', receivedAt: new Date().toISOString() },
    { type: 'PROOF_OF_INCOME', uri: 's3://income', receivedAt: new Date().toISOString() },
  ],
  sourceMetadata: {},
  submittedAt: new Date().toISOString(),
};

const autoApp: LoanApplication = {
  ...baseApp,
  product: 'AUTO',
  requestedAmount: 65_000,
  documents: [
    { type: 'ID', uri: 's3://id', receivedAt: new Date().toISOString() },
    { type: 'PROOF_OF_INCOME', uri: 's3://income', receivedAt: new Date().toISOString() },
    { type: 'VEHICLE_TITLE', uri: 's3://title', receivedAt: new Date().toISOString() },
  ],
};

function app(id: string, overrides: Partial<LoanApplication> = {}): LoanApplication {
  return { ...baseApp, applicationId: id, ...overrides };
}

function autoOverLimit(id: string): LoanApplication {
  return { ...autoApp, applicationId: id };
}

// ── delegation pattern ─────────────────────────────────────────────────────
// Workers are started once and call through to `currentMocks`, which is
// replaced before each test. This avoids the 7x Worker startup cost per test.

let currentMocks: Mocks;

function buildDefaultMocks(): Mocks {
  return {
    validateApplication: jest.fn(async (a: LoanApplication) => {
      const required: Record<string, string[]> = {
        PERSONAL: ['ID', 'PROOF_OF_INCOME'],
        AUTO: ['ID', 'PROOF_OF_INCOME', 'VEHICLE_TITLE'],
        DEBT_CONSOLIDATION: ['ID', 'PROOF_OF_INCOME', 'BANK_STATEMENT'],
      };
      const have = new Set(a.documents.map((d: DocumentRef) => d.type));
      const missing = (required[a.product] ?? []).filter((t: string) => !have.has(t as DocumentRef['type']));
      const errors: string[] = [];
      if (a.requestedAmount <= 0) errors.push('requestedAmount must be positive');
      if (!a.applicant.email.includes('@')) errors.push('invalid email');
      if (errors.length > 0) {
        const { ApplicationFailure } = await import('@temporalio/common');
        throw ApplicationFailure.create({
          message: `Validation failed: ${errors.join('; ')}`,
          type: 'ValidationError',
          nonRetryable: true,
        });
      }
      return { ok: missing.length === 0, missingDocuments: missing, errors: [] };
    }),
    pullCreditReport: jest.fn(async () => ({ score: 750, openTradelines: 5, delinquencies: 0, bureau: 'Equifax', pulledAt: new Date().toISOString() })),
    verifyIdentity: jest.fn(async () => ({ verified: true, matchScore: 95, provider: 'Onfido' })),
    screenForFraud: jest.fn(async () => ({ riskScore: 5, signals: [], provider: 'Sift' })),
    disburseFunds: jest.fn(async (input: { applicationId: string }) => ({ disbursementId: `disb_${input.applicationId}` })),
    recordAudit: jest.fn(async (_: AuditEvent) => {}),
    notifyApplicant: jest.fn(async () => {}),
    createReviewTask: jest.fn(async () => {}),
    closeReviewTask: jest.fn(async () => {}),
    emitOutcome: jest.fn(async (_: LoanOutcome) => {}),
  };
}

const delegates = {
  validateApplication: (a: LoanApplication) => currentMocks.validateApplication(a),
  pullCreditReport: (...args: unknown[]) => currentMocks.pullCreditReport(...args),
  verifyIdentity: (...args: unknown[]) => currentMocks.verifyIdentity(...args),
  screenForFraud: (...args: unknown[]) => currentMocks.screenForFraud(...args),
  disburseFunds: (...args: unknown[]) => currentMocks.disburseFunds(...args),
  recordAudit: (e: AuditEvent) => currentMocks.recordAudit(e),
  notifyApplicant: (...args: unknown[]) => currentMocks.notifyApplicant(...args),
  createReviewTask: (...args: unknown[]) => currentMocks.createReviewTask(...args),
  closeReviewTask: (...args: unknown[]) => currentMocks.closeReviewTask(...args),
  emitOutcome: (o: LoanOutcome) => currentMocks.emitOutcome(o),
};

// ── env / worker lifecycle ─────────────────────────────────────────────────

let env: TestWorkflowEnvironment;
let workerInstances: Worker[];
let workerRuns: Promise<void>[];

const WORKFLOWS_PATH = require.resolve('../workflows');

beforeAll(async () => {
  Runtime.install({ logger: new DefaultLogger('WARN') });
  env = await TestWorkflowEnvironment.createLocal({
    server: { searchAttributes: SEARCH_ATTRIBUTE_KEYS },
  });

  workerInstances = await Promise.all(
    Object.values(TASK_QUEUES).map((taskQueue) =>
      Worker.create({
        connection: env.nativeConnection,
        namespace: 'default',
        taskQueue,
        workflowsPath: WORKFLOWS_PATH,
        activities: delegates,
      }),
    ),
  );
  workerRuns = workerInstances.map((w) => w.run());
}, 60_000);

afterAll(async () => {
  // Shut workers down first so they drain cleanly before the server closes.
  workerInstances?.forEach((w) => w.shutdown());
  await Promise.allSettled(workerRuns ?? []);
  await env?.teardown();
}, 120_000);

beforeEach(() => {
  currentMocks = buildDefaultMocks();
});

async function startWorkflow(
  a: LoanApplication,
  timers?: WorkflowTimers,
): Promise<WorkflowHandle<typeof loanApplicationWorkflow>> {
  return env.client.workflow.start(loanApplicationWorkflow, {
    args: [a, timers ?? {}],
    taskQueue: TASK_QUEUES.ORCHESTRATOR,
    workflowId: a.applicationId,
  });
}

// ── Happy path ─────────────────────────────────────────────────────────────

describe('Happy path — PERSONAL auto-approve → FUNDED', () => {
  it('runs to FUNDED for a clean application', async () => {
    const result = await (await startWorkflow(app('happy-1'))).result();
    expect(result.status).toBe('FUNDED');
    expect(result.disbursementId).toMatch(/^disb_/);
    expect(result.reasons).toContain('auto_approved');
  });

  it('emits outcome when FUNDED (bug 3)', async () => {
    await (await startWorkflow(app('happy-emit-1'))).result();
    expect(currentMocks.emitOutcome).toHaveBeenCalledWith(
      expect.objectContaining({ status: 'FUNDED' }),
    );
  });
});

// ── AWAITING_DOCUMENTS ─────────────────────────────────────────────────────

describe('AWAITING_DOCUMENTS path', () => {
  it('waits for missing docs then funds', async () => {
    const a = app('awaiting-1', {
      documents: [{ type: 'PROOF_OF_INCOME', uri: 's3://income', receivedAt: new Date().toISOString() }],
    });
    const handle = await startWorkflow(a, { docWaitDurationMs: 30_000 });
    await waitForStatus(handle, 'AWAITING_DOCUMENTS');

    await handle.executeUpdate(submitDocuments, {
      args: [{ documents: [{ type: 'ID', uri: 's3://id', receivedAt: new Date().toISOString() }] }],
    });
    const result = await handle.result();
    expect(result.status).toBe('FUNDED');
  });

  it('declines when docs are never supplied within the timeout', async () => {
    const a = app('awaiting-timeout-1', { documents: [] });
    const result = await (await startWorkflow(a, { docWaitDurationMs: 1_500 })).result();
    expect(result.status).toBe('DECLINED');
    expect(result.reasons).toContain('documents_not_supplied');
  });

  it('emits outcome when DECLINED via doc timeout (bug 3)', async () => {
    const a = app('awaiting-emit-1', { documents: [] });
    await (await startWorkflow(a, { docWaitDurationMs: 1_500 })).result();
    expect(currentMocks.emitOutcome).toHaveBeenCalledWith(expect.objectContaining({ status: 'DECLINED' }));
  });

  it('rejects submitDocuments when not in AWAITING_DOCUMENTS (bug 4)', async () => {
    const handle = await startWorkflow(autoOverLimit('awaiting-bad-state-1'));
    await waitForStatus(handle, 'PENDING_HUMAN_REVIEW');

    await expectUpdateRejection(
      handle.executeUpdate(submitDocuments, {
        args: [{ documents: [{ type: 'ID', uri: 's3://id', receivedAt: new Date().toISOString() }] }],
      }),
      'application is not awaiting documents',
    );

    await handle.executeUpdate(submitHumanDecision, {
      args: [{ type: 'APPROVE', reviewerId: 'rev-1', notes: 'cleanup', approvedAmount: 65_000, interestRate: 0.08 }],
    });
    await handle.result();
  });
});

// ── Enrichment partial failure ─────────────────────────────────────────────

describe('Enrichment partial failure → REFER, never silent approve', () => {
  it('refers to human when the fraud provider exhausts retries', async () => {
    // Simulate the provider being permanently down. We throw a non-retryable
    // ApplicationFailure so the activity short-circuits instead of running
    // through all 8 retries (~4 min of backoff) — the workflow's
    // .catch(() => null) still yields a null fraud bundle either way.
    // The application MUST NOT auto-approve without a fraud signal.
    const { ApplicationFailure } = await import('@temporalio/common');
    currentMocks.screenForFraud = jest.fn(async () => {
      throw ApplicationFailure.create({
        message: 'FraudProvider permanently down',
        type: 'ProviderDown',
        nonRetryable: true,
      });
    });

    const handle = await startWorkflow(app('fraud-down-1'));
    await waitForStatus(handle, 'PENDING_HUMAN_REVIEW');

    const st = await handle.query(getState);
    expect(st.reasons).toContain('fraud_screening_unavailable');

    // Clean up so the workflow terminates.
    await handle.executeUpdate(submitHumanDecision, {
      args: [{ type: 'APPROVE', reviewerId: 'rev-1', notes: 'manual fraud check OK', approvedAmount: 15_000, interestRate: 0.069 }],
    });
    const result = await handle.result();
    expect(result.status).toBe('FUNDED');
  }, 60_000);
});

// ── DECLINED via underwriting ──────────────────────────────────────────────

describe('DECLINED path — underwriting hard decline', () => {
  it('declines when credit score is below threshold', async () => {
    currentMocks.pullCreditReport = jest.fn(async () => ({
      score: 580, openTradelines: 3, delinquencies: 0, bureau: 'Equifax', pulledAt: new Date().toISOString(),
    }));
    const result = await (await startWorkflow(app('declined-credit-1'))).result();
    expect(result.status).toBe('DECLINED');
    expect(result.reasons).toContain('credit_score_below_threshold');
  });

  it('emits outcome and notifies applicant on underwriting DECLINE (bug 3)', async () => {
    currentMocks.pullCreditReport = jest.fn(async () => ({
      score: 580, openTradelines: 3, delinquencies: 0, bureau: 'Equifax', pulledAt: new Date().toISOString(),
    }));
    await (await startWorkflow(app('declined-emit-1'))).result();
    expect(currentMocks.emitOutcome).toHaveBeenCalledWith(expect.objectContaining({ status: 'DECLINED' }));
    expect(currentMocks.notifyApplicant).toHaveBeenCalledWith(expect.objectContaining({ template: 'DECLINED' }));
  });
});

// ── Human-in-the-loop ─────────────────────────────────────────────────────

describe('Human-in-the-loop path', () => {
  it('funds after human APPROVE', async () => {
    const handle = await startWorkflow(autoOverLimit('human-approve-1'));
    await waitForStatus(handle, 'PENDING_HUMAN_REVIEW');

    await handle.executeUpdate(submitHumanDecision, {
      args: [{ type: 'APPROVE', reviewerId: 'rev-1', notes: 'ok', approvedAmount: 65_000, interestRate: 0.08 }],
    });
    const result = await handle.result();
    expect(result.status).toBe('FUNDED');
    expect(result.reasons).toContain('human_approved');
  });

  it('declines after human DECLINE and emits outcome (bug 3)', async () => {
    const handle = await startWorkflow(autoOverLimit('human-decline-1'));
    await waitForStatus(handle, 'PENDING_HUMAN_REVIEW');

    await handle.executeUpdate(submitHumanDecision, {
      args: [{ type: 'DECLINE', reviewerId: 'rev-1', notes: 'too risky' } as HumanDecision],
    });
    const result = await handle.result();
    expect(result.status).toBe('DECLINED');
    expect(result.reasons).toContain('human_declined');
    expect(currentMocks.emitOutcome).toHaveBeenCalledWith(expect.objectContaining({ status: 'DECLINED' }));
  });

  it('rejects submitHumanDecision when not in PENDING_HUMAN_REVIEW', async () => {
    const handle = await startWorkflow(app('human-wrong-state-1'));

    await expectUpdateRejection(
      handle.executeUpdate(submitHumanDecision, {
        args: [{ type: 'APPROVE', reviewerId: 'rev-1', notes: 'test', approvedAmount: 15_000, interestRate: 0.069 } as HumanDecision],
      }),
      'application is not awaiting a human decision',
    );

    await handle.result();
  });

  it('rejects approval exceeding reviewer authority limit of $100,000 (bug 6)', async () => {
    const handle = await startWorkflow(autoOverLimit('human-over-authority-1'));
    await waitForStatus(handle, 'PENDING_HUMAN_REVIEW');

    await expectUpdateRejection(
      handle.executeUpdate(submitHumanDecision, {
        args: [{ type: 'APPROVE', reviewerId: 'rev-1', notes: 'test', approvedAmount: 150_000, interestRate: 0.08 } as HumanDecision],
      }),
      'approval exceeds reviewer authority limit',
    );

    await handle.executeUpdate(submitHumanDecision, {
      args: [{ type: 'APPROVE', reviewerId: 'rev-1', notes: 'within limit', approvedAmount: 65_000, interestRate: 0.08 }],
    });
    await handle.result();
  });

  it('funds after REQUEST_DOCUMENTS → second underwriting pass (bug 2 — unique child workflow IDs)', async () => {
    currentMocks.verifyIdentity = jest.fn(async () => ({ verified: false, matchScore: 20, provider: 'Onfido' }));

    const a = app('re-uw-1', {
      documents: [
        { type: 'ID', uri: 's3://id', receivedAt: new Date().toISOString() },
        { type: 'PROOF_OF_INCOME', uri: 's3://income', receivedAt: new Date().toISOString() },
      ],
    });
    const handle = await startWorkflow(a, { docWaitDurationMs: 30_000 });

    await waitForStatus(handle, 'PENDING_HUMAN_REVIEW');
    await handle.executeUpdate(submitHumanDecision, {
      args: [{
        type: 'REQUEST_DOCUMENTS',
        reviewerId: 'rev-1',
        notes: 'need bank statement',
        requestedDocuments: ['BANK_STATEMENT'],
      } as HumanDecision],
    });

    await waitForStatus(handle, 'AWAITING_DOCUMENTS');
    await handle.executeUpdate(submitDocuments, {
      args: [{ documents: [{ type: 'BANK_STATEMENT', uri: 's3://bank', receivedAt: new Date().toISOString() }] }],
    });

    await waitForStatus(handle, 'PENDING_HUMAN_REVIEW');
    await handle.executeUpdate(submitHumanDecision, {
      args: [{ type: 'APPROVE', reviewerId: 'rev-1', notes: 'approved', approvedAmount: 15_000, interestRate: 0.069 }],
    });

    const result = await handle.result();
    expect(result.status).toBe('FUNDED');
  }, 120_000);
});

// ── WITHDRAWN ─────────────────────────────────────────────────────────────

describe('WITHDRAWN path', () => {
  it('transitions to WITHDRAWN when applicant signals withdrawal', async () => {
    const handle = await startWorkflow(autoOverLimit('withdraw-1'));
    await waitForStatus(handle, 'PENDING_HUMAN_REVIEW');
    await handle.signal(withdraw);
    const result = await handle.result();
    expect(result.status).toBe('WITHDRAWN');
  });

  it('sends withdrawal notification (bug 5) and emits outcome (bug 3)', async () => {
    const handle = await startWorkflow(autoOverLimit('withdraw-notify-1'));
    await waitForStatus(handle, 'PENDING_HUMAN_REVIEW');
    await handle.signal(withdraw);
    await handle.result();

    expect(currentMocks.notifyApplicant).toHaveBeenCalledWith(
      expect.objectContaining({ template: 'WITHDRAWN' }),
    );
    expect(currentMocks.emitOutcome).toHaveBeenCalledWith(
      expect.objectContaining({ status: 'WITHDRAWN' }),
    );
  });
});

// ── EXPIRED — 48h SLA breach ───────────────────────────────────────────────

describe('EXPIRED path — 48h SLA breach', () => {
  it('expires when the SLA timer fires before a terminal state is reached', async () => {
    // slaDurationMs: 5_000 gives enough time for underwriting to run but forces
    // EXPIRED before any human decision arrives. The workflow will reach
    // PENDING_HUMAN_REVIEW (since $65k AUTO exceeds the auto-approve limit),
    // then the SLA fires at t=5s and wins the race over runPipeline().
    const result = await (
      await startWorkflow(autoOverLimit('sla-expire-1'), { slaDurationMs: 5_000 })
    ).result();
    expect(result.status).toBe('EXPIRED');
    expect(result.reasons).toContain('sla_breach_48h');
  }, 30_000);
});

// ── getState query ─────────────────────────────────────────────────────────

describe('getState Query', () => {
  it('returns correct product, channel, and status when pending review', async () => {
    const handle = await startWorkflow(autoOverLimit('query-1'));
    await waitForStatus(handle, 'PENDING_HUMAN_REVIEW');

    const state = await handle.query(getState);
    expect(state.product).toBe('AUTO');
    expect(state.channel).toBe('PORTAL');
    expect(state.status).toBe('PENDING_HUMAN_REVIEW');
    expect(state.slaDeadline).toBeTruthy();

    await handle.executeUpdate(submitHumanDecision, {
      args: [{ type: 'APPROVE', reviewerId: 'rev-1', notes: 'cleanup', approvedAmount: 65_000, interestRate: 0.08 }],
    });
    await handle.result();
  });
});

// ── AWAITING_DOCUMENTS baseline (bug A) ────────────────────────────────────
// The doc-wait loop's wf.condition originally compared against the
// IMMUTABLE app.documents.length. After the first submission, any second
// pass would fall through immediately, spinning notify/validate without
// ever waiting for the next batch of docs. Fix: snapshot per iteration.

describe('AWAITING_DOCUMENTS baseline (bug A)', () => {
  it('does not tight-loop notifyApplicant if the wrong docs arrive', async () => {
    // App missing ID. Applicant submits only a duplicate PROOF_OF_INCOME on
    // the first pass; validation still reports ID missing. Then a real ID is
    // supplied. We assert that notifyApplicant fires exactly once per actual
    // AWAITING_DOCUMENTS entry (2 entries total), not on every loop iteration.
    const a = app('docs-baseline-1', {
      documents: [{ type: 'PROOF_OF_INCOME', uri: 's3://income', receivedAt: new Date().toISOString() }],
    });
    const handle = await startWorkflow(a, { docWaitDurationMs: 30_000 });
    await waitForStatus(handle, 'AWAITING_DOCUMENTS');

    // Wrong-type doc — does NOT satisfy validation; should keep waiting.
    await handle.executeUpdate(submitDocuments, {
      args: [{ documents: [{ type: 'PROOF_OF_INCOME', uri: 's3://income-2', receivedAt: new Date().toISOString() }] }],
    });

    // Give the workflow a chance to re-validate and re-enter the wait.
    // With the bug, this window would see MANY notifyApplicant calls.
    await sleep(2_000);

    expect(currentMocks.notifyApplicant.mock.calls.length).toBeLessThanOrEqual(3);

    // Now supply the real ID.
    await handle.executeUpdate(submitDocuments, {
      args: [{ documents: [{ type: 'ID', uri: 's3://id', receivedAt: new Date().toISOString() }] }],
    });
    const result = await handle.result();
    expect(result.status).toBe('FUNDED');
  }, 30_000);
});

// ── Concurrent human decisions (bug B) ─────────────────────────────────────
// Two reviewers racing on the same case both used to be "accepted" because
// status was still PENDING_HUMAN_REVIEW at validator time. The second one
// silently clobbered the first. Fix: validator rejects when a decision is
// already pending consumption.

describe('Concurrent human decisions (bug B)', () => {
  it('rejects a second submitHumanDecision while one is already pending', async () => {
    const handle = await startWorkflow(autoOverLimit('human-race-1'));
    await waitForStatus(handle, 'PENDING_HUMAN_REVIEW');

    // Fire two Updates back-to-back. Temporal serialises Update handlers;
    // the first should land, the second should be rejected by the validator
    // because pendingHumanDecision is already set.
    const first = handle.executeUpdate(submitHumanDecision, {
      args: [{ type: 'APPROVE', reviewerId: 'rev-A', notes: 'first', approvedAmount: 65_000, interestRate: 0.08 }],
    });
    const second = handle.executeUpdate(submitHumanDecision, {
      args: [{ type: 'DECLINE', reviewerId: 'rev-B', notes: 'second' } as HumanDecision],
    });

    const [r1, r2] = await Promise.allSettled([first, second]);
    // One must succeed and the other must be rejected with our specific message.
    const fulfilled = [r1, r2].filter((r) => r.status === 'fulfilled');
    const rejected = [r1, r2].filter((r) => r.status === 'rejected');
    expect(fulfilled.length).toBe(1);
    expect(rejected.length).toBe(1);
    // The rejection wrapper's message is "Workflow Update failed"; walk causes.
    const rej = rejected[0] as PromiseRejectedResult;
    const messages: string[] = [];
    let cur: unknown = rej.reason;
    for (let i = 0; i < 5 && cur; i++) {
      const e = cur as { message?: string; cause?: unknown };
      if (typeof e.message === 'string') messages.push(e.message);
      cur = e.cause;
    }
    expect(messages.join(' | ')).toContain('a decision is already pending consumption');

    const result = await handle.result();
    // The first one (APPROVE) wins; ensure exactly one decision was processed.
    expect(result.status).toBe('FUNDED');
    expect(result.reasons).toContain('human_approved');
  }, 30_000);
});

// ── REQUEST_DOCUMENTS surfaces requested docs (bug C) ──────────────────────

describe('REQUEST_DOCUMENTS surfaces requested docs (bug C)', () => {
  it('exposes human.requestedDocuments via getState.missingDocuments', async () => {
    currentMocks.verifyIdentity = jest.fn(async () => ({ verified: false, matchScore: 20, provider: 'Onfido' }));

    const handle = await startWorkflow(app('req-docs-state-1'), { docWaitDurationMs: 30_000 });
    await waitForStatus(handle, 'PENDING_HUMAN_REVIEW');

    await handle.executeUpdate(submitHumanDecision, {
      args: [{
        type: 'REQUEST_DOCUMENTS',
        reviewerId: 'rev-1',
        notes: 'need bank statement and ID copy',
        requestedDocuments: ['BANK_STATEMENT'],
      } as HumanDecision],
    });

    await waitForStatus(handle, 'AWAITING_DOCUMENTS');
    const st = await handle.query(getState);
    expect(st.missingDocuments).toEqual(['BANK_STATEMENT']);

    // Clean up so the test ends.
    await handle.executeUpdate(submitDocuments, {
      args: [{ documents: [{ type: 'BANK_STATEMENT', uri: 's3://bank', receivedAt: new Date().toISOString() }] }],
    });
    await waitForStatus(handle, 'PENDING_HUMAN_REVIEW');
    await handle.executeUpdate(submitHumanDecision, {
      args: [{ type: 'APPROVE', reviewerId: 'rev-1', notes: 'ok', approvedAmount: 15_000, interestRate: 0.069 }],
    });
    await handle.result();
  }, 60_000);
});

// ── Malformed application → clean DECLINED (bug D) ─────────────────────────
// Previously a non-retryable ValidationError from the activity would crash
// the workflow with no terminal outcome. Now we catch it and exit DECLINED.

describe('Malformed application reaches DECLINED (bug D)', () => {
  it('treats a non-positive requestedAmount as DECLINED and emits outcome', async () => {
    const a = app('malformed-amount-1', { requestedAmount: 0 });
    const result = await (await startWorkflow(a)).result();
    expect(result.status).toBe('DECLINED');
    expect(result.reasons).toContain('malformed_application');
    expect(currentMocks.emitOutcome).toHaveBeenCalledWith(expect.objectContaining({ status: 'DECLINED' }));
  });

  it('does not call notifyApplicant when the email itself is malformed', async () => {
    const a = app('malformed-email-1', {
      applicant: { ...baseApp.applicant, email: 'not-an-email' },
    });
    const result = await (await startWorkflow(a)).result();
    expect(result.status).toBe('DECLINED');
    expect(result.reasons).toContain('malformed_application');
    // No DECLINED email should have gone out for an unreachable address.
    const declineNotify = currentMocks.notifyApplicant.mock.calls.find(
      (c) => (c[0] as { template: string }).template === 'DECLINED',
    );
    expect(declineNotify).toBeUndefined();
  });
});
