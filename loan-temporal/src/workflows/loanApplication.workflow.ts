/**
 * LoanApplicationWorkflow — one instance per application, keyed by applicationId.
 *
 * This single workflow IS the application lifecycle state machine. It is durable:
 * if every worker crashes mid-underwriting or while waiting days for a human, the
 * exact position is recovered from event history and execution resumes. That
 * history is also the spine of the audit trail.
 *
 * State is exposed three ways:
 *   - Query  `getState`            -> ops UI reads "where is this app right now?"
 *   - Update `submitHumanDecision` -> a reviewer pushes a decision IN (validated)
 *   - Update `submitDocuments`     -> applicant supplies missing docs
 *   - Signal `withdraw`            -> applicant abandons
 *   - Search Attributes            -> fleet-wide filtering without touching each wf
 */
import * as wf from '@temporalio/workflow';
import { ActivityFailure, ApplicationFailure } from '@temporalio/common';

/** Maximum amount a reviewer can approve without escalation. Named constant so it's easy to locate and replace with a per-reviewer lookup when authority tiers are needed. */
const REVIEWER_AUTHORITY_LIMIT = 100_000;

import type {
  LoanApplication,
  ApplicationStatus,
  EnrichmentBundle,
  HumanDecision,
  DocumentSubmission,
  UnderwritingResult,
  LoanOutcome,
  AuditEvent,
} from '../shared/types';
import {
  SA_STATUS,
  SA_PRODUCT,
  SA_CHANNEL,
  SA_REVIEWER,
  SA_SLA_DEADLINE,
  SA_AMOUNT,
  TASK_QUEUES,
} from '../shared/searchAttributes';
import type * as activities from '../activities';
import { personalLoanUnderwriting } from './underwriting/personalLoan.workflow';
import { autoLoanUnderwriting } from './underwriting/autoLoan.workflow';

// --- activity proxies with per-call retry policies -----------------------

// Generic activities: idempotent, retry hard.
const { recordAudit, notifyApplicant, createReviewTask, closeReviewTask, emitOutcome, validateApplication } =
  wf.proxyActivities<typeof activities>({
    startToCloseTimeout: '30 seconds',
    retry: { maximumAttempts: 10, initialInterval: '1s', backoffCoefficient: 2 },
  });

// Third-party enrichment: longer schedule-to-close so we ride out brief outages,
// heartbeat so a hung call is detected. NOTE: these proxies route to per-provider
// task queues where the worker enforces that provider's rate limit.
const credit = wf.proxyActivities<typeof activities>({
  taskQueue: TASK_QUEUES.PROVIDER_CREDIT,
  scheduleToCloseTimeout: '10 minutes',
  startToCloseTimeout: '30 seconds',
  heartbeatTimeout: '10 seconds',
  retry: { initialInterval: '2s', backoffCoefficient: 2, maximumInterval: '1 minute', maximumAttempts: 8 },
});
const identity = wf.proxyActivities<typeof activities>({
  taskQueue: TASK_QUEUES.PROVIDER_IDENTITY,
  scheduleToCloseTimeout: '10 minutes',
  startToCloseTimeout: '30 seconds',
  heartbeatTimeout: '10 seconds',
  retry: { initialInterval: '2s', backoffCoefficient: 2, maximumInterval: '1 minute', maximumAttempts: 8 },
});
const fraud = wf.proxyActivities<typeof activities>({
  taskQueue: TASK_QUEUES.PROVIDER_FRAUD,
  scheduleToCloseTimeout: '10 minutes',
  startToCloseTimeout: '30 seconds',
  heartbeatTimeout: '10 seconds',
  retry: { initialInterval: '2s', backoffCoefficient: 2, maximumInterval: '1 minute', maximumAttempts: 8 },
});

// Funding: idempotent via applicationId key, retried aggressively.
const funding = wf.proxyActivities<typeof activities>({
  startToCloseTimeout: '60 seconds',
  retry: { maximumAttempts: 20, initialInterval: '2s', backoffCoefficient: 2, maximumInterval: '2 minutes' },
});

// --- signals / queries / updates -----------------------------------------

export const getState = wf.defineQuery<WorkflowState>('getState');
export const submitHumanDecision = wf.defineUpdate<void, [HumanDecision]>('submitHumanDecision');
export const submitDocuments = wf.defineUpdate<void, [DocumentSubmission]>('submitDocuments');
export const withdraw = wf.defineSignal('withdraw');

export interface WorkflowState {
  status: ApplicationStatus;
  product: string;
  channel: string;
  reasons: string[];
  assignedReviewer?: string;
  slaDeadline: string;
  missingDocuments: string[];
}

const DEFAULT_SLA_MS = 48 * 60 * 60 * 1000;
const DEFAULT_DOC_WAIT_MS = 24 * 60 * 60 * 1000;
const DEFAULT_HUMAN_REVIEW_SLA_MS = 4 * 60 * 60 * 1000;

/** Override the default timer durations. Intended only for tests. */
export interface WorkflowTimers {
  slaDurationMs?: number;
  docWaitDurationMs?: number;
  humanReviewSlaDurationMs?: number;
}

export async function loanApplicationWorkflow(
  app: LoanApplication,
  timers: WorkflowTimers = {},
): Promise<LoanOutcome> {
  const SLA_MS = timers.slaDurationMs ?? DEFAULT_SLA_MS;
  const DOC_WAIT_MS = timers.docWaitDurationMs ?? DEFAULT_DOC_WAIT_MS;
  const HUMAN_REVIEW_SLA_MS = timers.humanReviewSlaDurationMs ?? DEFAULT_HUMAN_REVIEW_SLA_MS;

  // Note: in the TypeScript SDK, Date.now() and new Date() are overridden
  // inside the workflow sandbox to return the deterministic activator time
  // (see @temporalio/workflow/lib/global-overrides.js). Unlike the Go/Java
  // SDKs, no special wf.now() is required here.
  const startMs = Date.now();
  const slaDeadline = new Date(startMs + SLA_MS);

  // mutable workflow state, also surfaced via the getState query
  const state: WorkflowState = {
    status: 'INGESTED',
    product: app.product,
    channel: app.channel,
    reasons: [],
    slaDeadline: slaDeadline.toISOString(),
    missingDocuments: [],
  };

  let pendingHumanDecision: HumanDecision | undefined;
  let withdrawn = false;
  let submittedDocs = app.documents.slice();
  let uwAttempt = 0;

  // ---- transition helper: one place that updates status + SA + audit ----
  async function transition(status: ApplicationStatus, detail: Record<string, unknown> = {}) {
    state.status = status;
    wf.upsertSearchAttributes([{ key: SA_STATUS, value: status }]);
    const event: AuditEvent = {
      applicationId: app.applicationId,
      at: new Date().toISOString(),
      actor: detail.actor ? String(detail.actor) : 'system:orchestrator',
      event: `transition:${status}`,
      status,
      detail,
    };
    await recordAudit(event);
  }

  // ---- handlers ----------------------------------------------------------
  wf.setHandler(getState, () => state);

  wf.setHandler(withdraw, () => {
    withdrawn = true;
  });

  // Document submission can arrive at any time; we merge it in.
  wf.setHandler(
    submitDocuments,
    (sub) => {
      submittedDocs = [...submittedDocs, ...sub.documents];
    },
    {
      validator: (sub) => {
        if (!sub.documents?.length) throw new Error('no documents supplied');
        if (state.status !== 'AWAITING_DOCUMENTS') throw new Error('application is not awaiting documents');
      },
    },
  );

  // Human decision: an Update (not a Signal) so the reviewer's UI gets a
  // synchronous accept/reject, and the validator enforces reviewer authority
  // BEFORE the decision is admitted to history.
  //
  // The "already pending" guard prevents a second reviewer from silently
  // overwriting a first reviewer's decision in the window between when the
  // first Update is accepted and when the main coroutine wakes up to consume
  // it. Without this, both Updates pass the status check (status is still
  // PENDING_HUMAN_REVIEW until the workflow processes the first one) and the
  // second handler clobbers the first — both reviewers see "accepted" but
  // only the latest decision lands in history.
  wf.setHandler(
    submitHumanDecision,
    (decision) => {
      pendingHumanDecision = decision;
      state.assignedReviewer = decision.reviewerId;
    },
    {
      validator: (decision) => {
        if (state.status !== 'PENDING_HUMAN_REVIEW') {
          throw new Error('application is not awaiting a human decision');
        }
        if (pendingHumanDecision !== undefined) {
          throw new Error('a decision is already pending consumption');
        }
        if (decision.type === 'APPROVE' && (decision.approvedAmount ?? 0) > REVIEWER_AUTHORITY_LIMIT) {
          throw new Error('approval exceeds reviewer authority limit');
        }
      },
    },
  );

  // Seed search attributes once.
  wf.upsertSearchAttributes([
    { key: SA_PRODUCT, value: app.product },
    { key: SA_CHANNEL, value: app.channel },
    { key: SA_AMOUNT, value: app.requestedAmount },
    { key: SA_SLA_DEADLINE, value: slaDeadline },
  ]);

  // ---- overall 48h SLA, raced against the whole pipeline ----------------
  // If the deadline passes before we reach a terminal state, we escalate/expire.
  const outcome = await Promise.race([runPipeline(), enforceSla()]);
  return outcome;

  // ===== inner functions (closures over state) ===========================

  async function enforceSla(): Promise<LoanOutcome> {
    await wf.sleep(SLA_MS);
    // Reaching here means the pipeline didn't finish in time.
    //
    // We also defer when funding is in flight: Promise.race doesn't cancel
    // the loser, so if we mark EXPIRED while disburseFunds is mid-flight
    // the workflow ends up with FUNDING -> EXPIRED -> FUNDED in history and
    // both EXPIRED and FUNDED outcomes hitting billing. The disbursement
    // activity is idempotent and on its own retry budget; let it land.
    if (isTerminal(state.status) || state.status === 'FUNDING') {
      return new Promise<LoanOutcome>(() => {}); // never resolves
    }
    await transition('EXPIRED', { reason: 'sla_breach_48h' });
    await emitOutcome(buildOutcome('EXPIRED', ['sla_breach_48h']));
    return buildOutcome('EXPIRED', ['sla_breach_48h']);
  }

  async function runPipeline(): Promise<LoanOutcome> {
    await transition('INGESTED', { submittedAt: app.submittedAt });

    // --- validation ---
    await transition('VALIDATING');
    if (withdrawn) return abandon();
    // A non-retryable ValidationError (bad email, non-positive amount) is a
    // permanent business reject — NOT a workflow-level crash. Without this
    // catch the workflow execution fails, skipping emitOutcome and leaving
    // the application without a terminal state, which violates the brief's
    // "every application reaches funded, declined, or escalated".
    // The activity-thrown ApplicationFailure is wrapped in ActivityFailure
    // when it crosses the workflow boundary, so check both.
    let validation;
    try {
      validation = await validateApplication({ ...app, documents: submittedDocs });
    } catch (err) {
      const failure =
        err instanceof ActivityFailure && err.cause instanceof ApplicationFailure
          ? err.cause
          : err instanceof ApplicationFailure
            ? err
            : undefined;
      if (failure && failure.type === 'ValidationError') {
        const reasons = ['malformed_application', failure.message];
        await transition('DECLINED', { reason: 'malformed_application', error: failure.message });
        // Best-effort notify: skip if the email itself is malformed (otherwise
        // we'd hammer the email service trying to deliver to a syntactically
        // invalid address until the activity exhausts retries).
        if (app.applicant.email.includes('@')) {
          await notifyApplicant({ applicationId: app.applicationId, email: app.applicant.email, template: 'DECLINED' });
        }
        return await finishDeclined(reasons);
      }
      throw err;
    }

    // --- await documents if anything is missing ---
    // Snapshot the doc count at the top of EACH iteration. Comparing against
    // a stale baseline (e.g. the original app.documents.length) would let the
    // loop fall through immediately on the second pass once any docs had been
    // submitted, spinning on notify/validate without ever waiting for new docs.
    while (!validation.ok) {
      const before = submittedDocs.length;
      state.missingDocuments = validation.missingDocuments;
      await transition('AWAITING_DOCUMENTS', { missing: validation.missingDocuments });
      await notifyApplicant({
        applicationId: app.applicationId,
        email: app.applicant.email,
        template: 'MISSING_DOCS',
        data: { missing: validation.missingDocuments },
      });

      const got = await wf.condition(
        () => withdrawn || submittedDocs.length > before,
        DOC_WAIT_MS,
      );
      if (withdrawn) return abandon();
      if (!got) {
        // Applicant never supplied docs -> decline as incomplete (terminal).
        await transition('DECLINED', { reason: 'documents_not_supplied' });
        return await finishDeclined(['documents_not_supplied']);
      }
      validation = await validateApplication({ ...app, documents: submittedDocs });
    }

    // --- enrichment (partial-failure tolerant) ---
    await transition('ENRICHING');
    if (withdrawn) return abandon();
    const enrichment = await enrich();

    // --- product-isolated underwriting via child workflow ---
    await transition('UNDERWRITING');
    let result = await runUnderwriting(enrichment);
    state.reasons = result.reasons;

    // --- decision loop (human review can send us back for docs) ---
    while (true) {
      if (withdrawn) return abandon();

      if (result.decision === 'APPROVE') {
        return await fund(result);
      }
      if (result.decision === 'DECLINE') {
        await transition('DECLINED', { reasons: result.reasons, rulesetVersion: result.rulesetVersion });
        await notifyApplicant({ applicationId: app.applicationId, email: app.applicant.email, template: 'DECLINED' });
        return await finishDeclined(result.reasons);
      }

      // REFER -> human in the loop
      const human = await awaitHumanDecision(result);
      if (withdrawn) return abandon();

      if (human.type === 'APPROVE') {
        result = {
          decision: 'APPROVE',
          reasons: [...result.reasons, 'human_approved'],
          rulesetVersion: result.rulesetVersion,
          approvedAmount: human.approvedAmount ?? result.approvedAmount ?? app.requestedAmount,
          interestRate: human.interestRate ?? result.interestRate ?? 0.129,
        };
      } else if (human.type === 'DECLINE') {
        await transition('DECLINED', { actor: `human:${human.reviewerId}`, notes: human.notes });
        await notifyApplicant({ applicationId: app.applicationId, email: app.applicant.email, template: 'DECLINED' });
        return await finishDeclined([...result.reasons, 'human_declined']);
      } else {
        // REQUEST_DOCUMENTS -> loop back to awaiting docs, then re-underwrite
        const before = submittedDocs.length;
        // Surface the human's request via the getState query so ops dashboards
        // show what's actually outstanding instead of whatever was missing the
        // last time validation ran (typically nothing — validation already passed).
        state.missingDocuments = human.requestedDocuments ?? [];
        await transition('AWAITING_DOCUMENTS', { requestedBy: human.reviewerId });
        await notifyApplicant({
          applicationId: app.applicationId,
          email: app.applicant.email,
          template: 'MISSING_DOCS',
          data: { requested: human.requestedDocuments },
        });
        const got = await wf.condition(() => withdrawn || submittedDocs.length > before, DOC_WAIT_MS);
        if (withdrawn) return abandon();
        if (!got) {
          await transition('DECLINED', { reason: 'requested_documents_not_supplied' });
          return await finishDeclined(['requested_documents_not_supplied']);
        }
        await transition('UNDERWRITING');
        result = await runUnderwriting(await enrich());
      }
    }
  }

  // --- enrichment with tolerance for partial failure ---------------------
  async function enrich(): Promise<EnrichmentBundle> {
    // Run all three concurrently. allSettled-style: a provider that exhausts
    // its retries yields null rather than failing the whole application.
    const [c, i, f] = await Promise.all([
      credit.pullCreditReport(app).catch(() => null),
      identity.verifyIdentity(app).catch(() => null),
      fraud.screenForFraud(app).catch(() => null),
    ]);
    await recordAudit({
      applicationId: app.applicationId,
      at: new Date().toISOString(),
      actor: 'system:enrichment',
      event: 'enrichment_complete',
      status: state.status,
      detail: { credit: !!c, identity: !!i, fraud: !!f },
    });
    return { credit: c, identity: i, fraud: f };
  }

  // --- product isolation: delegate to the product's child workflow -------
  // uwAttempt increments on every call so the child workflow ID is unique even
  // when a human reviewer sends the application back for documents and
  // underwriting runs a second (or third) time. Without the suffix, Temporal
  // rejects the second executeChild call because a closed workflow with the
  // same ID already exists in the namespace retention window.
  async function runUnderwriting(enrichment: EnrichmentBundle): Promise<UnderwritingResult> {
    const attempt = ++uwAttempt;
    const input = { application: app, enrichment };
    switch (app.product) {
      case 'PERSONAL':
        return wf.executeChild(personalLoanUnderwriting, {
          args: [input],
          taskQueue: TASK_QUEUES.UNDERWRITING_PERSONAL,
          workflowId: `uw-personal-${app.applicationId}-${attempt}`,
        });
      case 'AUTO':
        return wf.executeChild(autoLoanUnderwriting, {
          args: [input],
          taskQueue: TASK_QUEUES.UNDERWRITING_AUTO,
          workflowId: `uw-auto-${app.applicationId}-${attempt}`,
        });
      case 'DEBT_CONSOLIDATION':
        // For brevity this demo reuses personal rules on its own queue; in
        // production this is its own workflow + worker.
        return wf.executeChild(personalLoanUnderwriting, {
          args: [input],
          taskQueue: TASK_QUEUES.UNDERWRITING_DEBT,
          workflowId: `uw-debt-${app.applicationId}-${attempt}`,
        });
      default:
        throw ApplicationFailure.create({ message: `unknown product ${app.product}`, nonRetryable: true });
    }
  }

  // --- human-in-the-loop: pause until an Update arrives ------------------
  async function awaitHumanDecision(result: UnderwritingResult): Promise<HumanDecision> {
    await transition('PENDING_HUMAN_REVIEW', { reasons: result.reasons });
    await createReviewTask({ applicationId: app.applicationId, product: app.product, reasons: result.reasons });

    // Block here — possibly for hours/days — until submitHumanDecision sets it.
    // We also wake periodically to escalate stale reviews (search-attribute nudge).
    // Escalation is one-shot: without the flag the loop would re-fire an
    // identical audit row and a no-op SA upsert every HUMAN_REVIEW_SLA_MS until
    // a decision arrives, flooding the regulator-facing audit store.
    let escalated = false;
    while (pendingHumanDecision === undefined && !withdrawn) {
      const decided = await wf.condition(
        () => pendingHumanDecision !== undefined || withdrawn,
        HUMAN_REVIEW_SLA_MS,
      );
      if (!decided && pendingHumanDecision === undefined && !withdrawn && !escalated) {
        await recordAudit({
          applicationId: app.applicationId,
          at: new Date().toISOString(),
          actor: 'system:orchestrator',
          event: 'review_sla_breach_escalation',
          status: state.status,
          detail: {},
        });
        wf.upsertSearchAttributes([{ key: SA_REVIEWER, value: 'ESCALATED' }]);
        escalated = true;
      }
    }
    const decision = pendingHumanDecision!;
    pendingHumanDecision = undefined;
    await closeReviewTask({ applicationId: app.applicationId, decision });
    return decision;
  }

  // --- funding ------------------------------------------------------------
  async function fund(result: UnderwritingResult): Promise<LoanOutcome> {
    await transition('FUNDING', { approvedAmount: result.approvedAmount });
    const { disbursementId } = await funding.disburseFunds({
      applicationId: app.applicationId,
      amount: result.approvedAmount ?? app.requestedAmount,
      interestRate: result.interestRate ?? 0.129,
    });
    await transition('FUNDED', { disbursementId, amount: result.approvedAmount });
    await notifyApplicant({ applicationId: app.applicationId, email: app.applicant.email, template: 'APPROVED' });
    const out = buildOutcome('FUNDED', result.reasons, result.approvedAmount, disbursementId);
    await emitOutcome(out);
    return out;
  }

  // --- terminal helpers ---------------------------------------------------
  async function abandon(): Promise<LoanOutcome> {
    await transition('WITHDRAWN', { reason: 'applicant_withdrew' });
    await notifyApplicant({ applicationId: app.applicationId, email: app.applicant.email, template: 'WITHDRAWN' });
    const out = buildOutcome('WITHDRAWN', ['applicant_withdrew']);
    await emitOutcome(out);
    return out;
  }
  async function finishDeclined(reasons: string[]): Promise<LoanOutcome> {
    const out = buildOutcome('DECLINED', reasons);
    await emitOutcome(out);
    return out;
  }
  function buildOutcome(
    status: LoanOutcome['status'],
    reasons: string[],
    fundedAmount?: number,
    disbursementId?: string,
  ): LoanOutcome {
    return { applicationId: app.applicationId, status, reasons, fundedAmount, disbursementId };
  }
}

function isTerminal(s: ApplicationStatus): boolean {
  return s === 'FUNDED' || s === 'DECLINED' || s === 'WITHDRAWN' || s === 'EXPIRED';
}
