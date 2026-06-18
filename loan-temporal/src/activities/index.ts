/**
 * Activities are the ONLY place side effects happen: every network call, DB
 * write, and third-party integration lives here. Workflows stay deterministic
 * and just orchestrate. Each activity is independently retried by Temporal
 * according to the retry policy attached where it's proxied (see workflow files).
 *
 * Implementations here are mocked so the project runs locally with no real
 * credentials, but the signatures and the failure/idempotency semantics are
 * what you'd keep in production.
 */
import { Context, log } from '@temporalio/activity';
import { ApplicationFailure } from '@temporalio/common';
import type {
  LoanApplication,
  CreditReport,
  IdentityResult,
  FraudResult,
  AuditEvent,
  LoanOutcome,
  DocumentRef,
  HumanDecision,
} from '../shared/types';

// ---- helpers -------------------------------------------------------------

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

/** Simulate a flaky provider: ~25% transient failures, rare hard failures. */
async function flakyProvider<T>(name: string, makeResult: () => T): Promise<T> {
  // Heartbeat so Temporal can detect a hung activity and so retries on a new
  // worker can resume from the last reported progress for long-running work.
  Context.current().heartbeat(`calling ${name}`);
  await sleep(150);
  const roll = Math.random();
  if (roll < 0.25) {
    // Transient -> retryable. Temporal will back off and try again.
    throw new Error(`${name} transient error (HTTP 503)`);
  }
  return makeResult();
}

// ---- validation ----------------------------------------------------------

export interface ValidationResult {
  ok: boolean;
  missingDocuments: DocumentRef['type'][];
  errors: string[];
}

const REQUIRED_DOCS: Record<string, DocumentRef['type'][]> = {
  PERSONAL: ['ID', 'PROOF_OF_INCOME'],
  AUTO: ['ID', 'PROOF_OF_INCOME', 'VEHICLE_TITLE'],
  DEBT_CONSOLIDATION: ['ID', 'PROOF_OF_INCOME', 'BANK_STATEMENT'],
};

export async function validateApplication(app: LoanApplication): Promise<ValidationResult> {
  const errors: string[] = [];
  if (app.requestedAmount <= 0) errors.push('requestedAmount must be positive');
  if (!app.applicant.email.includes('@')) errors.push('invalid email');

  const required = REQUIRED_DOCS[app.product] ?? [];
  const have = new Set(app.documents.map((d) => d.type));
  const missingDocuments = required.filter((t) => !have.has(t));

  // A malformed application (bad data) is a non-retryable business failure.
  if (errors.length > 0) {
    throw ApplicationFailure.create({
      message: `Validation failed: ${errors.join('; ')}`,
      type: 'ValidationError',
      nonRetryable: true,
    });
  }
  return { ok: missingDocuments.length === 0, missingDocuments, errors };
}

// ---- enrichment (third-party) -------------------------------------------

export async function pullCreditReport(app: LoanApplication): Promise<CreditReport> {
  return flakyProvider('CreditBureau', () => ({
    score: 600 + Math.floor(Math.random() * 250),
    openTradelines: Math.floor(Math.random() * 12),
    delinquencies: Math.random() < 0.2 ? 1 : 0,
    bureau: 'Equifax',
    pulledAt: new Date().toISOString(),
  }));
}

export async function verifyIdentity(app: LoanApplication): Promise<IdentityResult> {
  return flakyProvider('IdentityProvider', () => ({
    verified: Math.random() > 0.05,
    matchScore: 70 + Math.floor(Math.random() * 30),
    provider: 'Onfido',
  }));
}

export async function screenForFraud(app: LoanApplication): Promise<FraudResult> {
  return flakyProvider('FraudProvider', () => ({
    riskScore: Math.floor(Math.random() * 100),
    signals: Math.random() < 0.15 ? ['velocity_anomaly'] : [],
    provider: 'Sift',
  }));
}

// ---- funding (must be idempotent!) --------------------------------------

/**
 * Disbursement is the one activity that must NEVER run twice for the same
 * application, because Temporal can retry an activity whose result was lost.
 * We pass an idempotency key (the applicationId) so the payment rail dedups.
 */
export async function disburseFunds(input: {
  applicationId: string;
  amount: number;
  interestRate: number;
}): Promise<{ disbursementId: string }> {
  Context.current().heartbeat('initiating disbursement');
  await sleep(200);
  // Real impl: POST /transfers with Idempotency-Key: input.applicationId
  if (Math.random() < 0.1) {
    throw new Error('payment rail transient error; safe to retry with same key');
  }
  return { disbursementId: `disb_${input.applicationId}` };
}

// ---- audit (compliance) --------------------------------------------------

export async function recordAudit(event: AuditEvent): Promise<void> {
  // Real impl: append-only write to the compliance store (e.g. immutable S3 /
  // an append-only ledger table). Workflow history is also durable, but the
  // compliance team wants a queryable system of record outside Temporal.
  log.info('AUDIT', { ...event });
}

// ---- notifications -------------------------------------------------------

export async function notifyApplicant(input: {
  applicationId: string;
  email: string;
  template: 'MISSING_DOCS' | 'APPROVED' | 'DECLINED' | 'REMINDER';
  data?: Record<string, unknown>;
}): Promise<void> {
  log.info('NOTIFY', input);
}

/**
 * Creates a task in the ops review queue (a row in a DB the ops UI reads).
 * This is what makes a workflow "appear" in a reviewer's worklist.
 */
export async function createReviewTask(input: {
  applicationId: string;
  product: string;
  reasons: string[];
}): Promise<void> {
  log.info('REVIEW_TASK_CREATED', input);
}

export async function closeReviewTask(input: {
  applicationId: string;
  decision: HumanDecision;
}): Promise<void> {
  log.info('REVIEW_TASK_CLOSED', input);
}

export async function emitOutcome(outcome: LoanOutcome): Promise<void> {
  // Real impl: publish to the billing system. Only FUNDED loans bill.
  log.info('OUTCOME', { ...outcome });
}
