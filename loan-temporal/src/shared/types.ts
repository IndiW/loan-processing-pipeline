/**
 * Canonical domain types shared between workflows, activities, clients and the API.
 *
 * The whole point of the `LoanApplication` type is that all three ingestion
 * channels (portal, broker email, aggregator batch) normalise their raw input
 * into THIS shape before a workflow is ever started. The core workflow never
 * knows or cares which channel an application came from.
 */

export type Channel = 'PORTAL' | 'BROKER_EMAIL' | 'AGGREGATOR_BATCH';

export type LoanProduct = 'PERSONAL' | 'AUTO' | 'DEBT_CONSOLIDATION';

/**
 * The terminal + non-terminal states an application can be in.
 * This enum IS the state machine; the workflow code drives transitions and
 * mirrors the current value into a Search Attribute so operators can query it.
 */
export type ApplicationStatus =
  | 'INGESTED'
  | 'VALIDATING'
  | 'AWAITING_DOCUMENTS'
  | 'ENRICHING'
  | 'UNDERWRITING'
  | 'PENDING_HUMAN_REVIEW'
  | 'FUNDING'
  // terminal states
  | 'FUNDED'
  | 'DECLINED'
  | 'WITHDRAWN'
  | 'EXPIRED';

export const TERMINAL_STATES: ApplicationStatus[] = [
  'FUNDED',
  'DECLINED',
  'WITHDRAWN',
  'EXPIRED',
];

export interface Applicant {
  fullName: string;
  email: string;
  dateOfBirth: string; // ISO date
  ssnLast4?: string; // never store full SSN in workflow history; see codec note in README
  annualIncome: number;
}

export interface LoanApplication {
  applicationId: string; // used AS the Temporal workflow ID -> natural idempotency
  channel: Channel;
  product: LoanProduct;
  applicant: Applicant;
  requestedAmount: number;
  /** References to documents in object storage. We store references, never blobs. */
  documents: DocumentRef[];
  /** Free-form, channel-specific metadata kept for audit only. */
  sourceMetadata: Record<string, string>;
  submittedAt: string; // ISO timestamp
}

export interface DocumentRef {
  type: 'ID' | 'PROOF_OF_INCOME' | 'BANK_STATEMENT' | 'VEHICLE_TITLE' | 'OTHER';
  uri: string; // s3://... or gs://...
  receivedAt: string;
}

// ---- Enrichment ----------------------------------------------------------

export interface CreditReport {
  score: number;
  openTradelines: number;
  delinquencies: number;
  bureau: string;
  pulledAt: string;
}

export interface IdentityResult {
  verified: boolean;
  matchScore: number;
  provider: string;
}

export interface FraudResult {
  riskScore: number; // 0-100, higher = riskier
  signals: string[];
  provider: string;
}

/**
 * Enrichment is a partial-failure-tolerant bundle. `null` means the provider
 * could not be reached after retries; underwriting decides whether the missing
 * piece is fatal (escalate) or tolerable (proceed degraded).
 */
export interface EnrichmentBundle {
  credit: CreditReport | null;
  identity: IdentityResult | null;
  fraud: FraudResult | null;
}

// ---- Underwriting --------------------------------------------------------

export type Decision = 'APPROVE' | 'DECLINE' | 'REFER';

/**
 * Result of running a product's underwriting rules. `REFER` means "a human must
 * look at this" — the orchestrator will then pause for a human decision.
 */
export interface UnderwritingResult {
  decision: Decision;
  /** Machine-readable reasons; every one is written to the audit log. */
  reasons: string[];
  /** Versioned so the audit log can reproduce exactly which ruleset ran. */
  rulesetVersion: string;
  approvedAmount?: number;
  interestRate?: number;
}

// ---- Human-in-the-loop ---------------------------------------------------

export type HumanDecisionType = 'APPROVE' | 'DECLINE' | 'REQUEST_DOCUMENTS';

export interface HumanDecision {
  type: HumanDecisionType;
  reviewerId: string;
  notes: string;
  /** For APPROVE: the amount/rate the reviewer signed off on. */
  approvedAmount?: number;
  interestRate?: number;
  /** For REQUEST_DOCUMENTS: which docs are still needed. */
  requestedDocuments?: DocumentRef['type'][];
}

export interface DocumentSubmission {
  documents: DocumentRef[];
}

// ---- Audit ---------------------------------------------------------------

export interface AuditEvent {
  applicationId: string;
  at: string;
  actor: string; // "system:underwriting" | "human:<reviewerId>" | provider name
  event: string;
  status: ApplicationStatus;
  detail: Record<string, unknown>;
}

// ---- Final workflow result ----------------------------------------------

export interface LoanOutcome {
  applicationId: string;
  status: Extract<ApplicationStatus, 'FUNDED' | 'DECLINED' | 'WITHDRAWN' | 'EXPIRED'>;
  fundedAmount?: number;
  disbursementId?: string;
  reasons: string[];
}
