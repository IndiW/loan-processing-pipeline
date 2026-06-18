/**
 * Ingestion: the ONLY channel-specific code in the system. Each adapter takes
 * raw, channel-shaped input and produces the canonical `LoanApplication`, then
 * starts a workflow. Using `applicationId` as the workflow ID gives us free
 * idempotency: a broker that re-sends the same email, or an aggregator that
 * re-delivers a batch row, will not create a duplicate workflow.
 */
import { Client, Connection } from '@temporalio/client';
import { randomUUID } from 'crypto';
import { loanApplicationWorkflow } from '../workflows/loanApplication.workflow';
import { TASK_QUEUES } from '../shared/searchAttributes';
import type { LoanApplication, Channel } from '../shared/types';

export async function makeClient(): Promise<Client> {
  const connection = await Connection.connect({
    address: process.env.TEMPORAL_ADDRESS ?? 'localhost:7233',
  });
  return new Client({ connection, namespace: process.env.TEMPORAL_NAMESPACE ?? 'default' });
}

/** Start (or no-op if it already exists) a workflow for a normalized application. */
export async function startApplication(client: Client, app: LoanApplication) {
  const handle = await client.workflow.start(loanApplicationWorkflow, {
    args: [app],
    taskQueue: TASK_QUEUES.ORCHESTRATOR,
    workflowId: app.applicationId, // idempotency key
    // If a workflow with this ID is already running, reject the duplicate start
    // instead of starting a second one.
    workflowIdConflictPolicy: 'USE_EXISTING',
  });
  return handle;
}

// ---- channel 1: self-serve online portal --------------------------------
// Synchronous API hands us already-structured data.
export function fromPortal(payload: {
  product: LoanApplication['product'];
  applicant: LoanApplication['applicant'];
  requestedAmount: number;
  documents?: LoanApplication['documents'];
}): LoanApplication {
  return normalize('PORTAL', {
    ...payload,
    documents: payload.documents ?? [],
    sourceMetadata: { origin: 'web-portal' },
  });
}

// ---- channel 2: broker referral via email --------------------------------
// A mail-parsing service extracts fields; we trust less, so metadata records
// the raw message id for audit and dispute resolution.
export function fromBrokerEmail(parsed: {
  brokerId: string;
  messageId: string;
  product: LoanApplication['product'];
  applicant: LoanApplication['applicant'];
  requestedAmount: number;
  documents?: LoanApplication['documents'];
}): LoanApplication {
  return normalize('BROKER_EMAIL', {
    product: parsed.product,
    applicant: parsed.applicant,
    requestedAmount: parsed.requestedAmount,
    documents: parsed.documents ?? [],
    sourceMetadata: { brokerId: parsed.brokerId, messageId: parsed.messageId },
  });
}

// ---- channel 3: aggregator batch feed ------------------------------------
// A batch poller maps each row; the batch is typically driven by a Temporal
// Schedule that fans out one startApplication per row.
export function fromAggregatorRow(row: {
  aggregator: string;
  externalId: string;
  product: LoanApplication['product'];
  applicant: LoanApplication['applicant'];
  requestedAmount: number;
  documents?: LoanApplication['documents'];
}): LoanApplication {
  return normalize('AGGREGATOR_BATCH', {
    product: row.product,
    applicant: row.applicant,
    requestedAmount: row.requestedAmount,
    documents: row.documents ?? [],
    // Deterministic ID from the aggregator's own key -> idempotent re-delivery.
    applicationId: `agg-${row.aggregator}-${row.externalId}`,
    sourceMetadata: { aggregator: row.aggregator, externalId: row.externalId },
  });
}

// ---- normalization -------------------------------------------------------
function normalize(
  channel: Channel,
  input: Partial<LoanApplication> & {
    product: LoanApplication['product'];
    applicant: LoanApplication['applicant'];
    requestedAmount: number;
    documents: LoanApplication['documents'];
    sourceMetadata: Record<string, string>;
  },
): LoanApplication {
  return {
    applicationId: input.applicationId ?? `app-${randomUUID()}`,
    channel,
    product: input.product,
    applicant: input.applicant,
    requestedAmount: input.requestedAmount,
    documents: input.documents,
    sourceMetadata: input.sourceMetadata,
    submittedAt: new Date().toISOString(),
  };
}
