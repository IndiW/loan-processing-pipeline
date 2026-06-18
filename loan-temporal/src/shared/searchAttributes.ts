/**
 * Typed Search Attributes (SDK >= 1.12) are how operators find work without
 * querying every workflow individually. Once these are registered on the
 * namespace (see README), the Temporal UI / CLI can answer questions like:
 *
 *   "show me every AUTO application stuck in PENDING_HUMAN_REVIEW for >2h"
 *   ApplicationStatus = "PENDING_HUMAN_REVIEW" AND LoanProduct = "AUTO"
 *
 * These are mirrored from the workflow whenever its status changes. The
 * workflow event history remains the source of truth; search attributes are
 * the operational index on top of it.
 */
import { defineSearchAttributeKey } from '@temporalio/common';

export const SA_STATUS = defineSearchAttributeKey('ApplicationStatus', 'KEYWORD');
export const SA_PRODUCT = defineSearchAttributeKey('LoanProduct', 'KEYWORD');
export const SA_CHANNEL = defineSearchAttributeKey('Channel', 'KEYWORD');
export const SA_REVIEWER = defineSearchAttributeKey('AssignedReviewer', 'KEYWORD');
export const SA_SLA_DEADLINE = defineSearchAttributeKey('SlaDeadline', 'DATETIME');
export const SA_AMOUNT = defineSearchAttributeKey('RequestedAmount', 'DOUBLE');

/** Names used when registering attributes against a dev server / cluster. */
export const SEARCH_ATTRIBUTE_KEYS = [
  SA_STATUS,
  SA_PRODUCT,
  SA_CHANNEL,
  SA_REVIEWER,
  SA_SLA_DEADLINE,
  SA_AMOUNT,
];

export const TASK_QUEUES = {
  /** Orchestrator + generic activities. */
  ORCHESTRATOR: 'loan-orchestrator',
  /** Per-product underwriting workers — deployed & versioned independently. */
  UNDERWRITING_PERSONAL: 'underwriting-personal',
  UNDERWRITING_AUTO: 'underwriting-auto',
  UNDERWRITING_DEBT: 'underwriting-debt-consolidation',
  /** Per-provider queues so each provider's rate limit is enforced in one place. */
  PROVIDER_CREDIT: 'provider-credit',
  PROVIDER_IDENTITY: 'provider-identity',
  PROVIDER_FRAUD: 'provider-fraud',
} as const;
