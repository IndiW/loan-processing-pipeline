/**
 * Each product gets its OWN underwriting workflow, running on its OWN task queue
 * (see searchAttributes.ts -> TASK_QUEUES). That means:
 *   - personal-loan rule changes deploy independently of auto/debt
 *   - a bug or bad deploy in one product can't take down the others
 *   - each product's worker pool scales on its own load
 *   - workflow versioning (`patched`) is scoped per product
 *
 * Rules here are PURE and DETERMINISTIC, so they run safely inside the workflow
 * and are captured verbatim in history. The `rulesetVersion` makes every
 * decision reproducible for an auditor.
 */
import type { EnrichmentBundle, LoanApplication, UnderwritingResult } from '../../shared/types';

const RULESET_VERSION = 'personal-v3';

export interface UnderwritingInput {
  application: LoanApplication;
  enrichment: EnrichmentBundle;
}

export async function personalLoanUnderwriting(
  input: UnderwritingInput,
): Promise<UnderwritingResult> {
  const { application, enrichment } = input;
  const reasons: string[] = [];

  // Missing a critical signal after retries -> refer to a human rather than
  // auto-decline. The orchestrator decides this is non-fatal because it can
  // still get a human decision.
  if (!enrichment.credit) {
    return {
      decision: 'REFER',
      reasons: ['credit_report_unavailable'],
      rulesetVersion: RULESET_VERSION,
    };
  }
  if (!enrichment.identity?.verified) {
    return {
      decision: 'REFER',
      reasons: ['identity_unverified'],
      rulesetVersion: RULESET_VERSION,
    };
  }
  // Approving a loan while the fraud signal is unavailable is the wrong
  // default for a regulated lender; treat a null result (provider down,
  // retries exhausted) the same as missing credit/identity → REFER.
  if (!enrichment.fraud) {
    return {
      decision: 'REFER',
      reasons: ['fraud_screening_unavailable'],
      rulesetVersion: RULESET_VERSION,
    };
  }

  const { credit, fraud } = enrichment;

  if (fraud.riskScore >= 80) {
    reasons.push('high_fraud_risk');
    return { decision: 'DECLINE', reasons, rulesetVersion: RULESET_VERSION };
  }
  if (credit.score < 620) {
    reasons.push('credit_score_below_threshold');
    return { decision: 'DECLINE', reasons, rulesetVersion: RULESET_VERSION };
  }
  if (credit.delinquencies > 0 || application.requestedAmount > 40000) {
    // Borderline -> human review.
    reasons.push(credit.delinquencies > 0 ? 'recent_delinquency' : 'amount_over_auto_limit');
    return { decision: 'REFER', reasons, rulesetVersion: RULESET_VERSION };
  }

  // Auto-approve.
  const interestRate = credit.score >= 720 ? 0.069 : 0.119;
  reasons.push('auto_approved');
  return {
    decision: 'APPROVE',
    reasons,
    rulesetVersion: RULESET_VERSION,
    approvedAmount: application.requestedAmount,
    interestRate,
  };
}
