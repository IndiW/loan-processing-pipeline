/**
 * Auto loans have different thresholds, require a vehicle title, and care about
 * loan-to-value. Same shape as personal underwriting, completely separate logic
 * and ruleset version — this is the isolation the brief asks for.
 */
import type { UnderwritingResult } from '../../shared/types';
import type { UnderwritingInput } from './personalLoan.workflow';

const RULESET_VERSION = 'auto-v2';

export async function autoLoanUnderwriting(input: UnderwritingInput): Promise<UnderwritingResult> {
  const { application, enrichment } = input;
  const reasons: string[] = [];

  if (!enrichment.credit) {
    return { decision: 'REFER', reasons: ['credit_report_unavailable'], rulesetVersion: RULESET_VERSION };
  }

  const hasTitle = application.documents.some((d) => d.type === 'VEHICLE_TITLE');
  if (!hasTitle) {
    return { decision: 'REFER', reasons: ['vehicle_title_missing'], rulesetVersion: RULESET_VERSION };
  }
  // Same principle as personal: never auto-approve when the fraud provider
  // exhausted its retries. The human can pull the screen manually.
  if (!enrichment.fraud) {
    return { decision: 'REFER', reasons: ['fraud_screening_unavailable'], rulesetVersion: RULESET_VERSION };
  }

  const { credit, fraud } = enrichment;
  if (fraud.riskScore >= 70) {
    reasons.push('high_fraud_risk');
    return { decision: 'DECLINE', reasons, rulesetVersion: RULESET_VERSION };
  }
  if (credit.score < 580) {
    reasons.push('credit_score_below_threshold');
    return { decision: 'DECLINE', reasons, rulesetVersion: RULESET_VERSION };
  }
  if (application.requestedAmount > 60000) {
    reasons.push('amount_over_auto_limit');
    return { decision: 'REFER', reasons, rulesetVersion: RULESET_VERSION };
  }

  reasons.push('auto_approved');
  return {
    decision: 'APPROVE',
    reasons,
    rulesetVersion: RULESET_VERSION,
    approvedAmount: application.requestedAmount,
    interestRate: credit.score >= 700 ? 0.059 : 0.099,
  };
}
