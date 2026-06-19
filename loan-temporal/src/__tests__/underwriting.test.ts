/**
 * Unit tests for underwriting rulesets.
 * Child workflows are pure async functions — testable without Temporal.
 */
import { personalLoanUnderwriting } from '../workflows/underwriting/personalLoan.workflow';
import { autoLoanUnderwriting } from '../workflows/underwriting/autoLoan.workflow';
import type { LoanApplication, EnrichmentBundle } from '../shared/types';

const baseApp: LoanApplication = {
  applicationId: 'app-uw-test',
  channel: 'PORTAL',
  product: 'PERSONAL',
  applicant: {
    fullName: 'Ada Lovelace',
    email: 'ada@example.com',
    dateOfBirth: '1985-12-10',
    annualIncome: 90_000,
  },
  requestedAmount: 15_000,
  documents: [
    { type: 'ID', uri: 's3://id', receivedAt: '2024-01-01T00:00:00Z' },
    { type: 'PROOF_OF_INCOME', uri: 's3://income', receivedAt: '2024-01-01T00:00:00Z' },
  ],
  sourceMetadata: {},
  submittedAt: '2024-01-01T00:00:00Z',
};

const goodEnrichment: EnrichmentBundle = {
  credit: { score: 750, openTradelines: 5, delinquencies: 0, bureau: 'Equifax', pulledAt: '2024-01-01T00:00:00Z' },
  identity: { verified: true, matchScore: 95, provider: 'Onfido' },
  fraud: { riskScore: 5, signals: [], provider: 'Sift' },
};

// ── PERSONAL LOAN ──────────────────────────────────────────────────────────

describe('personalLoanUnderwriting', () => {
  it('auto-approves a clean application with score ≥ 720 at the low rate', async () => {
    const result = await personalLoanUnderwriting({ application: baseApp, enrichment: goodEnrichment });
    expect(result.decision).toBe('APPROVE');
    expect(result.interestRate).toBe(0.069);
    expect(result.approvedAmount).toBe(baseApp.requestedAmount);
    expect(result.rulesetVersion).toBe('personal-v3');
  });

  it('auto-approves with higher rate for score 620-719', async () => {
    const enrichment: EnrichmentBundle = {
      ...goodEnrichment,
      credit: { ...goodEnrichment.credit!, score: 680 },
    };
    const result = await personalLoanUnderwriting({ application: baseApp, enrichment });
    expect(result.decision).toBe('APPROVE');
    expect(result.interestRate).toBe(0.119);
  });

  it('declines when credit score is below 620', async () => {
    const enrichment: EnrichmentBundle = {
      ...goodEnrichment,
      credit: { ...goodEnrichment.credit!, score: 580 },
    };
    const result = await personalLoanUnderwriting({ application: baseApp, enrichment });
    expect(result.decision).toBe('DECLINE');
    expect(result.reasons).toContain('credit_score_below_threshold');
  });

  it('declines when fraud risk score is ≥ 80', async () => {
    const enrichment: EnrichmentBundle = {
      ...goodEnrichment,
      fraud: { riskScore: 85, signals: ['velocity_anomaly'], provider: 'Sift' },
    };
    const result = await personalLoanUnderwriting({ application: baseApp, enrichment });
    expect(result.decision).toBe('DECLINE');
    expect(result.reasons).toContain('high_fraud_risk');
  });

  it('refers when credit report is unavailable', async () => {
    const enrichment: EnrichmentBundle = { ...goodEnrichment, credit: null };
    const result = await personalLoanUnderwriting({ application: baseApp, enrichment });
    expect(result.decision).toBe('REFER');
    expect(result.reasons).toContain('credit_report_unavailable');
  });

  it('refers when identity is unverified', async () => {
    const enrichment: EnrichmentBundle = {
      ...goodEnrichment,
      identity: { verified: false, matchScore: 30, provider: 'Onfido' },
    };
    const result = await personalLoanUnderwriting({ application: baseApp, enrichment });
    expect(result.decision).toBe('REFER');
    expect(result.reasons).toContain('identity_unverified');
  });

  it('refers when the fraud provider is unavailable (null fraud)', async () => {
    const enrichment: EnrichmentBundle = { ...goodEnrichment, fraud: null };
    const result = await personalLoanUnderwriting({ application: baseApp, enrichment });
    expect(result.decision).toBe('REFER');
    expect(result.reasons).toContain('fraud_screening_unavailable');
  });

  it('refers when there are delinquencies on credit report', async () => {
    const enrichment: EnrichmentBundle = {
      ...goodEnrichment,
      credit: { ...goodEnrichment.credit!, score: 700, delinquencies: 1 },
    };
    const result = await personalLoanUnderwriting({ application: baseApp, enrichment });
    expect(result.decision).toBe('REFER');
    expect(result.reasons).toContain('recent_delinquency');
  });

  it('refers when requested amount exceeds $40,000', async () => {
    const app: LoanApplication = { ...baseApp, requestedAmount: 45_000 };
    const result = await personalLoanUnderwriting({ application: app, enrichment: goodEnrichment });
    expect(result.decision).toBe('REFER');
    expect(result.reasons).toContain('amount_over_auto_limit');
  });

  it('prefers fraud decline over refer for high-risk score', async () => {
    const enrichment: EnrichmentBundle = {
      ...goodEnrichment,
      fraud: { riskScore: 95, signals: ['account_takeover'], provider: 'Sift' },
    };
    const result = await personalLoanUnderwriting({ application: baseApp, enrichment });
    expect(result.decision).toBe('DECLINE');
  });
});

// ── AUTO LOAN ─────────────────────────────────────────────────────────────

describe('autoLoanUnderwriting', () => {
  const autoApp: LoanApplication = {
    ...baseApp,
    product: 'AUTO',
    requestedAmount: 25_000,
    documents: [
      { type: 'ID', uri: 's3://id', receivedAt: '2024-01-01T00:00:00Z' },
      { type: 'PROOF_OF_INCOME', uri: 's3://income', receivedAt: '2024-01-01T00:00:00Z' },
      { type: 'VEHICLE_TITLE', uri: 's3://title', receivedAt: '2024-01-01T00:00:00Z' },
    ],
  };

  it('auto-approves a clean application with score ≥ 700 at the low rate', async () => {
    const result = await autoLoanUnderwriting({ application: autoApp, enrichment: goodEnrichment });
    expect(result.decision).toBe('APPROVE');
    expect(result.interestRate).toBe(0.059);
    expect(result.rulesetVersion).toBe('auto-v2');
  });

  it('auto-approves with higher rate for score 580-699', async () => {
    const enrichment: EnrichmentBundle = {
      ...goodEnrichment,
      credit: { ...goodEnrichment.credit!, score: 640 },
    };
    const result = await autoLoanUnderwriting({ application: autoApp, enrichment });
    expect(result.decision).toBe('APPROVE');
    expect(result.interestRate).toBe(0.099);
  });

  it('declines when credit score is below 580', async () => {
    const enrichment: EnrichmentBundle = {
      ...goodEnrichment,
      credit: { ...goodEnrichment.credit!, score: 550 },
    };
    const result = await autoLoanUnderwriting({ application: autoApp, enrichment });
    expect(result.decision).toBe('DECLINE');
    expect(result.reasons).toContain('credit_score_below_threshold');
  });

  it('declines when fraud risk score is ≥ 70', async () => {
    const enrichment: EnrichmentBundle = {
      ...goodEnrichment,
      fraud: { riskScore: 75, signals: [], provider: 'Sift' },
    };
    const result = await autoLoanUnderwriting({ application: autoApp, enrichment });
    expect(result.decision).toBe('DECLINE');
    expect(result.reasons).toContain('high_fraud_risk');
  });

  it('refers when credit report is unavailable', async () => {
    const enrichment: EnrichmentBundle = { ...goodEnrichment, credit: null };
    const result = await autoLoanUnderwriting({ application: autoApp, enrichment });
    expect(result.decision).toBe('REFER');
    expect(result.reasons).toContain('credit_report_unavailable');
  });

  it('refers when vehicle title is missing', async () => {
    const noTitle: LoanApplication = {
      ...autoApp,
      documents: autoApp.documents.filter((d) => d.type !== 'VEHICLE_TITLE'),
    };
    const result = await autoLoanUnderwriting({ application: noTitle, enrichment: goodEnrichment });
    expect(result.decision).toBe('REFER');
    expect(result.reasons).toContain('vehicle_title_missing');
  });

  it('refers when the fraud provider is unavailable (null fraud)', async () => {
    const enrichment: EnrichmentBundle = { ...goodEnrichment, fraud: null };
    const result = await autoLoanUnderwriting({ application: autoApp, enrichment });
    expect(result.decision).toBe('REFER');
    expect(result.reasons).toContain('fraud_screening_unavailable');
  });

  it('refers when amount exceeds $60,000', async () => {
    const app: LoanApplication = { ...autoApp, requestedAmount: 65_000 };
    const result = await autoLoanUnderwriting({ application: app, enrichment: goodEnrichment });
    expect(result.decision).toBe('REFER');
    expect(result.reasons).toContain('amount_over_auto_limit');
  });

  it('accepts fraud risk score of exactly 69 (below threshold)', async () => {
    const enrichment: EnrichmentBundle = {
      ...goodEnrichment,
      fraud: { riskScore: 69, signals: [], provider: 'Sift' },
    };
    const result = await autoLoanUnderwriting({ application: autoApp, enrichment });
    expect(result.decision).toBe('APPROVE');
  });
});
