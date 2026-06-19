/**
 * Unit tests for activities.
 * Activities are plain async functions — no Temporal infrastructure required.
 * We test validateApplication (the only activity with business-logic branching).
 */
import { validateApplication } from '../activities';
import { ApplicationFailure } from '@temporalio/common';
import type { LoanApplication } from '../shared/types';

const baseApp: LoanApplication = {
  applicationId: 'app-test-1',
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

describe('validateApplication', () => {
  it('passes a fully valid PERSONAL application', async () => {
    const result = await validateApplication(baseApp);
    expect(result.ok).toBe(true);
    expect(result.missingDocuments).toHaveLength(0);
    expect(result.errors).toHaveLength(0);
  });

  it('reports missing ID document', async () => {
    const app: LoanApplication = {
      ...baseApp,
      documents: [{ type: 'PROOF_OF_INCOME', uri: 's3://income', receivedAt: '2024-01-01T00:00:00Z' }],
    };
    const result = await validateApplication(app);
    expect(result.ok).toBe(false);
    expect(result.missingDocuments).toContain('ID');
  });

  it('reports missing PROOF_OF_INCOME document', async () => {
    const app: LoanApplication = {
      ...baseApp,
      documents: [{ type: 'ID', uri: 's3://id', receivedAt: '2024-01-01T00:00:00Z' }],
    };
    const result = await validateApplication(app);
    expect(result.ok).toBe(false);
    expect(result.missingDocuments).toContain('PROOF_OF_INCOME');
  });

  it('passes AUTO application with all required docs', async () => {
    const app: LoanApplication = {
      ...baseApp,
      product: 'AUTO',
      documents: [
        { type: 'ID', uri: 's3://id', receivedAt: '2024-01-01T00:00:00Z' },
        { type: 'PROOF_OF_INCOME', uri: 's3://income', receivedAt: '2024-01-01T00:00:00Z' },
        { type: 'VEHICLE_TITLE', uri: 's3://title', receivedAt: '2024-01-01T00:00:00Z' },
      ],
    };
    const result = await validateApplication(app);
    expect(result.ok).toBe(true);
  });

  it('reports VEHICLE_TITLE missing for AUTO application', async () => {
    const app: LoanApplication = {
      ...baseApp,
      product: 'AUTO',
      documents: [
        { type: 'ID', uri: 's3://id', receivedAt: '2024-01-01T00:00:00Z' },
        { type: 'PROOF_OF_INCOME', uri: 's3://income', receivedAt: '2024-01-01T00:00:00Z' },
      ],
    };
    const result = await validateApplication(app);
    expect(result.ok).toBe(false);
    expect(result.missingDocuments).toContain('VEHICLE_TITLE');
  });

  it('requires BANK_STATEMENT for DEBT_CONSOLIDATION', async () => {
    const app: LoanApplication = {
      ...baseApp,
      product: 'DEBT_CONSOLIDATION',
      documents: [
        { type: 'ID', uri: 's3://id', receivedAt: '2024-01-01T00:00:00Z' },
        { type: 'PROOF_OF_INCOME', uri: 's3://income', receivedAt: '2024-01-01T00:00:00Z' },
      ],
    };
    const result = await validateApplication(app);
    expect(result.ok).toBe(false);
    expect(result.missingDocuments).toContain('BANK_STATEMENT');
  });

  it('throws non-retryable ApplicationFailure for zero amount', async () => {
    const app: LoanApplication = { ...baseApp, requestedAmount: 0 };
    await expect(validateApplication(app)).rejects.toMatchObject({
      type: 'ValidationError',
      nonRetryable: true,
    });
  });

  it('throws non-retryable ApplicationFailure for negative amount', async () => {
    const app: LoanApplication = { ...baseApp, requestedAmount: -5000 };
    await expect(validateApplication(app)).rejects.toMatchObject({
      type: 'ValidationError',
      nonRetryable: true,
    });
  });

  it('throws non-retryable ApplicationFailure for invalid email', async () => {
    const app: LoanApplication = {
      ...baseApp,
      applicant: { ...baseApp.applicant, email: 'not-an-email' },
    };
    await expect(validateApplication(app)).rejects.toMatchObject({
      type: 'ValidationError',
      nonRetryable: true,
    });
  });
});
