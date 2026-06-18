/**
 * Local demo. With a dev server running (`temporal server start-dev`) and the
 * worker running (`npm run worker`), this:
 *   1. ingests one app from each channel
 *   2. polls state
 *   3. when an app lands in PENDING_HUMAN_REVIEW, submits a human APPROVE
 *
 * Run: npm run demo
 */
import { makeClient, fromPortal, fromBrokerEmail, fromAggregatorRow, startApplication } from './client/ingestion';
import { getState_, decide } from './client/ops';
import type { LoanApplication } from './shared/types';

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function main() {
  const client = await makeClient();

  const apps: LoanApplication[] = [
    fromPortal({
      product: 'PERSONAL',
      requestedAmount: 15000,
      applicant: { fullName: 'Ada Lovelace', email: 'ada@example.com', dateOfBirth: '1985-12-10', annualIncome: 90000 },
      documents: [
        { type: 'ID', uri: 's3://docs/ada-id', receivedAt: new Date().toISOString() },
        { type: 'PROOF_OF_INCOME', uri: 's3://docs/ada-income', receivedAt: new Date().toISOString() },
      ],
    }),
    fromBrokerEmail({
      brokerId: 'broker-42',
      messageId: 'msg-abc',
      product: 'AUTO',
      requestedAmount: 80000, // over auto limit -> will REFER to a human
      applicant: { fullName: 'Alan Turing', email: 'alan@example.com', dateOfBirth: '1980-06-23', annualIncome: 120000 },
      documents: [
        { type: 'ID', uri: 's3://docs/alan-id', receivedAt: new Date().toISOString() },
        { type: 'PROOF_OF_INCOME', uri: 's3://docs/alan-income', receivedAt: new Date().toISOString() },
        { type: 'VEHICLE_TITLE', uri: 's3://docs/alan-title', receivedAt: new Date().toISOString() },
      ],
    }),
    fromAggregatorRow({
      aggregator: 'lendingtree',
      externalId: '99887',
      product: 'DEBT_CONSOLIDATION',
      requestedAmount: 25000,
      applicant: { fullName: 'Grace Hopper', email: 'grace@example.com', dateOfBirth: '1975-12-09', annualIncome: 105000 },
      documents: [
        { type: 'ID', uri: 's3://docs/grace-id', receivedAt: new Date().toISOString() },
        { type: 'PROOF_OF_INCOME', uri: 's3://docs/grace-income', receivedAt: new Date().toISOString() },
        { type: 'BANK_STATEMENT', uri: 's3://docs/grace-bank', receivedAt: new Date().toISOString() },
      ],
    }),
  ];

  for (const app of apps) {
    await startApplication(client, app);
    console.log(`started ${app.applicationId} (${app.channel} / ${app.product})`);
  }

  // Poll, and clear any human reviews by approving them.
  for (let i = 0; i < 30; i++) {
    await sleep(1000);
    for (const app of apps) {
      const state = await getState_(client, app.applicationId);
      console.log(`${app.applicationId}: ${state.status}${state.reasons.length ? ' ' + JSON.stringify(state.reasons) : ''}`);
      if (state.status === 'PENDING_HUMAN_REVIEW') {
        await decide(client, app.applicationId, {
          type: 'APPROVE',
          reviewerId: 'reviewer-7',
          notes: 'manual review ok',
          approvedAmount: app.requestedAmount,
          interestRate: 0.099,
        });
        console.log(`  -> approved ${app.applicationId} by reviewer-7`);
      }
    }
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
