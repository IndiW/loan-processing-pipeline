/**
 * The operator/ops side. The review dashboard would call these:
 *   - getState  : "where is application X right now?" (Query, no history event)
 *   - decide    : push a reviewer's decision IN (Update -> validated, synchronous)
 *   - addDocuments / withdraw : applicant-driven inputs
 */
import type { Client } from '@temporalio/client';
import {
  getState,
  submitHumanDecision,
  submitDocuments,
  withdraw,
} from '../workflows/loanApplication.workflow';
import type { HumanDecision, DocumentRef } from '../shared/types';

export async function getState_(client: Client, applicationId: string) {
  const handle = client.workflow.getHandle(applicationId);
  return handle.query(getState);
}

/**
 * Submit a human decision. Because this is an Update, the call returns only
 * after the workflow's validator accepts it (e.g. rejects an over-authority
 * approval) — the ops UI gets immediate, trustworthy feedback.
 */
export async function decide(client: Client, applicationId: string, decision: HumanDecision) {
  const handle = client.workflow.getHandle(applicationId);
  await handle.executeUpdate(submitHumanDecision, { args: [decision] });
}

export async function addDocuments(client: Client, applicationId: string, documents: DocumentRef[]) {
  const handle = client.workflow.getHandle(applicationId);
  await handle.executeUpdate(submitDocuments, { args: [{ documents }] });
}

export async function withdrawApplication(client: Client, applicationId: string) {
  const handle = client.workflow.getHandle(applicationId);
  await handle.signal(withdraw);
}
