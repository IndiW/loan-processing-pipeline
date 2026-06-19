# Loan Disbursement on Temporal — runnable reference

A Temporal-orchestrated loan pipeline: ingest from 3 channels → validate →
enrich (credit/identity/fraud) → product-isolated underwriting → human review
when referred → fund. See **[ARCHITECTURE.md](../ARCHITECTURE.md)** for the design
write-up, diagrams, and trade-offs.

Built and type-checked against `@temporalio/* 1.18.1` (TypeScript SDK).

## Layout

```
src/
  shared/          types.ts (canonical LoanApplication), searchAttributes.ts (typed SAs, task queues)
  activities/      index.ts — all side effects (providers, funding, audit, notify)
  workflows/
    loanApplication.workflow.ts      ← the orchestrator / state machine
    underwriting/personalLoan.workflow.ts   ← product-isolated child workflow
    underwriting/autoLoan.workflow.ts
  client/          ingestion.ts (3 channel adapters), ops.ts (Query + decision Update)
  worker.ts        all worker roles (orchestrator, per-product, per-provider)
  demo.ts          starts one app per channel and clears any human reviews
  integration.test.ts  end-to-end run against an ephemeral server
```

## Run locally

Prerequisites: Node 18+, and the Temporal CLI
(`brew install temporal` or see https://docs.temporal.io/cli).

```bash
npm install

# 1. start a local dev server WITH the custom search attributes registered
temporal server start-dev \
  --search-attribute ApplicationStatus=Keyword \
  --search-attribute LoanProduct=Keyword \
  --search-attribute Channel=Keyword \
  --search-attribute AssignedReviewer=Keyword \
  --search-attribute SlaDeadline=Datetime \
  --search-attribute RequestedAmount=Double
# UI at http://localhost:8233

# 2. in a second terminal, start the workers
npm run worker

# 3. in a third terminal, run the demo
npm run demo
```

The demo starts a PERSONAL app (auto-approves → FUNDED), an AUTO app over the
auto limit (→ PENDING_HUMAN_REVIEW, which the demo then approves → FUNDED), and a
DEBT app. Watch the state transitions in the console and in the Temporal UI.

### One-shot end-to-end (no separate server)

`src/integration.test.ts` boots an ephemeral server in-process and runs the
whole thing. It requires outbound access to download the dev-server binary
(`temporal.download`); on a normal machine:

```bash
npx ts-node src/integration.test.ts
```

## Verify it compiles

```bash
npm run build   # tsc --noEmit, exits 0
```

## Where the brief's requirements live

| Requirement | File / mechanism |
| --- | --- |
| 3-channel ingestion, idempotent | `client/ingestion.ts` (`workflowId = applicationId`) |
| Lifecycle state machine | `workflows/loanApplication.workflow.ts` |
| Activities + retry/idempotency | `activities/index.ts` + proxy configs in the workflow |
| Human-in-the-loop pause/resume | `submitHumanDecision` Update + `condition()` block |
| Product isolation | `workflows/underwriting/*` on separate task queues |
| Rate limits / partial failure | per-provider queues + `Promise.all(... .catch(() => null))` |
| Observability | `getState` Query + Search Attributes |
| Audit | `recordAudit` activity + workflow history |
```
