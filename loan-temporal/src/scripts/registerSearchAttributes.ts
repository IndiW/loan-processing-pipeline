/**
 * Registers the custom search attributes on the cluster/namespace so the
 * orchestrator's upsertTypedSearchAttributes calls succeed and operators can
 * filter on them in the UI/CLI.
 *
 * On a dev server you can also just pass them to `temporal server start-dev
 * --search-attribute ApplicationStatus=Keyword ...` (see README).
 */
import { Connection } from '@temporalio/client';

async function main() {
  const connection = await Connection.connect({
    address: process.env.TEMPORAL_ADDRESS ?? 'localhost:7233',
  });
  const namespace = process.env.TEMPORAL_NAMESPACE ?? 'default';

  await connection.operatorService.addSearchAttributes({
    namespace,
    searchAttributes: {
      ApplicationStatus: 'INDEXED_VALUE_TYPE_KEYWORD' as never,
      LoanProduct: 'INDEXED_VALUE_TYPE_KEYWORD' as never,
      Channel: 'INDEXED_VALUE_TYPE_KEYWORD' as never,
      AssignedReviewer: 'INDEXED_VALUE_TYPE_KEYWORD' as never,
      SlaDeadline: 'INDEXED_VALUE_TYPE_DATETIME' as never,
      RequestedAmount: 'INDEXED_VALUE_TYPE_DOUBLE' as never,
    },
  });
  console.log('search attributes registered');
  await connection.close();
}

main().catch((e) => {
  // Already-exists errors are safe to ignore on re-run.
  console.error(e.message ?? e);
  process.exit(0);
});
