import { processDailyCurtailment } from "../services/curtailment";
import { reconcileDay } from "../services/historicalReconciliation";
import { format } from "date-fns";

async function reprocessDay(dateStr: string) {
  try {
    console.log(`\n=== Starting Data Re-processing for ${dateStr} ===`);

    // Use the reconciliation process to check and update if needed
    await reconcileDay(dateStr);

    console.log(`\n=== Data Re-processing Complete for ${dateStr} ===\n`);
  } catch (error) {
    console.error('Error during re-processing:', error);
    process.exit(1);
  }
}

const dateToProcess = process.argv[2];

if (!dateToProcess || !dateToProcess.match(/^\d{4}-\d{2}-\d{2}$/)) {
  console.error('Please provide date in YYYY-MM-DD format');
  console.error('Example: npm run reprocess-day 2024-04-16');
  process.exit(1);
}

reprocessDay(dateToProcess);