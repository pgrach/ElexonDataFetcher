import { processDailyCurtailment } from "../services/curtailment";
import { format } from "date-fns";

// This script is kept alongside ingestMonthlyData.ts to provide flexibility in data processing:
// - Use this script for quick single-day reprocessing
// - Use ingestMonthlyData.ts for processing multiple days or entire months
// This is particularly useful when dealing with historical data (e.g., 2022) or fixing specific days

async function reprocessDay(dateStr: string) {
  try {
    console.log(`\n=== Starting Data Re-processing for ${dateStr} ===`);

    await processDailyCurtailment(dateStr);

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