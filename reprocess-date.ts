import { reconcileDay } from "./server/services/historicalReconciliation";

const targetDate = "2025-03-02";

async function main() {
  console.log(`Starting reprocessing for ${targetDate}...`);
  
  try {
    await reconcileDay(targetDate);
    console.log(`Successfully reprocessed data for ${targetDate}`);
  } catch (error) {
    console.error(`Error reprocessing data for ${targetDate}:`, error);
    process.exit(1);
  }
}

main();