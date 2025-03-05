/**
 * Script to reprocess missing data for March 3, 2025
 * Focusing specifically on the hour 15 (settlement period 30)
 */
import { processDailyCurtailment } from "./server/services/curtailment";
import { processDate } from "./server/services/historicalReconciliation";

async function main() {
  try {
    const date = "2025-03-03";
    
    console.log(`Starting reprocessing of data for ${date}`);
    
    // Step 1: Reprocess curtailment data for the entire day
    // This will fetch data from Elexon API and update curtailment_records
    await processDailyCurtailment(date);
    
    // Step 2: Reprocess Bitcoin calculations for this date
    // This will ensure that all Bitcoin calculations are created based on the updated curtailment data
    const result = await processDate(date);
    
    console.log(`Reprocessing completed for ${date}:`);
    console.log(result);
    
    process.exit(0);
  } catch (error) {
    console.error('Error during reprocessing:', error);
    process.exit(1);
  }
}

main();