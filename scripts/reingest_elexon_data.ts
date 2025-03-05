import { processDailyCurtailment } from "../server/services/curtailment";
import { processHistoricalCalculations } from "../server/services/bitcoinService";
import { minerModels } from "../server/types/bitcoin";

// The specific date to process
const TARGET_DATE = '2025-03-03';

async function reingestElexonData() {
  try {
    console.log(`\n=== Starting Elexon Data Reingestion for ${TARGET_DATE} ===\n`);
    
    // Step 1: Reingest curtailment data from Elexon API
    console.log(`Step 1: Reingesting curtailment data from Elexon API for ${TARGET_DATE}`);
    await processDailyCurtailment(TARGET_DATE);
    console.log(`✓ Successfully reingested curtailment data for ${TARGET_DATE}`);
    
    // Step 2: Update Bitcoin calculations for each miner model
    console.log(`\nStep 2: Updating Bitcoin calculations for all miner models`);
    const minerModelList = Object.keys(minerModels);
    
    // Process historical Bitcoin calculations for target date
    await processHistoricalCalculations(TARGET_DATE, TARGET_DATE);
    console.log(`✓ Successfully updated Bitcoin calculations for ${TARGET_DATE}`);
    
    console.log(`\n=== Elexon Data Reingestion Complete for ${TARGET_DATE} ===`);
  } catch (error) {
    console.error(`Error during Elexon data reingestion:`, error);
    process.exit(1);
  }
}

// Execute the reingestion process
reingestElexonData();