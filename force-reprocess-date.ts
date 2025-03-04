import { processDailyCurtailment } from "./server/services/curtailment";
import { processSingleDay } from "./server/services/bitcoinService";

const targetDate = "2025-03-03";
const minerModels = ["S19J_PRO", "S9", "M20S"];

async function main() {
  console.log(`Starting forced reprocessing for ${targetDate}...`);
  
  try {
    // Force reprocessing of curtailment data
    console.log(`Reprocessing curtailment records for ${targetDate}`);
    await processDailyCurtailment(targetDate);
    
    // Update Bitcoin calculations for all miner models
    console.log(`Updating Bitcoin calculations for ${targetDate}...`);
    for (const minerModel of minerModels) {
      await processSingleDay(targetDate, minerModel)
        .catch(error => {
          console.error(`Error processing Bitcoin calculations for ${targetDate} with ${minerModel}:`, error);
        });
    }
    
    console.log(`Successfully reprocessed data for ${targetDate}`);
  } catch (error) {
    console.error(`Error reprocessing data for ${targetDate}:`, error);
    process.exit(1);
  }
}

main();