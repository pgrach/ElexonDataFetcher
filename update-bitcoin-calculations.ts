import { processSingleDay } from "./server/services/bitcoinService";

const targetDate = "2025-03-04";
const minerModels = ["S19J_PRO", "S9", "M20S"];

async function main() {
  console.log(`Updating Bitcoin calculations for ${targetDate}...`);
  
  try {
    // Process Bitcoin calculations for each miner model
    for (const minerModel of minerModels) {
      console.log(`Processing model: ${minerModel}`);
      await processSingleDay(targetDate, minerModel)
        .catch(error => {
          console.error(`Error processing Bitcoin calculations for ${targetDate} with ${minerModel}:`, error);
        });
    }
    
    console.log(`Successfully updated Bitcoin calculations for ${targetDate}`);
  } catch (error) {
    console.error(`Error updating Bitcoin calculations for ${targetDate}:`, error);
    process.exit(1);
  }
}

main();