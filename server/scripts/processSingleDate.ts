import { format } from 'date-fns';
import { minerModels } from '../types/bitcoin';
import { processSingleDay, prefetchDifficultyData } from '../services/bitcoinService';
import { db } from "@db";

// Get command line arguments
const date = process.argv[2];
const minerModel = process.argv[3];

if (!date || !minerModel) {
  console.error('Usage: npm run process-date <YYYY-MM-DD> <miner_model>');
  process.exit(1);
}

if (!minerModels[minerModel]) {
  console.error(`Invalid miner model. Valid models: ${Object.keys(minerModels).join(', ')}`);
  process.exit(1);
}

async function processDate() {
  try {
    console.log(`Processing date ${date} for model ${minerModel}`);
    
    // Prefetch difficulty data
    await prefetchDifficultyData([date]);

    // Process the date
    await processSingleDay(date, minerModel);
    
    console.log('Processing complete');
  } catch (error) {
    console.error('Error processing date:', error);
    process.exit(1);
  }
}

processDate();
