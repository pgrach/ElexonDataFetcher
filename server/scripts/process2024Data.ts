import { fetch2024Difficulties } from '../services/bitcoinService';
import { updateHistoricalCalculations } from './updateHistoricalCalculations';

async function process2024Data() {
  try {
    console.log('\n=== Phase 1: Prefetching 2024 Difficulty Data ===');
    await fetch2024Difficulties();
    
    console.log('\n=== Phase 2: Processing Historical Calculations ===');
    await updateHistoricalCalculations();
    
  } catch (error) {
    console.error('Error in process2024Data:', error);
    process.exit(1);
  }
}

// Start the process
process2024Data();
