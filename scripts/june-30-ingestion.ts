import { processDailyCurtailment } from '../server/services/curtailmentService.js';

async function main() {
  try {
    console.log('=== JUNE 30, 2025 COMPREHENSIVE INGESTION ===');
    console.log('Starting data ingestion using the proven curtailment service...');
    
    const result = await processDailyCurtailment('2025-06-30');
    
    console.log('\n=== INGESTION RESULTS ===');
    console.log('Result:', result);
    
    console.log('\n✅ INGESTION COMPLETE');
    console.log('June 30, 2025 data has been successfully processed');
    
  } catch (error) {
    console.error('\n❌ INGESTION FAILED:', error);
  }
}

main();