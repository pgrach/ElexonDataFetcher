/**
 * Reconciliation System Test
 * 
 * This script tests the unified reconciliation system by performing a sample operation
 * and verifying that it works correctly.
 */

import { getReconciliationStatus, processDate } from './unified_reconciliation';

async function main() {
  try {
    console.log('=== Reconciliation System Test ===');
    
    // Get current status
    console.log('\nStep 1: Checking current reconciliation status...');
    const status = await getReconciliationStatus();
    
    // Process today's date as a test
    const today = new Date().toISOString().split('T')[0];
    console.log(`\nStep 2: Testing with today's date (${today})...`);
    
    const result = await processDate(today);
    
    if (result.success) {
      console.log(`\n✅ Test completed successfully: ${result.message}`);
    } else {
      console.log(`\n❌ Test failed: ${result.message}`);
    }
    
    console.log('\nTest execution completed.');
  } catch (error) {
    console.error('Error during test execution:', error);
    process.exit(1);
  }
}

main().catch(error => {
  console.error('Unhandled error:', error);
  process.exit(1);
});