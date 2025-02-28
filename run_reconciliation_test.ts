/**
 * Reconciliation System Test
 * 
 * This script tests the unified reconciliation system by performing a sample operation
 * and verifying that it works correctly.
 */

import { 
  getReconciliationStatus, 
  processDate,
  findDatesWithMissingCalculations 
} from './unified_reconciliation';

/**
 * Format a number with thousands separators
 */
function formatNumber(num: number): string {
  return num.toLocaleString();
}

/**
 * Format a percentage
 */
function formatPercentage(num: number): string {
  return num.toFixed(2) + '%';
}

/**
 * Format a date difference to show elapsed time
 */
function formatElapsedTime(startTime: number): string {
  const elapsedMs = Date.now() - startTime;
  if (elapsedMs < 1000) {
    return `${elapsedMs}ms`;
  } else if (elapsedMs < 60000) {
    return `${(elapsedMs / 1000).toFixed(2)}s`;
  } else {
    const minutes = Math.floor(elapsedMs / 60000);
    const seconds = ((elapsedMs % 60000) / 1000).toFixed(1);
    return `${minutes}m ${seconds}s`;
  }
}

/**
 * Run reconciliation test with analysis mode
 */
async function runAnalysisTest() {
  console.log('\n=== Analysis Test ===');
  
  // Get status information
  console.log('Fetching reconciliation status...');
  const startTime = Date.now();
  const status = await getReconciliationStatus();
  
  // Explicitly type the status to fix TypeScript issues
  type ReconciliationStatus = {
    overview: {
      totalRecords: number;
      totalCalculations: number;
      missingCalculations: number;
      completionPercentage: number;
    };
    dateStats: Array<{
      date: string;
      expected: number;
      actual: number;
      missing: number;
      completionPercentage: number;
    }>;
  };
  
  const typedStatus = status as ReconciliationStatus;
  
  console.log(`\nStatus: ${formatPercentage(typedStatus.overview.completionPercentage)} complete`);
  console.log(`Total Records: ${formatNumber(typedStatus.overview.totalRecords)}`);
  console.log(`Total Calculations: ${formatNumber(typedStatus.overview.totalCalculations)}`);
  console.log(`Missing Calculations: ${formatNumber(typedStatus.overview.missingCalculations)}`);
  console.log(`Time: ${formatElapsedTime(startTime)}`);
  
  // Find dates with missing calculations (limit to 5)
  console.log('\nFinding dates with missing calculations...');
  const missingScanTime = Date.now();
  const missingDates = await findDatesWithMissingCalculations(5);
  
  if (missingDates.length === 0) {
    console.log('No dates with missing calculations found.');
  } else {
    console.log(`Found ${missingDates.length} dates with missing calculations:`);
    missingDates.forEach(date => {
      // Extract date string from object if it's an object
      const dateStr = typeof date === 'string' ? date : (date as any).date || '';
      
      const dateStats = typedStatus.dateStats.find(stat => stat.date === dateStr);
      if (dateStats) {
        console.log(`- ${dateStr}: ${dateStats.actual}/${dateStats.expected} (${formatPercentage(dateStats.completionPercentage)})`);
      } else {
        console.log(`- ${dateStr}`);
      }
    });
  }
  console.log(`Time: ${formatElapsedTime(missingScanTime)}`);
}

/**
 * Run reconciliation test with sample processing
 */
async function runProcessingTest() {
  // Process today's date as a test
  const today = new Date().toISOString().split('T')[0];
  
  console.log('\n=== Processing Test ===');
  console.log(`Testing with date ${today}...`);
  
  const processStartTime = Date.now();
  const result = await processDate(today);
  
  if (result.success) {
    console.log(`\n✅ Test completed successfully: ${result.message}`);
  } else {
    console.log(`\n❌ Test failed: ${result.message}`);
  }
  
  console.log(`Time: ${formatElapsedTime(processStartTime)}`);
}

/**
 * Main test function
 */
async function main() {
  try {
    const totalStartTime = Date.now();
    console.log('=== Unified Reconciliation System Test ===');
    
    // Run analysis test
    await runAnalysisTest();
    
    // Run processing test
    await runProcessingTest();
    
    // Show test summary
    console.log('\n=== Test Summary ===');
    console.log(`Total execution time: ${formatElapsedTime(totalStartTime)}`);
    console.log('Test completed successfully.');
  } catch (error) {
    console.error('Error during test execution:', error);
    process.exit(1);
  }
}

// Run the test if executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch(error => {
    console.error('Unhandled error:', error);
    process.exit(1);
  });
}