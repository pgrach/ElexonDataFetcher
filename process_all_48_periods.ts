/**
 * Process All 48 Periods for March 28, 2025
 * 
 * This script processes all 48 settlement periods for March 28, 2025, in batches
 * to avoid timeouts. It includes robust error handling and progress tracking.
 * 
 * The script will:
 * 1. Check which periods have already been processed
 * 2. Process the missing periods in small batches
 * 3. Update all summary tables after each batch
 * 4. Report progress throughout the process
 */

import { processDate } from './optimized_critical_date_processor';
import { updateSummaries, updateBitcoinCalculations } from './update_summaries';
import { db } from './db';
import { sql } from 'drizzle-orm';

// Configuration
const TARGET_DATE = '2025-03-28';
const BATCH_SIZE = 5;
const PAUSE_BETWEEN_BATCHES_MS = 2000;

// Color console output
const colors = {
  info: '\x1b[36m',    // Cyan
  success: '\x1b[32m', // Green
  warning: '\x1b[33m', // Yellow
  error: '\x1b[31m',   // Red
  reset: '\x1b[0m'     // Reset
};

/**
 * Log messages with color formatting
 */
function log(message: string, type: 'info' | 'success' | 'warning' | 'error' = 'info'): void {
  const timestamp = new Date().toLocaleTimeString();
  const color = colors[type];
  const icon = type === 'info' ? 'ℹ' : 
               type === 'success' ? '✓' : 
               type === 'warning' ? '⚠' : 
               type === 'error' ? '✗' : '';
               
  console.log(`${color}${icon} [${timestamp}] ${message}${colors.reset}`);
}

/**
 * Utility function to introduce a delay
 */
async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Check which periods already have data
 */
async function getProcessedPeriods(): Promise<Set<number>> {
  try {
    const result = await db.execute(sql`
      SELECT DISTINCT settlement_period
      FROM curtailment_records
      WHERE settlement_date = ${TARGET_DATE}
    `);
    
    const processedPeriods = new Set<number>();
    if (result.rows && result.rows.length > 0) {
      for (const row of result.rows) {
        processedPeriods.add(Number(row.settlement_period));
      }
    }
    
    return processedPeriods;
  } catch (error) {
    log(`Error getting processed periods: ${error}`, 'error');
    return new Set<number>();
  }
}

/**
 * Process a batch of periods
 */
async function processBatch(periods: number[]): Promise<void> {
  if (periods.length === 0) return;
  
  log(`Processing batch: periods ${periods[0]}-${periods[periods.length - 1]}`, 'info');
  
  let processedCount = 0;
  let totalRecords = 0;
  
  for (const period of periods) {
    try {
      log(`Processing period ${period}...`, 'info');
      
      // Process the period with a 3-minute timeout
      const result = await Promise.race([
        processDate(TARGET_DATE, period, period),
        new Promise<null>((_, reject) => 
          setTimeout(() => reject(new Error('Timeout')), 180000)
        )
      ]);
      
      if (result) {
        processedCount++;
        totalRecords += result.totalRecords;
        log(`Period ${period}: Added ${result.totalRecords} records (${result.totalVolume.toFixed(2)} MWh, £${result.totalPayment.toFixed(2)})`, 'success');
      }
    } catch (error) {
      log(`Error processing period ${period}: ${error}`, 'error');
    }
    
    // Short pause between periods
    await delay(500);
  }
  
  log(`Batch ${periods[0]}-${periods[periods.length - 1]} complete:`, 'success');
  log(`- Periods processed: ${processedCount}/${periods.length}`, 'info');
  log(`- Records added: ${totalRecords}`, 'info');
  
  // Update summaries after each batch
  try {
    log('Updating summary tables...', 'info');
    await updateSummaries(TARGET_DATE);
    log('Updating Bitcoin calculations...', 'info');
    await updateBitcoinCalculations(TARGET_DATE);
    log('Summary updates complete', 'success');
  } catch (error) {
    log(`Error updating summaries: ${error}`, 'error');
  }
}

/**
 * Main function to process all periods
 */
async function main(): Promise<void> {
  log(`Starting processing of all 48 periods for ${TARGET_DATE}`, 'info');
  
  // Check what's already processed
  const processedPeriods = await getProcessedPeriods();
  log(`Found ${processedPeriods.size} already processed periods`, 'info');
  
  // Build list of periods to process
  const periodsToProcess: number[] = [];
  for (let period = 1; period <= 48; period++) {
    if (!processedPeriods.has(period)) {
      periodsToProcess.push(period);
    }
  }
  
  log(`${periodsToProcess.length} periods need processing: ${periodsToProcess.join(', ')}`, 'info');
  
  // Process in batches
  for (let i = 0; i < periodsToProcess.length; i += BATCH_SIZE) {
    const batch = periodsToProcess.slice(i, i + BATCH_SIZE);
    await processBatch(batch);
    
    // Pause between batches to avoid rate limiting and allow database to catch up
    if (i + BATCH_SIZE < periodsToProcess.length) {
      log(`Pausing between batches...`, 'info');
      await delay(PAUSE_BETWEEN_BATCHES_MS);
    }
  }
  
  // Final summary update
  try {
    log('Performing final summary updates...', 'info');
    await updateSummaries(TARGET_DATE);
    await updateBitcoinCalculations(TARGET_DATE);
    log('Final updates complete', 'success');
  } catch (error) {
    log(`Error in final summary update: ${error}`, 'error');
  }
  
  // Check final status
  const finalProcessedPeriods = await getProcessedPeriods();
  const coverage = (finalProcessedPeriods.size / 48) * 100;
  
  log(`Processing complete!`, 'success');
  log(`Final coverage: ${finalProcessedPeriods.size}/48 periods (${coverage.toFixed(1)}%)`, 'info');
  
  if (finalProcessedPeriods.size < 48) {
    const missing: number[] = [];
    for (let period = 1; period <= 48; period++) {
      if (!finalProcessedPeriods.has(period)) {
        missing.push(period);
      }
    }
    log(`Missing periods: ${missing.join(', ')}`, 'warning');
  } else {
    log(`All 48 periods have been successfully processed!`, 'success');
  }
}

// Run the script
main().catch(error => {
  log(`Unhandled error: ${error}`, 'error');
  process.exit(1);
});