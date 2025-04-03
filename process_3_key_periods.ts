/**
 * Process 3 Key Periods for March 28, 2025
 * 
 * This script specifically targets 3 key periods that have been successfully
 * processed before: periods 11, 25, and 37.
 */

import { processDate } from './optimized_critical_date_processor';
import { updateSummaries, updateBitcoinCalculations } from './update_summaries';
import { db } from './db';
import { sql } from 'drizzle-orm';

// Configuration
const TARGET_DATE = '2025-03-28';
const KEY_PERIODS = [11, 25, 37]; // Morning, afternoon, evening

/**
 * Utility function to introduce a delay
 */
async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Process a single period with timeout protection
 */
async function processSinglePeriod(period: number): Promise<boolean> {
  console.log(`Processing period ${period}...`);
  
  try {
    // Process with timeout protection
    const timeoutPromise = new Promise<null>((_, reject) => {
      setTimeout(() => reject(new Error(`Timeout processing period ${period}`)), 120000);
    });
    
    const result = await Promise.race([
      processDate(TARGET_DATE, period, period),
      timeoutPromise
    ]);
    
    if (result) {
      console.log(`Period ${period} completed:
      - Records: ${result.recordsAdded}
      - Volume: ${result.totalVolume ? result.totalVolume.toFixed(2) : "N/A"} MWh
      - Payment: £${result.totalPayment ? result.totalPayment.toFixed(2) : "N/A"}`);
      return true;
    }
    return false;
  } catch (error) {
    console.error(`Error processing period ${period}: ${error}`);
    return false;
  }
}

/**
 * Check current status of settlement periods
 */
async function checkPeriodStatus(): Promise<void> {
  try {
    const result = await db.execute(sql`
      SELECT 
        settlement_period,
        COUNT(*) AS record_count,
        SUM(volume) AS total_volume,
        SUM(payment) AS total_payment
      FROM curtailment_records 
      WHERE settlement_date = ${TARGET_DATE}
      GROUP BY settlement_period
      ORDER BY settlement_period ASC
    `);
    
    if (!result.rows || result.rows.length === 0) {
      console.log(`No data found for ${TARGET_DATE}`);
      return;
    }
    
    console.log(`\nCurrent Status for ${TARGET_DATE}:`);
    console.log(`Period | Records | Volume (MWh) | Payment (£)`);
    console.log(`-------|---------|--------------|------------`);
    
    const populatedPeriods = new Set<number>();
    
    let totalRecords = 0;
    let totalVolume = 0;
    let totalPayment = 0;
    
    for (const period of result.rows) {
      const periodNum = Number(period.settlement_period);
      const records = Number(period.record_count);
      const volume = Number(period.total_volume);
      const payment = Number(period.total_payment);
      
      populatedPeriods.add(periodNum);
      totalRecords += records;
      totalVolume += volume;
      totalPayment += payment;
      
      console.log(`${periodNum.toString().padStart(6, ' ')} | ${records.toString().padStart(7, ' ')} | ${volume.toFixed(2).padStart(12, ' ')} | ${payment.toFixed(2).padStart(10, ' ')}`);
    }
    
    console.log(`-------|---------|--------------|------------`);
    console.log(`Total  | ${totalRecords.toString().padStart(7, ' ')} | ${totalVolume.toFixed(2).padStart(12, ' ')} | ${totalPayment.toFixed(2).padStart(10, ' ')}`);
    
    // Calculate coverage
    const coverage = (populatedPeriods.size / 48) * 100;
    console.log(`\nPeriods with data: ${populatedPeriods.size}/48 (${coverage.toFixed(1)}% coverage)`);
  } catch (error) {
    console.error(`Error checking period status: ${error}`);
  }
}

/**
 * Process the key periods and update all related summary tables
 */
async function processKeyPeriods(): Promise<void> {
  console.log(`\n=== Processing Key Periods for ${TARGET_DATE} ===`);
  console.log(`Started at: ${new Date().toISOString()}`);
  console.log(`Targeting ${KEY_PERIODS.length} periods: ${KEY_PERIODS.join(', ')}\n`);

  // Check initial status
  await checkPeriodStatus();
  
  // Process each period one by one
  let successCount = 0;
  for (const period of KEY_PERIODS) {
    try {
      console.log(`\nProcessing period ${period}...`);
      const success = await processSinglePeriod(period);
      if (success) {
        successCount++;
        // Update after each successful period to ensure data is saved
        console.log(`Updating summaries after period ${period}...`);
        await updateSummaries(TARGET_DATE);
        await delay(2000); // Brief pause to allow database to catch up
      }
    } catch (error) {
      console.error(`Error processing period ${period}: ${error}`);
    }
  }

  // Final updates
  if (successCount > 0) {
    try {
      console.log('\nPerforming final summary updates...');
      await updateSummaries(TARGET_DATE);
      console.log('Updating Bitcoin calculations...');
      await updateBitcoinCalculations(TARGET_DATE);
      console.log('All updates complete!');
    } catch (error) {
      console.error(`Error updating summaries: ${error}`);
    }
    
    // Show final status
    await checkPeriodStatus();
  } else {
    console.log('\nNo periods were successfully processed. No updates needed.');
  }
}

// Execute the process
processKeyPeriods().catch(error => {
  console.error(`Unhandled error: ${error}`);
  process.exit(1);
});