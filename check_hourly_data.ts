/**
 * Check Hourly Data Completeness
 * 
 * This script checks if all 48 periods have data for a specific date
 * and identifies any missing periods to prioritize data ingestion.
 */

import { db } from './db';
import { curtailmentRecords, dailySummaries } from './db/schema';
import { eq } from 'drizzle-orm';
import { format } from 'date-fns';

/**
 * Check the completeness of hourly data for a specific date
 */
async function checkHourlyData(date: string): Promise<{
  date: string;
  totalRecords: number;
  periodsPresent: number[];
  periodsMissing: number[];
  totalVolume: number;
  totalPayment: number;
  dailySummaryExists: boolean;
  dailySummaryVolume?: number;
  dailySummaryPayment?: number;
}> {
  console.log(`\n=== Checking Hourly Data for ${date} ===\n`);
  
  // Get all records for this date
  const records = await db.query.curtailmentRecords.findMany({
    where: eq(curtailmentRecords.settlementDate, date)
  });
  
  console.log(`Found ${records.length} records for ${date}`);
  
  // Get unique periods
  const periodsPresent = Array.from(new Set(records.map(r => r.settlementPeriod))).sort((a, b) => a - b);
  
  // Find missing periods
  const allPeriods = Array.from({ length: 48 }, (_, i) => i + 1);
  const periodsMissing = allPeriods.filter(p => !periodsPresent.includes(p));
  
  // Calculate totals
  const totalVolume = records.reduce((sum, r) => sum + parseFloat(r.volume.toString()), 0);
  const totalPayment = records.reduce((sum, r) => sum + parseFloat(r.payment.toString()), 0);
  
  // Get daily summary
  const summary = await db.query.dailySummaries.findFirst({
    where: eq(dailySummaries.summaryDate, date)
  });
  
  const result = {
    date,
    totalRecords: records.length,
    periodsPresent,
    periodsMissing,
    totalVolume,
    totalPayment,
    dailySummaryExists: !!summary,
    dailySummaryVolume: summary ? parseFloat(summary.totalCurtailedEnergy?.toString() || '0') : undefined,
    dailySummaryPayment: summary ? parseFloat(summary.totalPayment?.toString() || '0') : undefined
  };
  
  // Display results
  console.log('--- Data Summary ---');
  console.log(`Total Records: ${result.totalRecords}`);
  console.log(`Periods Present: ${result.periodsPresent.length}/48`);
  console.log(`Periods Missing: ${result.periodsMissing.length}/48`);
  console.log(`Total Volume: ${result.totalVolume.toFixed(2)} MWh`);
  console.log(`Total Payment: £${result.totalPayment.toFixed(2)}`);
  
  if (result.periodsPresent.length > 0) {
    console.log(`\nPeriods Present: ${result.periodsPresent.join(', ')}`);
  }
  
  if (result.periodsMissing.length > 0) {
    console.log(`\nPeriods Missing: ${result.periodsMissing.join(', ')}`);
  }
  
  if (result.dailySummaryExists) {
    console.log('\n--- Daily Summary in Database ---');
    console.log(`Volume: ${result.dailySummaryVolume?.toFixed(2) || 'N/A'} MWh`);
    console.log(`Payment: £${result.dailySummaryPayment?.toFixed(2) || 'N/A'}`);
    
    // Check if daily summary matches the sum of hourly records
    const volumeDiff = Math.abs((result.dailySummaryVolume || 0) - result.totalVolume);
    const paymentDiff = Math.abs((result.dailySummaryPayment || 0) - result.totalPayment);
    
    if (volumeDiff > 0.01 || paymentDiff > 0.01) {
      console.log('\n⚠️ Daily summary does not match hourly records!');
      console.log(`Volume Difference: ${volumeDiff.toFixed(2)} MWh`);
      console.log(`Payment Difference: £${paymentDiff.toFixed(2)}`);
    } else {
      console.log('\n✅ Daily summary matches hourly records');
    }
  } else {
    console.log('\n⚠️ No daily summary found in database!');
  }
  
  // Generate command to fix missing periods
  if (result.periodsMissing.length > 0) {
    console.log('\n--- How to Fix Missing Data ---');
    
    if (result.periodsMissing.length <= 12) {
      // Generate commands to fix small batches of missing periods
      const batches: number[][] = [];
      let currentBatch: number[] = [];
      
      for (const period of result.periodsMissing) {
        if (currentBatch.length === 0 || period === currentBatch[currentBatch.length - 1] + 1) {
          currentBatch.push(period);
        } else {
          batches.push([...currentBatch]);
          currentBatch = [period];
        }
      }
      
      if (currentBatch.length > 0) {
        batches.push(currentBatch);
      }
      
      batches.forEach(batch => {
        const [start, end] = [batch[0], batch[batch.length - 1]];
        console.log(`npx tsx fix_hourly_data.ts ${date} ${start} ${end}`);
      });
    } else {
      // Generate commands to fix in larger batches
      const batchSize = 12;
      const numBatches = Math.ceil(result.periodsMissing.length / batchSize);
      
      for (let i = 0; i < numBatches; i++) {
        const startIdx = i * batchSize;
        const endIdx = Math.min(startIdx + batchSize - 1, result.periodsMissing.length - 1);
        const startPeriod = result.periodsMissing[startIdx];
        const endPeriod = result.periodsMissing[endIdx];
        
        console.log(`npx tsx fix_hourly_data.ts ${date} ${startPeriod} ${endPeriod}`);
      }
    }
  }
  
  return result;
}

/**
 * Main function
 */
async function main() {
  try {
    // Get the date from command-line arguments or use default
    const dateToCheck = process.argv[2] || format(new Date(), 'yyyy-MM-dd');
    
    // Check hourly data for the date
    await checkHourlyData(dateToCheck);
    
  } catch (error) {
    console.error(`Error in main process:`, error);
    process.exit(1);
  }
}

main();