/**
 * Fetch Missing Periods Script
 * 
 * This script checks for missing settlement periods for a specific date
 * and fetches data from Elexon API to fill in the gaps.
 * 
 * Usage:
 *   npx tsx fetch_missing_periods.ts 2025-03-04
 */

import { db } from "./db";
import { format, parseISO } from 'date-fns';
import { fetchBidsOffers } from "./server/services/elexon";
import { processDailyCurtailment } from "./server/services/curtailment";
import { curtailmentRecords } from "./db/schema";
import { eq } from "drizzle-orm";

// Get the date from command line argument, default to yesterday
const dateArg = process.argv[2] || format(new Date(Date.now() - 86400000), 'yyyy-MM-dd');

async function getMissingPeriods(date: string): Promise<number[]> {
  console.log(`Checking for missing periods on ${date}...`);
  
  // Get existing periods for the date
  const existingPeriodsResult = await db
    .select({ period: curtailmentRecords.settlementPeriod })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date))
    .groupBy(curtailmentRecords.settlementPeriod);
  
  const existingPeriods = new Set(existingPeriodsResult.map(r => r.period));
  
  // Find missing periods (should be 1-48)
  const missingPeriods: number[] = [];
  for (let i = 1; i <= 48; i++) {
    if (!existingPeriods.has(i)) {
      missingPeriods.push(i);
    }
  }
  
  console.log(`Found ${missingPeriods.length} missing periods: ${missingPeriods.join(', ')}`);
  return missingPeriods;
}

async function fetchMissingPeriodsFromElexon(date: string, periods: number[]): Promise<void> {
  console.log(`Fetching missing periods from Elexon API for ${date}...`);
  
  let totalRecords = 0;
  
  // Process each missing period
  for (const period of periods) {
    try {
      console.log(`Fetching data for period ${period}...`);
      const records = await fetchBidsOffers(date, period);
      console.log(`Retrieved ${records.length} records for period ${period}`);
      totalRecords += records.length;
    } catch (error) {
      console.error(`Error fetching period ${period}:`, error);
    }
  }
  
  console.log(`Completed fetching ${totalRecords} records for ${periods.length} periods`);
}

async function reprocessDayWithRetry(date: string, maxRetries = 3): Promise<void> {
  let attempt = 1;
  
  while (attempt <= maxRetries) {
    try {
      console.log(`Reprocessing day ${date} (attempt ${attempt}/${maxRetries})...`);
      await processDailyCurtailment(date);
      console.log(`Successfully reprocessed ${date}`);
      return;
    } catch (error) {
      console.error(`Error reprocessing day (attempt ${attempt}/${maxRetries}):`, error);
      if (attempt === maxRetries) {
        throw error;
      }
      attempt++;
      await new Promise(resolve => setTimeout(resolve, 5000)); // Wait 5 seconds before retry
    }
  }
}

async function verifyPeriodCoverage(date: string): Promise<void> {
  const missingPeriods = await getMissingPeriods(date);
  
  if (missingPeriods.length === 0) {
    console.log(`✅ Complete coverage achieved for ${date}`);
  } else {
    console.log(`⚠️ Still missing ${missingPeriods.length} periods for ${date}: ${missingPeriods.join(', ')}`);
  }
  
  // Get summary counts
  const periodsResult = await db
    .select({ count: db.func.count() })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date));
  
  const distinctPeriods = await db
    .select({ count: db.func.count() })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date))
    .groupBy(curtailmentRecords.settlementPeriod);
  
  console.log(`Total records: ${periodsResult[0]?.count || 0}`);
  console.log(`Distinct periods: ${distinctPeriods.length}/48`);
}

async function main() {
  try {
    console.log(`\n=== Processing Missing Periods for ${dateArg} ===\n`);
    
    // Step 1: Identify missing periods
    const missingPeriods = await getMissingPeriods(dateArg);
    
    if (missingPeriods.length === 0) {
      console.log('No missing periods found. Data is complete!');
      return;
    }
    
    // Step 2: Fetch missing periods from Elexon API
    await fetchMissingPeriodsFromElexon(dateArg, missingPeriods);
    
    // Step 3: Reprocess the entire day to ensure consistency
    await reprocessDayWithRetry(dateArg);
    
    // Step 4: Verify the results
    await verifyPeriodCoverage(dateArg);
    
    console.log('\n=== Processing Complete ===\n');
  } catch (error) {
    console.error('Error processing missing periods:', error);
    process.exit(1);
  } finally {
    // Clean exit
    process.exit(0);
  }
}

// Run the script
main();