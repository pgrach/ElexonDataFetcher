/**
 * Data Verification Script for 2025-03-20
 * 
 * This script checks the Elexon API data against our database records for 
 * 2025-03-20, identifies any missing or duplicate data, and provides
 * options to correct any issues found.
 * 
 * Usage:
 *   npx tsx check_elexon_2025_03_20.ts
 */

import { db } from "./db";
import { curtailmentRecords } from "./db/schema";
import { fetchBidsOffers } from "./server/services/elexon";
import { eq, sql, count } from "drizzle-orm";
import { processDailyCurtailment } from "./server/services/curtailment";
import { processSingleDay } from "./server/services/bitcoinService";

// Target date to check
const TARGET_DATE = '2025-03-20';
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

/**
 * Check the current database state for the target date
 */
async function checkCurrentDatabaseState(): Promise<{
  recordCount: number;
  periodCount: number;
  farmCount: number;
  totalVolume: string;
  totalPayment: string;
  missingPeriods: number[];
  periodCounts: Record<number, number>;
}> {
  try {
    // Get basic statistics
    const dbStats = await db.select({
      recordCount: count(),
      periodCount: sql<number>`COUNT(DISTINCT ${curtailmentRecords.settlementPeriod})`,
      farmCount: sql<number>`COUNT(DISTINCT ${curtailmentRecords.farmId})`,
      totalVolume: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
      totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));

    // Get period counts
    const periodRecords = await db.select({
      period: curtailmentRecords.settlementPeriod,
      recordCount: count()
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE))
    .groupBy(curtailmentRecords.settlementPeriod);

    // Create a map of period counts
    const periodCounts: Record<number, number> = {};
    periodRecords.forEach(record => {
      periodCounts[record.period] = record.recordCount;
    });

    // Find missing periods (there should be 48 periods in a day)
    const existingPeriods = new Set(periodRecords.map(record => record.period));
    const missingPeriods: number[] = [];
    for (let i = 1; i <= 48; i++) {
      if (!existingPeriods.has(i)) {
        missingPeriods.push(i);
      }
    }

    return {
      recordCount: dbStats[0]?.recordCount || 0,
      periodCount: dbStats[0]?.periodCount || 0,
      farmCount: dbStats[0]?.farmCount || 0,
      totalVolume: dbStats[0]?.totalVolume || '0',
      totalPayment: dbStats[0]?.totalPayment || '0',
      missingPeriods,
      periodCounts
    };
  } catch (error) {
    console.error('Error checking database state:', error);
    return {
      recordCount: 0,
      periodCount: 0,
      farmCount: 0,
      totalVolume: '0',
      totalPayment: '0',
      missingPeriods: Array.from({ length: 48 }, (_, i) => i + 1),
      periodCounts: {}
    };
  }
}

/**
 * Check Elexon API data for a specific period
 */
async function checkElexonPeriod(period: number): Promise<{
  recordCount: number;
  totalVolume: number;
  farmIds: string[];
}> {
  try {
    const records = await fetchBidsOffers(TARGET_DATE, period);
    let totalVolume = 0;
    
    // Sum up the absolute values of volumes
    records.forEach(record => {
      totalVolume += Math.abs(record.volume);
    });
    
    return {
      recordCount: records.length,
      totalVolume,
      farmIds: records.map(record => record.id)
    };
  } catch (error) {
    console.error(`Error checking Elexon API for period ${period}:`, error);
    return {
      recordCount: 0,
      totalVolume: 0,
      farmIds: []
    };
  }
}

/**
 * Check all periods from Elexon API
 */
async function checkAllElexonPeriods(): Promise<{
  recordCount: number;
  totalVolume: number;
  periodData: Record<number, { recordCount: number; totalVolume: number; farmIds: string[] }>;
  missingPeriods: number[];
}> {
  const periodData: Record<number, { recordCount: number; totalVolume: number; farmIds: string[] }> = {};
  let totalRecordCount = 0;
  let totalVolume = 0;
  let missingPeriods: number[] = [];

  // Check each period (limit to a few at a time to avoid rate limits)
  const BATCH_SIZE = 6;
  for (let batchStart = 1; batchStart <= 48; batchStart += BATCH_SIZE) {
    const batchEnd = Math.min(batchStart + BATCH_SIZE - 1, 48);
    console.log(`Checking Elexon API for periods ${batchStart}-${batchEnd}...`);
    
    const batchPromises = [];
    for (let period = batchStart; period <= batchEnd; period++) {
      batchPromises.push(checkElexonPeriod(period));
    }
    
    const batchResults = await Promise.all(batchPromises);
    
    for (let i = 0; i < batchResults.length; i++) {
      const period = batchStart + i;
      const result = batchResults[i];
      
      periodData[period] = result;
      totalRecordCount += result.recordCount;
      totalVolume += result.totalVolume;
      
      if (result.recordCount === 0) {
        missingPeriods.push(period);
      }
      
      console.log(`Period ${period}: ${result.recordCount} records, ${result.totalVolume.toFixed(2)} MWh`);
    }
  }

  return {
    recordCount: totalRecordCount,
    totalVolume,
    periodData,
    missingPeriods
  };
}

/**
 * Compare database and Elexon API data
 */
async function compareData(
  dbState: Awaited<ReturnType<typeof checkCurrentDatabaseState>>,
  elexonState: Awaited<ReturnType<typeof checkAllElexonPeriods>>
): Promise<{
  missingPeriodsInDb: number[];
  missingPeriodsInElexon: number[];
  periodsWithDifferences: Array<{ 
    period: number; 
    dbRecords: number; 
    elexonRecords: number;
    dbVolume: number;
    elexonVolume: number;
  }>;
  summary: {
    totalDbRecords: number;
    totalElexonRecords: number;
    totalDbVolume: number;
    totalElexonVolume: number;
    volumeDifference: number;
    recordDifference: number;
  }
}> {
  const missingPeriodsInDb = dbState.missingPeriods;
  const missingPeriodsInElexon = elexonState.missingPeriods;
  const periodsWithDifferences: Array<{ 
    period: number; 
    dbRecords: number; 
    elexonRecords: number;
    dbVolume: number;
    elexonVolume: number;
  }> = [];

  // Check each period for differences
  for (let period = 1; period <= 48; period++) {
    const dbRecords = dbState.periodCounts[period] || 0;
    const elexonData = elexonState.periodData[period];
    
    if (elexonData && dbRecords !== elexonData.recordCount) {
      // We have a difference in record count
      periodsWithDifferences.push({
        period,
        dbRecords,
        elexonRecords: elexonData.recordCount,
        dbVolume: 0, // We don't have per-period volume from DB query
        elexonVolume: elexonData.totalVolume
      });
    }
  }

  return {
    missingPeriodsInDb,
    missingPeriodsInElexon,
    periodsWithDifferences,
    summary: {
      totalDbRecords: dbState.recordCount,
      totalElexonRecords: elexonState.recordCount,
      totalDbVolume: parseFloat(dbState.totalVolume),
      totalElexonVolume: elexonState.totalVolume,
      volumeDifference: parseFloat(dbState.totalVolume) - elexonState.totalVolume,
      recordDifference: dbState.recordCount - elexonState.recordCount
    }
  };
}

/**
 * Process a single period to fix data
 */
async function processOnlyMissingPeriods(periods: number[]): Promise<void> {
  console.log(`Processing only the missing periods: ${periods.join(', ')}`);
  
  // Clear only the specific periods we want to update
  for (const period of periods) {
    const deleteResult = await db.delete(curtailmentRecords)
      .where(
        sql`${curtailmentRecords.settlementDate} = ${TARGET_DATE} AND ${curtailmentRecords.settlementPeriod} = ${period}`
      );
    
    console.log(`Cleared records for period ${period}`);
    
    // Now fetch and process this period
    try {
      const records = await fetchBidsOffers(TARGET_DATE, period);
      console.log(`Retrieved ${records.length} records for period ${period}`);
      
      // We'll use the processDailyCurtailment function to handle the insertion
      // but it processes the entire day, so we'll need to implement a custom
      // solution for just the selected periods
      
      // For now, simulate what we would do
      console.log(`Would process ${records.length} records for period ${period}`);
    } catch (error) {
      console.error(`Error processing period ${period}:`, error);
    }
  }
}

/**
 * Fixed curtailment data by reprocessing the entire day
 */
async function fixAllData(): Promise<void> {
  console.log(`Reprocessing all curtailment data for ${TARGET_DATE}`);
  
  try {
    await processDailyCurtailment(TARGET_DATE);
    
    // Update Bitcoin calculations for each miner model
    for (const minerModel of MINER_MODELS) {
      console.log(`Updating Bitcoin calculations for ${minerModel}`);
      await processSingleDay(TARGET_DATE, minerModel);
    }
    
    console.log('Data reprocessing complete');
  } catch (error) {
    console.error('Error reprocessing data:', error);
  }
}

/**
 * Main function - Analysis only mode
 */
async function main() {
  console.log(`=== Checking Elexon API vs Database for ${TARGET_DATE} (Analysis Only) ===`);
  
  // Step 1: Check current state in database
  console.log('\nChecking current database state...');
  const dbState = await checkCurrentDatabaseState();
  
  console.log(`Database state for ${TARGET_DATE}:`);
  console.log(`- Records: ${dbState.recordCount}`);
  console.log(`- Periods: ${dbState.periodCount}/48`);
  console.log(`- Farms: ${dbState.farmCount}`);
  console.log(`- Total Volume: ${parseFloat(dbState.totalVolume).toFixed(2)} MWh`);
  console.log(`- Total Payment: £${parseFloat(dbState.totalPayment).toFixed(2)}`);
  
  if (dbState.missingPeriods.length > 0) {
    console.log(`- Missing Periods: ${dbState.missingPeriods.join(', ')}`);
  } else {
    console.log('- All 48 periods have data in the database');
  }
  
  // Focus on checking just periods 47 and 48 which showed data in our previous run
  console.log('\nFocusing on checking periods 47-48 in the Elexon API...');
  const period47Data = await checkElexonPeriod(47);
  const period48Data = await checkElexonPeriod(48);
  
  console.log(`\nElexon API data for specific periods:`);
  console.log(`- Period 47: ${period47Data.recordCount} records, ${period47Data.totalVolume.toFixed(2)} MWh`);
  if (period47Data.farmIds.length > 0) {
    console.log(`  Farm IDs: ${period47Data.farmIds.join(', ')}`);
  }
  
  console.log(`- Period 48: ${period48Data.recordCount} records, ${period48Data.totalVolume.toFixed(2)} MWh`);
  if (period48Data.farmIds.length > 0) {
    console.log(`  Farm IDs: ${period48Data.farmIds.join(', ')}`);
  }
  
  // Compare with DB
  const dbPeriod47Count = dbState.periodCounts[47] || 0;
  const dbPeriod48Count = dbState.periodCounts[48] || 0;
  
  console.log('\nComparison for specific periods:');
  console.log(`- Period 47: DB=${dbPeriod47Count} records, Elexon=${period47Data.recordCount} records`);
  console.log(`- Period 48: DB=${dbPeriod48Count} records, Elexon=${period48Data.recordCount} records`);
  
  // Suggest action
  console.log('\nAction Required:');
  
  if (dbState.recordCount === 0 && (period47Data.recordCount > 0 || period48Data.recordCount > 0)) {
    console.log(`- Found ${period47Data.recordCount + period48Data.recordCount} records in Elexon API that are missing from database.`);
    console.log('- Consider running: await processDailyCurtailment("2025-03-20");');
  } else if (dbPeriod47Count !== period47Data.recordCount || dbPeriod48Count !== period48Data.recordCount) {
    console.log('- Record count discrepancy detected for periods 47 and/or 48.');
    console.log('- Consider running: await processDailyCurtailment("2025-03-20");');
  } else {
    console.log('- No significant issues detected for periods 47-48.');
  }
}

// Run the main function
main().catch(error => {
  console.error('Error running script:', error);
  process.exit(1);
}).finally(() => {
  console.log('Script completed');
});