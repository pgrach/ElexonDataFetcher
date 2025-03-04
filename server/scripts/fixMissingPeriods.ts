/**
 * Fix Missing Curtailment Periods
 * 
 * This script identifies and fixes missing curtailment periods for a specific date.
 * It fetches data from the Elexon API for any missing periods and updates the database.
 * 
 * Usage:
 *   npx tsx server/scripts/fixMissingPeriods.ts
 */
import { db } from "@db";
import { curtailmentRecords, dailySummaries, monthlySummaries, yearlySummaries } from "@db/schema";
import { fetchBidsOffers } from "../services/elexon";
import { processHistoricalCalculations } from "../services/bitcoinService";
import { eq, sql } from "drizzle-orm";
import { processDailyCurtailment } from "../services/curtailment";

// Default to 2025-03-02 but allow override via command line parameter
const TARGET_DATE = process.argv.find(arg => arg.startsWith('TARGET_DATE='))?.split('=')[1] || '2025-03-02';
const FORCE_REPROCESS = process.argv.includes('FORCE_REPROCESS=true') || true; // Set to true to force reprocessing even when no missing periods are found

async function getExistingPeriods(date: string): Promise<Set<number>> {
  try {
    const results = await db
      .select({
        period: curtailmentRecords.settlementPeriod
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date))
      .groupBy(curtailmentRecords.settlementPeriod);
    
    return new Set(results.map(r => r.period));
  } catch (error) {
    console.error('Error getting existing periods:', error);
    throw error;
  }
}

async function getMissingPeriods(date: string): Promise<number[]> {
  const existingPeriods = await getExistingPeriods(date);
  const missingPeriods: number[] = [];
  
  // A day should have 48 settlement periods
  for (let i = 1; i <= 48; i++) {
    if (!existingPeriods.has(i)) {
      missingPeriods.push(i);
    }
  }
  
  return missingPeriods;
}

async function getDBStats(date: string) {
  try {
    // Get curtailment records stats
    const curtailmentStats = await db
      .select({
        recordCount: sql<number>`COUNT(*)::int`,
        periodCount: sql<number>`COUNT(DISTINCT settlement_period)::int`,
        farmCount: sql<number>`COUNT(DISTINCT farm_id)::int`,
        totalVolume: sql<string>`SUM(ABS(volume::numeric))::text`,
        totalPayment: sql<string>`SUM(payment::numeric)::text`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date));

    // Get daily summary
    const summary = await db
      .select()
      .from(dailySummaries)
      .where(eq(dailySummaries.summaryDate, date));

    return {
      curtailment: curtailmentStats[0],
      summary: summary[0]
    };
  } catch (error) {
    console.error('Error getting database stats:', error);
    throw error;
  }
}

async function fixMissingPeriods() {
  try {
    console.log(`\n=== Fixing Missing Periods for ${TARGET_DATE} ===\n`);
    
    // Get current database stats
    console.log('Fetching current database stats...');
    const before = await getDBStats(TARGET_DATE);
    
    console.log('\nDatabase Current State:');
    console.log('Curtailment Records:', {
      records: before.curtailment.recordCount,
      periods: before.curtailment.periodCount,
      volume: Number(before.curtailment.totalVolume).toFixed(2),
      payment: Number(before.curtailment.totalPayment).toFixed(2)
    });
    
    // Find missing periods
    const missingPeriods = await getMissingPeriods(TARGET_DATE);
    console.log(`\nFound ${missingPeriods.length} missing periods: ${missingPeriods.join(', ')}`);
    
    // Add time of day analysis for missing periods
    if (missingPeriods.length > 0) {
      const morningMissing = missingPeriods.filter(p => p >= 1 && p <= 16);
      const afternoonMissing = missingPeriods.filter(p => p >= 17 && p <= 32);
      const eveningMissing = missingPeriods.filter(p => p >= 33 && p <= 48);
      
      console.log('\nMissing Period Analysis:');
      console.log(`Morning (00:00-08:00): ${morningMissing.length} periods missing`);
      console.log(`Afternoon (08:00-16:00): ${afternoonMissing.length} periods missing`);
      console.log(`Evening (16:00-24:00): ${eveningMissing.length} periods missing`);
    }
    
    if (missingPeriods.length === 0 && !FORCE_REPROCESS) {
      console.log('No missing periods found. Database is complete.');
      console.log('Set FORCE_REPROCESS to true if you want to reprocess anyway.');
      return;
    }
    
    if (missingPeriods.length === 0) {
      console.log('Force reprocessing enabled. Continuing with full data reprocessing...');
    }
    
    // Option 1: Process individual missing periods
    /*
    let totalRecordsAdded = 0;
    
    for (const period of missingPeriods) {
      console.log(`\nProcessing missing period ${period}...`);
      
      try {
        const records = await fetchBidsOffers(TARGET_DATE, period);
        console.log(`Fetched ${records.length} records from Elexon API for period ${period}`);
        
        // Process would go here, but it's better to use the centralized function
      } catch (error) {
        console.error(`Error processing period ${period}:`, error);
      }
    }
    */
    
    // Option 2: Use the centralized function to process the entire day
    console.log('\nReprocessing entire day to ensure data consistency...');
    await processDailyCurtailment(TARGET_DATE);
    console.log('✓ Updated curtailment records');
    
    // Update bitcoin calculations
    console.log('\nUpdating Bitcoin calculations...');
    await processHistoricalCalculations(TARGET_DATE, TARGET_DATE);
    console.log('✓ Updated historical bitcoin calculations');
    
    // Verify updates
    const after = await getDBStats(TARGET_DATE);
    
    console.log('\nUpdated Database State:');
    console.log('Curtailment Records:', {
      records: after.curtailment.recordCount,
      periods: after.curtailment.periodCount,
      volume: Number(after.curtailment.totalVolume).toFixed(2),
      payment: Number(after.curtailment.totalPayment).toFixed(2)
    });
    
    if (after.summary) {
      console.log('Updated Daily Summary:', {
        energy: Number(after.summary.totalCurtailedEnergy).toFixed(2),
        payment: Number(after.summary.totalPayment).toFixed(2)
      });
    }
    
    // Calculate differences
    const recordDiff = after.curtailment.recordCount - before.curtailment.recordCount;
    const periodDiff = after.curtailment.periodCount - before.curtailment.periodCount;
    const volumeDiff = Number(after.curtailment.totalVolume) - Number(before.curtailment.totalVolume);
    const paymentDiff = Number(after.curtailment.totalPayment) - Number(before.curtailment.totalPayment);
    
    console.log('\nChanges Made:');
    console.log(`Records Added: ${recordDiff}`);
    console.log(`Periods Added: ${periodDiff}`);
    console.log(`Volume Added: ${volumeDiff.toFixed(2)} MWh`);
    console.log(`Payment Added: £${paymentDiff.toFixed(2)}`);
    
    // Check for remaining missing periods
    const remainingMissing = await getMissingPeriods(TARGET_DATE);
    
    if (remainingMissing.length > 0) {
      console.log(`\n⚠️ Still missing ${remainingMissing.length} periods: ${remainingMissing.join(', ')}`);
      console.log('These periods likely had no curtailment data in the Elexon API.');
    } else {
      console.log('\n✅ All periods are now accounted for!');
    }
    
  } catch (error) {
    console.error('Error fixing missing periods:', error);
    process.exit(1);
  }
}

// Run the fix
fixMissingPeriods();