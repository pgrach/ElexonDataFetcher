/**
 * Condensed Data Reprocessing Script for 2025-04-03
 * 
 * This script is a simplified version focusing on fetching and processing
 * curtailment records for 2025-04-03 from the Elexon API.
 */

import { db } from './db';
import { curtailmentRecords, dailySummaries } from './db/schema';
import { fetchBidsOffers } from './server/services/elexon';
import { and, eq, sql } from 'drizzle-orm';
import { format } from 'date-fns';

// Configuration
const TARGET_DATE = '2025-04-03';

/**
 * Simple logging utility with timestamps
 */
function log(message: string): void {
  const timestamp = new Date().toISOString().substring(11, 19);
  console.log(`[${timestamp}] ${message}`);
}

/**
 * Clear existing curtailment records for the target date
 */
async function clearExistingCurtailmentRecords(): Promise<void> {
  log(`Clearing existing curtailment records for ${TARGET_DATE}...`);
  
  try {
    const result = await db.delete(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE))
      .returning({ id: curtailmentRecords.id });
    
    log(`Cleared ${result.length} existing curtailment records`);
  } catch (error) {
    log(`Error clearing curtailment records: ${(error as Error).message}`);
    throw error;
  }
}

/**
 * Fetch Elexon data and store valid curtailment records
 */
async function processCurtailmentData(): Promise<void> {
  log(`Processing curtailment data for ${TARGET_DATE}...`);
  
  try {
    let totalRecords = 0;
    
    // Try to connect to Elexon API
    try {
      const testPeriod = await fetchBidsOffers(TARGET_DATE, 1);
      log(`Successfully connected to Elexon API`);
    } catch (error) {
      log(`Error testing Elexon API connection: ${(error as Error).message}`);
      throw error;
    }
    
    // Process each settlement period
    for (let period = 1; period <= 48; period++) {
      log(`Processing settlement period ${period}...`);
      
      try {
        // Fetch data from Elexon API
        const records = await fetchBidsOffers(TARGET_DATE, period);
        
        log(`Found ${records.length} records for period ${period}`);
        
        // Skip if no records
        if (!records || records.length === 0) {
          continue;
        }
        
        // Insert records - use raw SQL for more efficient insertion
        const insertValues = records.map(record => `(
          '${TARGET_DATE}',
          ${period},
          '${record.id}',
          '${record.leadPartyName || ""}',
          '${record.volume}',
          '${record.volume * record.originalPrice}',
          ${record.soFlag},
          ${!!record.cadlFlag}
        )`).join(',');
        
        if (insertValues.length > 0) {
          const insertQuery = `
            INSERT INTO curtailment_records (
              settlement_date,
              settlement_period,
              farm_id,
              lead_party_name,
              volume,
              payment,
              so_flag,
              cadl_flag
            ) VALUES ${insertValues}
          `;
          
          await db.execute(sql.raw(insertQuery));
          totalRecords += records.length;
          log(`Inserted ${records.length} records for period ${period}`);
        }
      } catch (error) {
        log(`Error processing period ${period}: ${(error as Error).message}`);
        // Continue with other periods despite errors
      }
    }
    
    log(`Successfully processed ${totalRecords} curtailment records for ${TARGET_DATE}`);
  } catch (error) {
    log(`Error processing curtailment data: ${(error as Error).message}`);
    throw error;
  }
}

/**
 * Calculate daily summary from curtailment records
 */
async function calculateDailySummary(): Promise<void> {
  log(`Calculating daily summary for ${TARGET_DATE}...`);
  
  try {
    // Check if we have any curtailment records
    const recordCount = await db
      .select({ count: sql<number>`COUNT(*)` })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    if (recordCount[0].count === 0) {
      log(`No curtailment records found for ${TARGET_DATE}, skipping summary calculation`);
      return;
    }
    
    // Calculate totals from curtailment records
    const totals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
        totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    const totalCurtailedEnergy = parseFloat(totals[0]?.totalCurtailedEnergy || '0');
    const totalPayment = Math.abs(parseFloat(totals[0]?.totalPayment || '0')); // Convert to positive value
    
    log(`Calculated totals: ${totalCurtailedEnergy.toFixed(2)} MWh, £${totalPayment.toFixed(2)}`);
    
    // Delete existing summary if it exists
    await db.delete(dailySummaries)
      .where(eq(dailySummaries.summaryDate, TARGET_DATE));
    
    // Insert new summary
    await db.insert(dailySummaries).values({
      summaryDate: TARGET_DATE,
      totalCurtailedEnergy: totalCurtailedEnergy.toString(),
      totalPayment: totalPayment.toString()
    });
    
    log(`Updated daily summary for ${TARGET_DATE}`);
  } catch (error) {
    log(`Error calculating daily summary: ${(error as Error).message}`);
    throw error;
  }
}

/**
 * Run the condensed reprocessing pipeline focusing on curtailment records
 */
async function runCondensedReprocessing(): Promise<void> {
  const startTime = performance.now();
  
  try {
    log(`==== Starting condensed reprocessing for ${TARGET_DATE} ====`);
    
    // Step 1: Clear existing curtailment records
    await clearExistingCurtailmentRecords();
    
    // Step 2: Process curtailment data from Elexon API
    await processCurtailmentData();
    
    // Step 3: Calculate daily summary
    await calculateDailySummary();
    
    // Step 4: Verify results
    const recordStats = await db
      .select({
        recordCount: sql<number>`COUNT(*)`,
        periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
        totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
        totalPayment: sql<string>`SUM(ABS(payment::numeric))`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    log(`Verification results:`);
    log(`- Total records: ${recordStats[0]?.recordCount || 0}`);
    log(`- Settlement periods: ${recordStats[0]?.periodCount || 0}/48`);
    log(`- Total volume: ${parseFloat(recordStats[0]?.totalVolume || '0').toFixed(2)} MWh`);
    log(`- Total payment: £${parseFloat(recordStats[0]?.totalPayment || '0').toFixed(2)}`);
    
    const endTime = performance.now();
    const durationSeconds = ((endTime - startTime) / 1000).toFixed(2);
    
    log(`\n==== Reprocessing completed in ${durationSeconds}s ====\n`);
  } catch (error) {
    log(`\nERROR: Reprocessing failed: ${(error as Error).message}\n`);
    throw error;
  }
}

// Execute the reprocessing
runCondensedReprocessing()
  .then(() => {
    console.log('\nCondensed reprocessing completed successfully. Exiting...');
    process.exit(0);
  })
  .catch((error) => {
    console.error('\nCondensed reprocessing failed with error:', error);
    process.exit(1);
  });