/**
 * Thorough Data Reprocessing Script for 2025-04-14
 * 
 * This script performs a comprehensive reprocessing of data for 2025-04-14,
 * ensuring ALL data from Elexon is captured with no omissions and additional
 * verification steps are performed.
 * 
 * Run with: npx tsx thorough-reprocess-april14.ts
 */

import { db } from './db';
import { 
  curtailmentRecords, 
  dailySummaries, 
  historicalBitcoinCalculations, 
  bitcoinDailySummaries,
  monthlySummaries
} from './db/schema';
import { eq, sql, and, inArray } from 'drizzle-orm';
import { format } from 'date-fns';
import { fetchBidsOffers } from './server/services/elexon';
import { processSingleDay } from './server/services/bitcoinService';
import { processSingleDate } from './server/services/windGenerationService';
import * as fs from 'fs';
import * as path from 'path';

const TARGET_DATE = '2025-04-14';
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S']; // Standard miner models
const LOG_DIRECTORY = './logs';
const LOG_FILE = path.join(LOG_DIRECTORY, `thorough_reprocess_${TARGET_DATE.replace(/-/g, '')}_${new Date().toISOString().replace(/:/g, '-')}.log`);

// Create a logger that writes to both console and file
const logger = {
  log: (message: string) => {
    const timestamp = new Date().toISOString();
    const formattedMessage = `[${timestamp}] [INFO] ${message}`;
    console.log(formattedMessage);
    fs.appendFileSync(LOG_FILE, formattedMessage + '\\n');
  },
  error: (message: string, error?: any) => {
    const timestamp = new Date().toISOString();
    let formattedMessage = `[${timestamp}] [ERROR] ${message}`;
    if (error) {
      formattedMessage += `\\n${error.stack || JSON.stringify(error)}`;
    }
    console.error(formattedMessage);
    fs.appendFileSync(LOG_FILE, formattedMessage + '\\n');
  }
};

// Make sure the log directory exists
if (!fs.existsSync(LOG_DIRECTORY)) {
  fs.mkdirSync(LOG_DIRECTORY, { recursive: true });
}

// Initialize the log file
fs.writeFileSync(LOG_FILE, `=== Thorough Reprocessing Log for ${TARGET_DATE} ===\\n`);

// Helper function to wait between API calls to avoid rate limiting
const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

// Query Elexon for curtailment data records with retries
async function fetchElexonData(date: string, period: number, retries = 3, delay = 2000): Promise<any[]> {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      logger.log(`[${date} P${period}] Attempt ${attempt}/${retries}: Fetching data from Elexon...`);
      const records = await fetchBidsOffers(date, period);
      
      // Calculate total curtailment data (negative volume)
      const curtailmentRecords = records.filter(r => r.volume < 0 && (r.soFlag || r.cadlFlag));
      const totalVolume = curtailmentRecords.reduce((sum, r) => sum + Math.abs(r.volume), 0);
      const totalPayment = curtailmentRecords.reduce((sum, r) => sum + (Math.abs(r.volume) * r.originalPrice), 0);
      
      logger.log(`[${date} P${period}] Retrieved ${records.length} records, ${curtailmentRecords.length} curtailment records (${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)})`);
      
      // Verify we have data if this period should have curtailment
      if (totalVolume === 0 && isExpectedCurtailmentPeriod(period)) {
        if (attempt < retries) {
          logger.log(`[${date} P${period}] Expected curtailment data not found, retrying after delay...`);
          await sleep(delay);
          continue;
        } else {
          logger.error(`[${date} P${period}] Failed to find expected curtailment data after ${retries} attempts`);
        }
      }
      
      return records;
    } catch (error) {
      if (attempt < retries) {
        logger.error(`[${date} P${period}] Error on attempt ${attempt}/${retries}, retrying after delay...`, error);
        await sleep(delay * attempt); // Exponential backoff
      } else {
        logger.error(`[${date} P${period}] Failed after ${retries} attempts`, error);
        throw error;
      }
    }
  }
  
  return []; // Return empty array if all retries fail
}

// Helper function to determine if a period is likely to have curtailment data
// Based on observed patterns from April 14, 2025
function isExpectedCurtailmentPeriod(period: number): boolean {
  // These periods had significant curtailment in previous runs
  return period >= 4 && period <= 32;
}

async function thoroughReprocessData() {
  logger.log(`=== Starting Thorough Reprocessing for ${TARGET_DATE} ===`);
  const startTime = Date.now();
  
  try {
    // Step 1: Delete existing data for the target date to ensure a clean slate
    logger.log(`Removing existing curtailment records for ${TARGET_DATE}...`);
    await db.delete(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
      
    // Step 2: Remove all Bitcoin-related data for this date
    logger.log(`Removing existing Bitcoin calculations for ${TARGET_DATE}...`);
    for (const minerModel of MINER_MODELS) {
      await db.delete(historicalBitcoinCalculations)
        .where(and(
          eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
          eq(historicalBitcoinCalculations.minerModel, minerModel)
        ));
    }
    
    // Also remove bitcoin daily summaries if they exist
    await db.delete(bitcoinDailySummaries)
      .where(eq(bitcoinDailySummaries.summaryDate, TARGET_DATE));
    
    // Step 3: Remove daily summary to force recalculation
    logger.log(`Removing existing daily summary for ${TARGET_DATE}...`);
    await db.delete(dailySummaries)
      .where(eq(dailySummaries.summaryDate, TARGET_DATE));
    
    // Step 4: Reprocess wind generation data (this is non-destructive)
    logger.log(`Processing wind generation data for ${TARGET_DATE}...`);
    try {
      const windRecords = await processSingleDate(TARGET_DATE);
      logger.log(`Successfully processed ${windRecords} wind generation records for ${TARGET_DATE}`);
    } catch (error) {
      logger.error(`Error processing wind generation data:`, error);
    }
    
    // Step 5: Process all settlement periods (1-48)
    logger.log(`Processing all 48 settlement periods for ${TARGET_DATE} with extra verification...`);
    
    let totalRecords = 0;
    let totalVolume = 0;
    let totalPayment = 0;
    let periodsWithData = 0;
    
    // Process each period one by one with retries
    // We'll use all 48 periods to ensure we don't miss any data
    for (let period = 1; period <= 48; period++) {
      try {
        // Add a small delay between periods to avoid rate limiting
        if (period > 1) {
          await sleep(1500);
        }
        
        // Fetch and process raw data for each period with retries
        const elexonRecords = await fetchElexonData(TARGET_DATE, period);
        
        // Filter for valid curtailment records (negative volume with flags)
        const curtailmentRecordsToInsert = elexonRecords
          .filter(record => record.volume < 0 && (record.soFlag || record.cadlFlag))
          .map(record => {
            const absVolume = Math.abs(record.volume);
            const payment = absVolume * record.originalPrice;
            
            return {
              settlementDate: TARGET_DATE,
              settlementPeriod: period,
              farmId: record.id,
              leadPartyName: record.leadPartyName || 'Unknown',
              volume: record.volume.toString(), // Keep negative value
              payment: payment.toString(),
              originalPrice: record.originalPrice.toString(),
              finalPrice: record.finalPrice.toString(),
              soFlag: record.soFlag,
              cadlFlag: record.cadlFlag
            };
          });
        
        // Insert all valid records for this period
        if (curtailmentRecordsToInsert.length > 0) {
          await db.insert(curtailmentRecords).values(curtailmentRecordsToInsert);
          
          // Calculate period totals for logging
          const periodVolume = curtailmentRecordsToInsert.reduce(
            (sum, r) => sum + Math.abs(parseFloat(r.volume)), 0
          );
          const periodPayment = curtailmentRecordsToInsert.reduce(
            (sum, r) => sum + parseFloat(r.payment), 0
          );
          
          logger.log(`[${TARGET_DATE} P${period}] Inserted ${curtailmentRecordsToInsert.length} curtailment records (${periodVolume.toFixed(2)} MWh, £${periodPayment.toFixed(2)})`);
          
          totalRecords += curtailmentRecordsToInsert.length;
          totalVolume += periodVolume;
          totalPayment += periodPayment;
          periodsWithData++;
        } else {
          if (isExpectedCurtailmentPeriod(period)) {
            logger.log(`[${TARGET_DATE} P${period}] WARNING: No curtailment records found despite being an expected curtailment period`);
          } else {
            logger.log(`[${TARGET_DATE} P${period}] No curtailment records to insert`);
          }
        }
        
        // After each period, verify inserted records match what we expected
        const verificationCount = await db
          .select({ count: sql<string>`COUNT(*)` })
          .from(curtailmentRecords)
          .where(and(
            eq(curtailmentRecords.settlementDate, TARGET_DATE),
            eq(curtailmentRecords.settlementPeriod, period)
          ));
        
        const actualCount = parseInt(verificationCount[0]?.count || '0');
        if (actualCount !== curtailmentRecordsToInsert.length) {
          logger.error(`[${TARGET_DATE} P${period}] Verification failed: Expected ${curtailmentRecordsToInsert.length} records, found ${actualCount}`);
        }
      } catch (error) {
        logger.error(`Error processing period ${period}:`, error);
      }
    }
    
    logger.log(`\nProcessed ${totalRecords} total curtailment records across ${periodsWithData} periods`);
    logger.log(`Total volume: ${totalVolume.toFixed(2)} MWh`);
    logger.log(`Total payment: £${totalPayment.toFixed(2)}`);
    
    // Step 6: Post-processing verification
    logger.log(`\nPerforming database verification...`);
    const dbVerification = await db.select({
      recordCount: sql<string>`COUNT(*)`,
      periodCount: sql<string>`COUNT(DISTINCT ${curtailmentRecords.settlementPeriod})`,
      totalVolume: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
      totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    logger.log(`Database verification results:`);
    logger.log(`- Record count: ${dbVerification[0]?.recordCount || '0'}`);
    logger.log(`- Settlement periods: ${dbVerification[0]?.periodCount || '0'}`);
    logger.log(`- Total volume: ${Number(dbVerification[0]?.totalVolume || 0).toFixed(2)} MWh`);
    logger.log(`- Total payment: £${Number(dbVerification[0]?.totalPayment || 0).toFixed(2)}`);
    
    if (totalRecords === 0 || Number(dbVerification[0]?.recordCount || 0) === 0) {
      logger.error(`No curtailment records found for ${TARGET_DATE}, cannot continue.`);
      return;
    }
    
    // Step 7: Update daily summary
    logger.log(`\nUpdating daily summary for ${TARGET_DATE}...`);
    
    // Use the verified totals from the database for maximum accuracy
    const dbEnergy = Number(dbVerification[0]?.totalVolume || 0);
    const dbPayment = Number(dbVerification[0]?.totalPayment || 0);
    
    await db.insert(dailySummaries).values({
      summaryDate: TARGET_DATE,
      totalCurtailedEnergy: dbEnergy.toString(),
      totalPayment: dbPayment.toString(),
      lastUpdated: new Date()
    }).onConflictDoUpdate({
      target: [dailySummaries.summaryDate],
      set: {
        totalCurtailedEnergy: dbEnergy.toString(),
        totalPayment: dbPayment.toString(),
        lastUpdated: new Date()
      }
    });
    
    // Step 8: Update monthly summary
    const yearMonth = TARGET_DATE.substring(0, 7);
    logger.log(`\nUpdating monthly summary for ${yearMonth}...`);
    
    // Calculate total from all daily summaries in this month
    const monthlyTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(${dailySummaries.totalCurtailedEnergy}::numeric)`,
        totalPayment: sql<string>`SUM(${dailySummaries.totalPayment}::numeric)`
      })
      .from(dailySummaries)
      .where(sql`date_trunc('month', ${dailySummaries.summaryDate}::date) = date_trunc('month', ${TARGET_DATE}::date)`);

    if (monthlyTotals[0].totalCurtailedEnergy && monthlyTotals[0].totalPayment) {
      await db.insert(monthlySummaries).values({
        yearMonth,
        totalCurtailedEnergy: monthlyTotals[0].totalCurtailedEnergy,
        totalPayment: monthlyTotals[0].totalPayment,
        updatedAt: new Date()
      }).onConflictDoUpdate({
        target: [monthlySummaries.yearMonth],
        set: {
          totalCurtailedEnergy: monthlyTotals[0].totalCurtailedEnergy,
          totalPayment: monthlyTotals[0].totalPayment,
          updatedAt: new Date()
        }
      });
    }
    
    // Step 9: Process Bitcoin calculations for each miner model
    logger.log(`\nProcessing Bitcoin calculations for ${TARGET_DATE}...`);
    
    for (const minerModel of MINER_MODELS) {
      try {
        logger.log(`Processing ${minerModel}...`);
        await processSingleDay(TARGET_DATE, minerModel);
        logger.log(`Successfully processed ${minerModel}`);
      } catch (error) {
        logger.error(`Error processing Bitcoin calculations for ${minerModel}:`, error);
      }
    }
    
    // Step 10: Final verification
    logger.log(`\nPerforming final verification...`);
    
    // Verify daily summary was created
    const summary = await db
      .select()
      .from(dailySummaries)
      .where(eq(dailySummaries.summaryDate, TARGET_DATE));
    
    logger.log(`Daily Summary verification:`);
    if (summary.length > 0) {
      logger.log(`- Date: ${summary[0].summaryDate}`);
      logger.log(`- Total Curtailed Energy: ${summary[0].totalCurtailedEnergy} MWh`);
      logger.log(`- Total Payment: £${summary[0].totalPayment}`);
      if (summary[0].totalWindGeneration) {
        logger.log(`- Total Wind Generation: ${summary[0].totalWindGeneration} MWh`);
      }
    } else {
      logger.error(`No daily summary found for ${TARGET_DATE} after reprocessing.`);
    }
    
    // Verify Bitcoin calculations were created
    const bitcoinCalcs = await db
      .select({
        minerModel: historicalBitcoinCalculations.minerModel,
        count: sql<number>`COUNT(*)`
      })
      .from(historicalBitcoinCalculations)
      .where(eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE))
      .groupBy(historicalBitcoinCalculations.minerModel);
    
    logger.log(`Bitcoin Calculations verification:`);
    if (bitcoinCalcs.length > 0) {
      for (const calc of bitcoinCalcs) {
        logger.log(`- ${calc.minerModel}: ${calc.count} records`);
      }
    } else {
      logger.error(`No Bitcoin calculations found for ${TARGET_DATE} after reprocessing.`);
    }
    
    const endTime = Date.now();
    const duration = (endTime - startTime) / 1000;
    
    logger.log(`\n=== Thorough Reprocessing Complete ===`);
    logger.log(`Completed at: ${format(new Date(), "yyyy-MM-dd HH:mm:ss")}`);
    logger.log(`Total duration: ${duration.toFixed(2)} seconds`);
    
  } catch (error) {
    logger.error("Fatal error during reprocessing:", error);
    process.exit(1);
  }
}

// Run the reprocessing script
thoroughReprocessData().catch(error => {
  logger.error("Script execution error:", error);
  process.exit(1);
});