/**
 * Complete Data Reprocessing Script for 2025-04-03
 * 
 * This script performs a full reprocessing of data for April 3, 2025:
 * 1. Clears existing curtailment records for the date
 * 2. Fetches fresh data from Elexon API for all 48 settlement periods
 * 3. Processes and stores valid curtailment records
 * 4. Updates daily, monthly, and yearly summaries
 * 5. Recalculates Bitcoin mining potential for all miner models
 * 6. Updates Bitcoin daily, monthly, and yearly summaries
 * 7. Verifies the results for data integrity
 */

import { db } from './db';
import { 
  curtailmentRecords, 
  dailySummaries,
  monthlySummaries,
  yearlySummaries,
  historicalBitcoinCalculations,
  bitcoinDailySummaries,
  bitcoinMonthlySummaries,
  bitcoinYearlySummaries
} from './db/schema';
import { fetchBidsOffers } from './server/services/elexon';
import { and, eq, sql } from 'drizzle-orm';
import { calculateBitcoin } from './server/utils/bitcoin';
import { getDifficultyData } from './server/services/dynamodbService';
import { ElexonBidOffer } from './server/types/elexon';
import { format, parseISO } from 'date-fns';

// Configuration
const TARGET_DATE = '2025-04-03';
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];
const BATCH_SIZE = 50; // Number of records to process in a batch

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
 * Clear existing Bitcoin calculations for the target date
 */
async function clearExistingBitcoinCalculations(): Promise<void> {
  log(`Clearing existing Bitcoin calculations for ${TARGET_DATE}...`);
  
  try {
    // Clear historical calculations for each miner model
    for (const minerModel of MINER_MODELS) {
      const result = await db.delete(historicalBitcoinCalculations)
        .where(and(
          eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
          eq(historicalBitcoinCalculations.minerModel, minerModel)
        ))
        .returning({ id: historicalBitcoinCalculations.id });
      
      log(`Cleared ${result.length} historical Bitcoin calculations for ${minerModel}`);
    }
    
    // Clear Bitcoin daily summaries
    for (const minerModel of MINER_MODELS) {
      const result = await db.delete(bitcoinDailySummaries)
        .where(and(
          eq(bitcoinDailySummaries.summaryDate, TARGET_DATE),
          eq(bitcoinDailySummaries.minerModel, minerModel)
        ))
        .returning({ id: bitcoinDailySummaries.id });
      
      log(`Cleared ${result.length} Bitcoin daily summaries for ${minerModel}`);
    }
  } catch (error) {
    log(`Error clearing Bitcoin calculations: ${(error as Error).message}`);
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
    
    // Load BMU mapping to validate wind farm IDs
    // This should be a list of valid wind farm BMU IDs
    let validWindFarmIds: Set<string> = new Set();
    try {
      // Try to load wind farm IDs from the database or use fetchBidsOffers's internal validation
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
        
        if (!records || records.length === 0) {
          log(`No records found for period ${period}`);
          continue;
        }
        
        // Only process valid records (validation is done inside fetchBidsOffers)
        log(`Found ${records.length} records for period ${period}`);
        
        // Insert records in batches to improve performance
        const insertPromises = [];
        for (const record of records) {
          insertPromises.push(
            db.insert(curtailmentRecords).values({
              settlementDate: TARGET_DATE,
              settlementPeriod: period,
              farmId: record.id,
              leadPartyName: record.leadPartyName || "",
              volume: record.volume.toString(),
              payment: (record.volume * record.originalPrice).toString(), // payment = volume * price
              soFlag: record.soFlag,
              cadlFlag: !!record.cadlFlag
            })
          );
        }
        
        await Promise.all(insertPromises);
        totalRecords += records.length;
        log(`Inserted ${records.length} records for period ${period}`);
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
 * Update monthly summary based on daily summaries
 */
async function updateMonthlySummary(): Promise<void> {
  log(`Updating monthly summary for ${TARGET_DATE}...`);
  
  try {
    // Extract year and month from the target date
    const date = parseISO(TARGET_DATE);
    const yearMonth = format(date, 'yyyy-MM');
    
    // Calculate monthly totals from daily summaries
    const monthlyTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(${dailySummaries.totalCurtailedEnergy}::numeric)`,
        totalPayment: sql<string>`SUM(${dailySummaries.totalPayment}::numeric)`
      })
      .from(dailySummaries)
      .where(sql`TO_CHAR(${dailySummaries.summaryDate}, 'YYYY-MM') = ${yearMonth}`);
    
    const totalCurtailedEnergy = parseFloat(monthlyTotals[0]?.totalCurtailedEnergy || '0');
    const totalPayment = parseFloat(monthlyTotals[0]?.totalPayment || '0');
    
    log(`Calculated monthly totals: ${totalCurtailedEnergy.toFixed(2)} MWh, £${totalPayment.toFixed(2)}`);
    
    // Update or insert monthly summary
    await db
      .insert(monthlySummaries)
      .values({
        yearMonth: yearMonth,
        totalCurtailedEnergy: totalCurtailedEnergy.toString(),
        totalPayment: totalPayment.toString()
      })
      .onConflictDoUpdate({
        target: monthlySummaries.yearMonth,
        set: {
          totalCurtailedEnergy: totalCurtailedEnergy.toString(),
          totalPayment: totalPayment.toString()
        }
      });
    
    log(`Updated monthly summary for ${yearMonth}`);
  } catch (error) {
    log(`Error updating monthly summary: ${(error as Error).message}`);
    throw error;
  }
}

/**
 * Update yearly summary based on monthly summaries
 */
async function updateYearlySummary(): Promise<void> {
  log(`Updating yearly summary for ${TARGET_DATE}...`);
  
  try {
    // Extract year from the target date
    const year = TARGET_DATE.substring(0, 4);
    
    // Calculate yearly totals from monthly summaries
    const yearlyTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(${monthlySummaries.totalCurtailedEnergy}::numeric)`,
        totalPayment: sql<string>`SUM(${monthlySummaries.totalPayment}::numeric)`
      })
      .from(monthlySummaries)
      .where(sql`${monthlySummaries.yearMonth} LIKE ${`${year}-%`}`);
    
    const totalCurtailedEnergy = parseFloat(yearlyTotals[0]?.totalCurtailedEnergy || '0');
    const totalPayment = parseFloat(yearlyTotals[0]?.totalPayment || '0');
    
    log(`Calculated yearly totals: ${totalCurtailedEnergy.toFixed(2)} MWh, £${totalPayment.toFixed(2)}`);
    
    // Update or insert yearly summary
    await db
      .insert(yearlySummaries)
      .values({
        year: year,
        totalCurtailedEnergy: totalCurtailedEnergy.toString(),
        totalPayment: totalPayment.toString()
      })
      .onConflictDoUpdate({
        target: yearlySummaries.year,
        set: {
          totalCurtailedEnergy: totalCurtailedEnergy.toString(),
          totalPayment: totalPayment.toString()
        }
      });
    
    log(`Updated yearly summary for ${year}`);
  } catch (error) {
    log(`Error updating yearly summary: ${(error as Error).message}`);
    throw error;
  }
}

/**
 * Process Bitcoin calculations for a specific miner model
 */
async function processBitcoinCalculations(minerModel: string): Promise<number> {
  log(`Processing Bitcoin calculations for ${minerModel}...`);
  
  try {
    // Get historical difficulty from DynamoDB or use fallback
    let difficulty;
    try {
      difficulty = await getDifficultyData(TARGET_DATE);
      log(`Using historical difficulty from DynamoDB: ${difficulty}`);
    } catch (error) {
      log(`Error getting difficulty data: ${(error as Error).message}`);
      // Get latest difficulty from database as fallback
      const latestDiff = await db
        .select({
          difficulty: historicalBitcoinCalculations.difficulty
        })
        .from(historicalBitcoinCalculations)
        .orderBy(sql`calculated_at DESC`)
        .limit(1);
        
      difficulty = latestDiff[0]?.difficulty || 113757508810853; // Default fallback difficulty
      log(`Using fallback difficulty: ${difficulty}`);
    }
    
    // Get all curtailment records for the target date
    const records = await db
      .select({
        settlementPeriod: curtailmentRecords.settlementPeriod,
        farmId: curtailmentRecords.farmId,
        volume: curtailmentRecords.volume
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    log(`Found ${records.length} curtailment records for ${TARGET_DATE}`);
    
    // Skip if no records found
    if (records.length === 0) {
      log(`No curtailment records found for ${TARGET_DATE}, skipping Bitcoin calculations`);
      return 0;
    }
    
    // Filter records with non-zero energy
    const validRecords = records.filter(r => {
      const volume = Math.abs(parseFloat(r.volume.toString()));
      return volume > 0 && !isNaN(volume);
    });
    
    log(`Found ${validRecords.length} valid curtailment records with non-zero energy`);
    
    // Process records in batches
    let totalBitcoin = 0;
    const batches = Math.ceil(validRecords.length / BATCH_SIZE);
    let successfulRecords = 0;
    
    for (let i = 0; i < batches; i++) {
      const start = i * BATCH_SIZE;
      const end = Math.min(start + BATCH_SIZE, validRecords.length);
      const batch = validRecords.slice(start, end);
      
      const insertPromises = [];
      
      for (const record of batch) {
        // Convert volume to positive number
        const mwh = Math.abs(parseFloat(record.volume.toString()));
        
        // Calculate Bitcoin mined
        const bitcoinMined = calculateBitcoin(mwh, minerModel, parseFloat(difficulty.toString()));
        totalBitcoin += bitcoinMined;
        
        // Prepare insert
        insertPromises.push(
          db.insert(historicalBitcoinCalculations).values({
            settlementDate: TARGET_DATE,
            settlementPeriod: Number(record.settlementPeriod),
            minerModel: minerModel,
            farmId: record.farmId,
            bitcoinMined: bitcoinMined.toString(),
            difficulty: difficulty.toString(),
            calculatedAt: new Date()
          }).onConflictDoUpdate({
            target: [
              historicalBitcoinCalculations.settlementDate, 
              historicalBitcoinCalculations.settlementPeriod, 
              historicalBitcoinCalculations.farmId, 
              historicalBitcoinCalculations.minerModel
            ],
            set: {
              bitcoinMined: bitcoinMined.toString(),
              difficulty: difficulty.toString(),
              calculatedAt: new Date()
            }
          })
        );
      }
      
      // Execute all inserts for this batch
      try {
        await Promise.all(insertPromises);
        successfulRecords += insertPromises.length;
        log(`Batch ${i+1}/${batches}: Processed ${insertPromises.length} records`);
      } catch (error) {
        log(`Error processing batch ${i+1}: ${(error as Error).message}`);
      }
    }
    
    log(`Successfully processed ${successfulRecords} Bitcoin calculations for ${minerModel}`);
    log(`Total Bitcoin calculated: ${totalBitcoin.toFixed(8)}`);
    
    // Get actual total from database to handle any potential calculation discrepancies
    const historicalTotal = await db
      .select({ total: sql<string>`SUM(bitcoin_mined::numeric)` })
      .from(historicalBitcoinCalculations)
      .where(and(
        eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
        eq(historicalBitcoinCalculations.minerModel, minerModel)
      ));
    
    const dbTotal = historicalTotal[0]?.total ? parseFloat(historicalTotal[0].total) : 0;
    log(`Database total for ${minerModel}: ${dbTotal.toFixed(8)} BTC`);
    
    // Return the actual database total
    return dbTotal;
  } catch (error) {
    log(`Error processing Bitcoin calculations for ${minerModel}: ${(error as Error).message}`);
    throw error;
  }
}

/**
 * Update Bitcoin daily summary
 */
async function updateBitcoinDailySummary(minerModel: string, totalBitcoin: number): Promise<void> {
  log(`Updating Bitcoin daily summary for ${TARGET_DATE} and ${minerModel}...`);
  
  try {
    // Create or update Bitcoin daily summary
    await db
      .insert(bitcoinDailySummaries)
      .values({
        summaryDate: TARGET_DATE,
        minerModel: minerModel,
        bitcoinMined: totalBitcoin.toString()
      })
      .onConflictDoUpdate({
        target: [bitcoinDailySummaries.summaryDate, bitcoinDailySummaries.minerModel],
        set: {
          bitcoinMined: totalBitcoin.toString()
        }
      });
    
    log(`Updated Bitcoin daily summary for ${TARGET_DATE} and ${minerModel}`);
  } catch (error) {
    log(`Error updating Bitcoin daily summary: ${(error as Error).message}`);
    throw error;
  }
}

/**
 * Update Bitcoin monthly summary
 */
async function updateBitcoinMonthlySummary(minerModel: string): Promise<void> {
  log(`Updating Bitcoin monthly summary for ${TARGET_DATE} and ${minerModel}...`);
  
  try {
    // Extract year and month from the target date
    const date = parseISO(TARGET_DATE);
    const yearMonth = format(date, 'yyyy-MM');
    
    // Calculate monthly totals from daily summaries
    const monthlyTotals = await db
      .select({
        totalBitcoinMined: sql<string>`SUM(${bitcoinDailySummaries.bitcoinMined}::numeric)`
      })
      .from(bitcoinDailySummaries)
      .where(and(
        sql`TO_CHAR(${bitcoinDailySummaries.summaryDate}, 'YYYY-MM') = ${yearMonth}`,
        eq(bitcoinDailySummaries.minerModel, minerModel)
      ));
    
    const totalBitcoinMined = parseFloat(monthlyTotals[0]?.totalBitcoinMined || '0');
    
    log(`Calculated monthly Bitcoin total for ${minerModel}: ${totalBitcoinMined.toFixed(8)} BTC`);
    
    // Update or insert monthly summary
    await db
      .insert(bitcoinMonthlySummaries)
      .values({
        yearMonth: yearMonth,
        minerModel: minerModel,
        bitcoinMined: totalBitcoinMined.toString()
      })
      .onConflictDoUpdate({
        target: [bitcoinMonthlySummaries.yearMonth, bitcoinMonthlySummaries.minerModel],
        set: {
          bitcoinMined: totalBitcoinMined.toString()
        }
      });
    
    log(`Updated Bitcoin monthly summary for ${yearMonth} and ${minerModel}`);
  } catch (error) {
    log(`Error updating Bitcoin monthly summary: ${(error as Error).message}`);
    throw error;
  }
}

/**
 * Update Bitcoin yearly summary
 */
async function updateBitcoinYearlySummary(minerModel: string): Promise<void> {
  log(`Updating Bitcoin yearly summary for ${TARGET_DATE} and ${minerModel}...`);
  
  try {
    // Extract year from the target date
    const year = TARGET_DATE.substring(0, 4);
    
    // Calculate yearly totals from monthly summaries
    const yearlyTotals = await db
      .select({
        totalBitcoinMined: sql<string>`SUM(${bitcoinMonthlySummaries.bitcoinMined}::numeric)`
      })
      .from(bitcoinMonthlySummaries)
      .where(and(
        sql`${bitcoinMonthlySummaries.yearMonth} LIKE ${`${year}-%`}`,
        eq(bitcoinMonthlySummaries.minerModel, minerModel)
      ));
    
    const totalBitcoinMined = parseFloat(yearlyTotals[0]?.totalBitcoinMined || '0');
    
    log(`Calculated yearly Bitcoin total for ${minerModel}: ${totalBitcoinMined.toFixed(8)} BTC`);
    
    // Update or insert yearly summary
    await db
      .insert(bitcoinYearlySummaries)
      .values({
        year: year,
        minerModel: minerModel,
        bitcoinMined: totalBitcoinMined.toString()
      })
      .onConflictDoUpdate({
        target: [bitcoinYearlySummaries.year, bitcoinYearlySummaries.minerModel],
        set: {
          bitcoinMined: totalBitcoinMined.toString()
        }
      });
    
    log(`Updated Bitcoin yearly summary for ${year} and ${minerModel}`);
  } catch (error) {
    log(`Error updating Bitcoin yearly summary: ${(error as Error).message}`);
    throw error;
  }
}

/**
 * Verify the results of the reprocessing
 */
async function verifyResults(): Promise<void> {
  log(`Verifying results for ${TARGET_DATE}...`);
  
  try {
    // 1. Check curtailment records
    const curtailmentTotal = await db
      .select({
        recordCount: sql<number>`COUNT(*)`,
        periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
        totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
        totalPayment: sql<string>`SUM(ABS(payment::numeric))`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    log(`Curtailment records verification:`);
    log(`- Total records: ${curtailmentTotal[0]?.recordCount || 0}`);
    log(`- Settlement periods: ${curtailmentTotal[0]?.periodCount || 0}/48`);
    log(`- Total volume: ${parseFloat(curtailmentTotal[0]?.totalVolume || '0').toFixed(2)} MWh`);
    log(`- Total payment: £${parseFloat(curtailmentTotal[0]?.totalPayment || '0').toFixed(2)}`);
    
    // 2. Check daily summary
    const dailySummary = await db
      .select()
      .from(dailySummaries)
      .where(eq(dailySummaries.summaryDate, TARGET_DATE));
    
    if (dailySummary.length > 0) {
      log(`Daily summary verification:`);
      log(`- Total energy: ${parseFloat(dailySummary[0].totalCurtailedEnergy?.toString() || '0').toFixed(2)} MWh`);
      log(`- Total payment: £${parseFloat(dailySummary[0].totalPayment?.toString() || '0').toFixed(2)}`);
    } else {
      log(`Warning: No daily summary found for ${TARGET_DATE}`);
    }
    
    // 3. Check Bitcoin calculations for each miner model
    for (const minerModel of MINER_MODELS) {
      const bitcoinTotal = await db
        .select({
          recordCount: sql<number>`COUNT(*)`,
          totalBitcoin: sql<string>`SUM(bitcoin_mined::numeric)`
        })
        .from(historicalBitcoinCalculations)
        .where(and(
          eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
          eq(historicalBitcoinCalculations.minerModel, minerModel)
        ));
      
      log(`Bitcoin calculations for ${minerModel}:`);
      log(`- Total records: ${bitcoinTotal[0]?.recordCount || 0}`);
      log(`- Total Bitcoin: ${parseFloat(bitcoinTotal[0]?.totalBitcoin || '0').toFixed(8)} BTC`);
      
      // 4. Check Bitcoin daily summary
      const bitcoinDaily = await db
        .select()
        .from(bitcoinDailySummaries)
        .where(and(
          eq(bitcoinDailySummaries.summaryDate, TARGET_DATE),
          eq(bitcoinDailySummaries.minerModel, minerModel)
        ));
      
      if (bitcoinDaily.length > 0) {
        log(`Bitcoin daily summary for ${minerModel}:`);
        log(`- Total Bitcoin: ${parseFloat(bitcoinDaily[0].bitcoinMined?.toString() || '0').toFixed(8)} BTC`);
      } else {
        log(`Warning: No Bitcoin daily summary found for ${TARGET_DATE} and ${minerModel}`);
      }
    }
    
    log(`Verification complete for ${TARGET_DATE}`);
  } catch (error) {
    log(`Error verifying results: ${(error as Error).message}`);
    throw error;
  }
}

/**
 * Run the complete reprocessing pipeline
 */
async function runReprocessing(): Promise<void> {
  const startTime = performance.now();
  
  try {
    log(`==== Starting reprocessing for ${TARGET_DATE} ====`);
    
    // Step 1: Clear existing data
    await clearExistingCurtailmentRecords();
    await clearExistingBitcoinCalculations();
    
    // Step 2: Process curtailment data
    await processCurtailmentData();
    
    // Step 3: Calculate daily summary
    await calculateDailySummary();
    
    // Step 4: Update monthly and yearly summaries
    await updateMonthlySummary();
    await updateYearlySummary();
    
    // Step 5: Process Bitcoin calculations for each miner model
    log(`\n==== Processing Bitcoin calculations for ${TARGET_DATE} ====\n`);
    
    for (const minerModel of MINER_MODELS) {
      log(`\n==== Processing ${minerModel} miner model ====\n`);
      
      // Process calculations and get total Bitcoin
      const totalBitcoin = await processBitcoinCalculations(minerModel);
      
      // Update daily summary
      await updateBitcoinDailySummary(minerModel, totalBitcoin);
      
      // Update monthly summary
      await updateBitcoinMonthlySummary(minerModel);
      
      // Update yearly summary
      await updateBitcoinYearlySummary(minerModel);
    }
    
    // Step 6: Verify results
    await verifyResults();
    
    const endTime = performance.now();
    const durationSeconds = ((endTime - startTime) / 1000).toFixed(2);
    
    log(`\n==== Reprocessing completed in ${durationSeconds}s ====\n`);
  } catch (error) {
    log(`\nERROR: Reprocessing failed: ${(error as Error).message}\n`);
    throw error;
  }
}

// Execute the reprocessing
runReprocessing()
  .then(() => {
    console.log('\nReprocessing completed successfully. Exiting...');
    process.exit(0);
  })
  .catch((error) => {
    console.error('\nReprocessing failed with error:', error);
    process.exit(1);
  });