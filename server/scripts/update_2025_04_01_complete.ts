/**
 * Complete Update Script for 2025-04-01
 * 
 * This script performs a full data update for 2025-04-01 including:
 * 1. Reingesting curtailment records from Elexon API
 * 2. Updating daily summary
 * 3. Updating monthly summary (April 2025)
 * 4. Updating yearly summary (2025)
 * 5. Updating Bitcoin calculation tables
 */

import { db } from "@db";
import { dailySummaries, monthlySummaries, yearlySummaries, curtailmentRecords, historicalBitcoinCalculations } from "@db/schema";
import { eq, sql, and, not, exists } from "drizzle-orm";
import { format } from "date-fns";
import { processSingleDay } from "../services/bitcoinService";
import { fetchBidsOffers } from "../services/elexon";
import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from 'url';
import { ElexonBidOffer } from "../types/elexon";

// Constants
const TARGET_DATE = '2025-04-01';
const YEAR_MONTH = '2025-04';
const YEAR = '2025';
const BATCH_SIZE = 12; // Process 12 periods at a time
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S']; // Common miner models to calculate for

// Function to reingest curtailment records from Elexon API
async function reingestCurtailmentRecords(): Promise<void> {
  try {
    console.log('\n============================================');
    console.log(`STARTING CURTAILMENT REINGESTION (${TARGET_DATE})`);
    console.log('============================================\n');
    
    const startTime = Date.now();

    // Path setup for BMU mapping
    const __filename = fileURLToPath(import.meta.url);
    const __dirname = path.dirname(__filename);
    // Check both possible BMU mapping file names
    let BMU_MAPPING_PATH = path.join(__dirname, "../../data/bmu_mapping.json");
    if (!await fs.access(BMU_MAPPING_PATH).then(() => true).catch(() => false)) {
      // If bmu_mapping.json doesn't exist, try bmuMapping.json
      BMU_MAPPING_PATH = path.join(__dirname, "../../data/bmuMapping.json");
    }

    // Load wind farm BMU IDs
    console.log('Loading BMU mapping from:', BMU_MAPPING_PATH);
    const mappingContent = await fs.readFile(BMU_MAPPING_PATH, 'utf8');
    const bmuMapping = JSON.parse(mappingContent);
    console.log(`Loaded ${bmuMapping.length} BMU mappings`);
    
    const validWindFarmIds = new Set(
      bmuMapping
        .filter((bmu: any) => bmu.fuelType === "WIND")
        .map((bmu: any) => bmu.elexonBmUnit)
    );
    
    const bmuLeadPartyMap = new Map(
      bmuMapping
        .filter((bmu: any) => bmu.fuelType === "WIND")
        .map((bmu: any) => [bmu.elexonBmUnit, bmu.leadPartyName || 'Unknown'])
    );
    
    console.log(`Found ${validWindFarmIds.size} wind farm BMUs`);
    
    // Create an array to store all inserted record IDs for verification
    const insertedRecordIds: (string | number)[] = [];
    let totalVolume = 0;
    let totalPayment = 0;
    let recordsProcessed = 0;
    
    // Step 1: Clear existing records for the target date using a transaction
    console.log(`Clearing existing records for ${TARGET_DATE} in a transaction...`);
    
    // Implement with a simple transaction to prevent race conditions
    // First run a count to see if we need to delete anything
    const recordCount = await db.select({
      count: sql<number>`COUNT(*)::int`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log(`Found ${recordCount[0]?.count || 0} existing records for ${TARGET_DATE}`);
    
    if (recordCount[0]?.count > 0) {
      // Delete existing records
      const deleteResult = await db.delete(curtailmentRecords)
        .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
      
      // Verify deletion by checking count again
      const afterDeleteCount = await db.select({
        count: sql<number>`COUNT(*)::int`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
      
      if (afterDeleteCount[0]?.count > 0) {
        console.error(`Failed to clear all records! Still have ${afterDeleteCount[0].count} records.`);
        throw new Error('Failed to clear existing records completely');
      }
    }
    
    console.log(`Successfully cleared existing records for ${TARGET_DATE}`);

    // Step 2: Process all 48 periods in batches
    for (let startPeriod = 1; startPeriod <= 48; startPeriod += BATCH_SIZE) {
      const endPeriod = Math.min(startPeriod + BATCH_SIZE - 1, 48);
      const periodPromises = [];
      
      console.log(`Processing periods ${startPeriod} to ${endPeriod}...`);
      
      for (let period = startPeriod; period <= endPeriod; period++) {
        periodPromises.push((async () => {
          try {
            // Fetch data from Elexon API
            const records = await fetchBidsOffers(TARGET_DATE, period);
            
            // Filter for valid wind farm curtailment records
            const validRecords = records.filter(record =>
              record.volume < 0 && 
              (record.soFlag || record.cadlFlag) &&
              validWindFarmIds.has(record.id)
            );
            
            let periodTotal = { volume: 0, payment: 0 };
            
            if (validRecords.length > 0) {
              console.log(`[${TARGET_DATE} P${period}] Processing ${validRecords.length} records`);
            }
            
            // Process all records in this period
            if (validRecords.length > 0) {
              // Calculate totals and prepare values for batch insert
              const valuesToInsert = validRecords.map(record => {
                const volume = Math.abs(record.volume);
                const payment = volume * record.originalPrice;
                // Make sure we have a string for leadPartyName
                const leadPartyName = (bmuLeadPartyMap.get(record.id) || 'Unknown') as string;
                
                periodTotal.volume += volume;
                periodTotal.payment += payment;
                
                return {
                  settlementDate: TARGET_DATE,
                  settlementPeriod: period,
                  farmId: record.id,
                  leadPartyName: leadPartyName,
                  soFlag: record.soFlag,
                  cadlFlag: record.cadlFlag || false,
                  volume: record.volume.toString(),
                  originalPrice: record.originalPrice.toString(),
                  payment: payment.toString(),
                  finalPrice: record.finalPrice ? record.finalPrice.toString() : "0"
                };
              });
              
              try {
                // Batch insert all records for this period at once
                const result = await db.insert(curtailmentRecords).values(valuesToInsert);
                recordsProcessed += validRecords.length;
                
                // Log success
                console.log(`[${TARGET_DATE} P${period}] Successfully inserted ${validRecords.length} records`);
              } catch (insertError) {
                console.error(`[${TARGET_DATE} P${period}] Error batch inserting records:`, insertError);
              }
            }
            
            if (periodTotal.volume > 0) {
              console.log(`[${TARGET_DATE} P${period}] Total: ${periodTotal.volume.toFixed(2)} MWh, £${periodTotal.payment.toFixed(2)}`);
            }
            
            totalVolume += periodTotal.volume;
            totalPayment += periodTotal.payment;
            
            return periodTotal;
          } catch (error) {
            console.error(`Error processing period ${period} for date ${TARGET_DATE}:`, error);
            return { volume: 0, payment: 0 };
          }
        })());
      }
      
      // Wait for all period promises to complete
      await Promise.all(periodPromises);
    }
    
    console.log(`\n=== Reingestion Summary for ${TARGET_DATE} ===`);
    console.log(`Records processed: ${recordsProcessed}`);
    console.log(`Total volume: ${totalVolume.toFixed(2)} MWh`);
    console.log(`Total payment: £${totalPayment.toFixed(2)}`);
    
    const endTime = Date.now();
    console.log(`Reingestion completed in ${((endTime - startTime) / 1000).toFixed(2)} seconds.`);
    
  } catch (error) {
    console.error('Error during reingestion:', error);
    throw error;
  }
}

// Function to update the summary tables
async function updateSummaryTables(): Promise<void> {
  try {
    console.log('\n============================================');
    console.log(`UPDATING SUMMARY TABLES FOR ${TARGET_DATE}`);
    console.log('============================================\n');
    
    const startTime = Date.now();
    
    // Step 1: Update daily summary
    console.log(`\n=== Updating Daily Summary for ${TARGET_DATE} ===`);
    
    // Calculate totals from curtailment records
    const totals = await db
      .select({
        totalCurtailedEnergy: sql`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
        totalPayment: sql`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    if (!totals[0] || !totals[0].totalCurtailedEnergy) {
      console.log('No curtailment records found for this date, setting summary to zero values');
      totals[0] = {
        totalCurtailedEnergy: '0',
        totalPayment: '0'
      };
    }
    
    // Update daily summary
    await db.insert(dailySummaries).values({
      summaryDate: TARGET_DATE,
      totalCurtailedEnergy: totals[0].totalCurtailedEnergy?.toString() || '0',
      totalPayment: totals[0].totalPayment?.toString() || '0'
    }).onConflictDoUpdate({
      target: [dailySummaries.summaryDate],
      set: {
        totalCurtailedEnergy: totals[0].totalCurtailedEnergy?.toString() || '0',
        totalPayment: totals[0].totalPayment?.toString() || '0'
      }
    });
    
    console.log('Daily summary updated:', {
      energy: `${Number(totals[0].totalCurtailedEnergy || 0).toFixed(2)} MWh`,
      payment: `£${Number(totals[0].totalPayment || 0).toFixed(2)}`
    });
    
    // Step 2: Update monthly summary
    console.log(`\n=== Updating Monthly Summary for ${YEAR_MONTH} ===`);
    
    // Calculate monthly totals from daily summaries
    const monthlyTotals = await db
      .select({
        totalCurtailedEnergy: sql`SUM(${dailySummaries.totalCurtailedEnergy}::numeric)`,
        totalPayment: sql`SUM(${dailySummaries.totalPayment}::numeric)`
      })
      .from(dailySummaries)
      .where(sql`date_trunc('month', ${dailySummaries.summaryDate}::date) = date_trunc('month', ${TARGET_DATE}::date)`);
    
    if (!monthlyTotals[0] || !monthlyTotals[0].totalCurtailedEnergy) {
      console.log('No daily summaries found for this month, setting monthly summary to zero values');
      monthlyTotals[0] = {
        totalCurtailedEnergy: '0',
        totalPayment: '0'
      };
    }
    
    // Update monthly summary
    await db.insert(monthlySummaries).values({
      yearMonth: YEAR_MONTH,
      totalCurtailedEnergy: monthlyTotals[0].totalCurtailedEnergy?.toString() || '0',
      totalPayment: monthlyTotals[0].totalPayment?.toString() || '0',
      updatedAt: new Date()
    }).onConflictDoUpdate({
      target: [monthlySummaries.yearMonth],
      set: {
        totalCurtailedEnergy: monthlyTotals[0].totalCurtailedEnergy?.toString() || '0',
        totalPayment: monthlyTotals[0].totalPayment?.toString() || '0',
        updatedAt: new Date()
      }
    });
    
    console.log('Monthly summary updated:', {
      energy: `${Number(monthlyTotals[0].totalCurtailedEnergy || 0).toFixed(2)} MWh`,
      payment: `£${Number(monthlyTotals[0].totalPayment || 0).toFixed(2)}`
    });
    
    // Step 3: Update yearly summary
    console.log(`\n=== Updating Yearly Summary for ${YEAR} ===`);
    
    // Calculate yearly totals from daily summaries
    const yearlyTotals = await db
      .select({
        totalCurtailedEnergy: sql`SUM(${dailySummaries.totalCurtailedEnergy}::numeric)`,
        totalPayment: sql`SUM(${dailySummaries.totalPayment}::numeric)`
      })
      .from(dailySummaries)
      .where(sql`date_trunc('year', ${dailySummaries.summaryDate}::date) = date_trunc('year', ${TARGET_DATE}::date)`);
    
    if (!yearlyTotals[0] || !yearlyTotals[0].totalCurtailedEnergy) {
      console.log('No daily summaries found for this year, setting yearly summary to zero values');
      yearlyTotals[0] = {
        totalCurtailedEnergy: '0',
        totalPayment: '0'
      };
    }
    
    // Update yearly summary
    await db.insert(yearlySummaries).values({
      year: YEAR,
      totalCurtailedEnergy: yearlyTotals[0].totalCurtailedEnergy?.toString() || '0',
      totalPayment: yearlyTotals[0].totalPayment?.toString() || '0',
      updatedAt: new Date()
    }).onConflictDoUpdate({
      target: [yearlySummaries.year],
      set: {
        totalCurtailedEnergy: yearlyTotals[0].totalCurtailedEnergy?.toString() || '0',
        totalPayment: yearlyTotals[0].totalPayment?.toString() || '0',
        updatedAt: new Date()
      }
    });
    
    console.log('Yearly summary updated:', {
      energy: `${Number(yearlyTotals[0].totalCurtailedEnergy || 0).toFixed(2)} MWh`,
      payment: `£${Number(yearlyTotals[0].totalPayment || 0).toFixed(2)}`
    });
    
    const endTime = Date.now();
    console.log(`\nSummary table updates completed in ${((endTime - startTime) / 1000).toFixed(2)} seconds.`);
    
  } catch (error) {
    console.error('Error updating summary tables:', error);
    throw error;
  }
}

// Function to update Bitcoin calculation tables
async function updateBitcoinCalculations(): Promise<void> {
  try {
    console.log('\n============================================');
    console.log(`UPDATING BITCOIN CALCULATIONS FOR ${TARGET_DATE}`);
    console.log('============================================\n');
    
    const startTime = Date.now();
    
    // Delete existing Bitcoin calculations for this date with verification
    console.log(`Deleting existing Bitcoin calculations for ${TARGET_DATE}...`);
    
    // First check if we have records to delete
    const bitcoinRecordCount = await db.select({
      count: sql<number>`COUNT(*)::int`
    })
    .from(historicalBitcoinCalculations)
    .where(eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE));
    
    console.log(`Found ${bitcoinRecordCount[0]?.count || 0} existing Bitcoin calculations for ${TARGET_DATE}`);
    
    if (bitcoinRecordCount[0]?.count > 0) {
      // Delete existing records
      await db.delete(historicalBitcoinCalculations)
        .where(eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE));
      
      // Verify deletion
      const afterDeleteCount = await db.select({
        count: sql<number>`COUNT(*)::int`
      })
      .from(historicalBitcoinCalculations)
      .where(eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE));
      
      if (afterDeleteCount[0]?.count > 0) {
        console.error(`Failed to clear all Bitcoin calculations! Still have ${afterDeleteCount[0].count} records.`);
        throw new Error('Failed to clear existing Bitcoin calculations completely');
      }
    }
    
    console.log(`Successfully cleared existing Bitcoin calculations for ${TARGET_DATE}`);
    
    // Process Bitcoin calculations for each miner model
    for (const minerModel of MINER_MODELS) {
      console.log(`\n=== Processing Bitcoin calculations for ${minerModel} ===`);
      await processSingleDay(TARGET_DATE, minerModel);
    }
    
    const endTime = Date.now();
    console.log(`\nBitcoin calculations completed in ${((endTime - startTime) / 1000).toFixed(2)} seconds.`);
    
  } catch (error) {
    console.error('Error updating Bitcoin calculations:', error);
    throw error;
  }
}

// Main function to run the entire process
async function runFullUpdate(): Promise<void> {
  try {
    console.log('\n============================================');
    console.log(`STARTING FULL UPDATE FOR ${TARGET_DATE}`);
    console.log('============================================\n');
    
    const startTime = Date.now();
    
    // Step 1: Reingest curtailment records
    await reingestCurtailmentRecords();
    
    // Step 2: Update summary tables
    await updateSummaryTables();
    
    // Step 3: Update Bitcoin calculations
    await updateBitcoinCalculations();
    
    const endTime = Date.now();
    console.log('\n============================================');
    console.log('FULL UPDATE COMPLETED SUCCESSFULLY');
    console.log(`Total duration: ${((endTime - startTime) / 1000).toFixed(2)} seconds`);
    console.log('============================================\n');
    
  } catch (error) {
    console.error('\n============================================');
    console.error('FULL UPDATE FAILED');
    console.error('Error:', error);
    console.error('============================================\n');
    process.exit(1);
  }
}

// Run the script if called directly
// In ES modules mode, we don't have require.main === module
// Just run the script directly

// Export functions for use in other scripts
export {
  reingestCurtailmentRecords,
  updateSummaryTables,
  updateBitcoinCalculations,
  runFullUpdate
};

// Run the function when script is executed directly
runFullUpdate();