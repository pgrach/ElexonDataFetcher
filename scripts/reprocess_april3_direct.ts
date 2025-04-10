/**
 * Reprocess Curtailment Data for 2025-04-03
 * 
 * This script focuses on reprocessing the curtailment data for April 3, 2025
 * by directly fetching from the Elexon API and updating all dependent tables.
 */

import { db } from "../db";
import { 
  curtailmentRecords, 
  dailySummaries, 
  monthlySummaries,
  yearlySummaries
} from "../db/schema";
import { fetchBidsOffers } from "../server/services/elexon";
import { eq, sql } from "drizzle-orm";
import fs from "fs/promises";
import path from "path";

// Target date constants
const TARGET_DATE = "2025-04-03";
const YEAR_MONTH = TARGET_DATE.substring(0, 7); // "2025-04" 
const YEAR = TARGET_DATE.substring(0, 4); // "2025"

/**
 * Main reprocessing function
 */
async function reprocessData() {
  try {
    console.log(`\n======= REPROCESSING DATA FOR ${TARGET_DATE} =======`);
    console.log(`Started at: ${new Date().toISOString()}\n`);
    
    // Step 1: Clear existing data
    await clearExistingData();
    
    // Step 2: Process all 48 settlement periods
    await processAllPeriods();
    
    // Step 3: Update summaries
    await updateSummaries();
    
    console.log(`\n======= REPROCESSING COMPLETED SUCCESSFULLY =======`);
    console.log(`Completed at: ${new Date().toISOString()}`);
    
    process.exit(0);
  } catch (error) {
    console.error("ERROR DURING REPROCESSING:", error);
    process.exit(1);
  }
}

/**
 * Clear existing data for the target date
 */
async function clearExistingData(): Promise<void> {
  console.log("Step 1: Clearing existing data...");
  
  // Get existing curtailment record stats
  const existingRecords = await db
    .select({
      count: sql<number>`COUNT(*)::int`,
      periods: sql<number>`COUNT(DISTINCT settlement_period)::int`,
      totalVolume: sql<string>`SUM(ABS(volume::numeric))::text`,
      totalPayment: sql<string>`SUM(payment::numeric)::text`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
  
  const recordCount = existingRecords[0]?.count || 0;
  
  console.log(`Found ${recordCount} existing curtailment records`);
  if (recordCount > 0) {
    console.log(`Settlement Periods: ${existingRecords[0]?.periods || 0}/48`);
    console.log(`Total Volume: ${existingRecords[0]?.totalVolume || '0'} MWh`);
    console.log(`Total Payment: £${existingRecords[0]?.totalPayment || '0'}`);
  }
  
  // Clear existing curtailment records
  if (recordCount > 0) {
    await db.delete(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    console.log(`Deleted ${recordCount} existing curtailment records`);
  }
  
  // Clear existing daily summary
  await db.delete(dailySummaries)
    .where(eq(dailySummaries.summaryDate, TARGET_DATE));
  console.log(`Cleared daily summary for ${TARGET_DATE}`);
}

/**
 * Process all 48 settlement periods for the target date
 */
async function processAllPeriods(): Promise<void> {
  console.log("\nStep 2: Processing all 48 settlement periods...");
  
  // Batch size for processing periods
  const BATCH_SIZE = 12;
  let totalVolume = 0;
  let totalPayment = 0;
  let recordCount = 0;
  let periodsWithRecords = 0;
  
  // Process periods in batches
  for (let startPeriod = 1; startPeriod <= 48; startPeriod += BATCH_SIZE) {
    const endPeriod = Math.min(startPeriod + BATCH_SIZE - 1, 48);
    console.log(`Processing periods ${startPeriod}-${endPeriod}...`);
    
    // Process each period in the batch
    const periodPromises = [];
    for (let period = startPeriod; period <= endPeriod; period++) {
      periodPromises.push(processPeriod(period));
    }
    
    // Wait for all periods in the batch to complete
    const periodResults = await Promise.all(periodPromises);
    
    // Accumulate results
    for (const result of periodResults) {
      if (result.records > 0) {
        recordCount += result.records;
        totalVolume += result.volume;
        totalPayment += result.payment;
        periodsWithRecords++;
      }
    }
  }
  
  console.log(`\nProcessing completed:`);
  console.log(`- Records: ${recordCount}`);
  console.log(`- Periods with data: ${periodsWithRecords}/48`);
  console.log(`- Total Volume: ${totalVolume.toFixed(2)} MWh`);
  console.log(`- Total Payment: £${totalPayment.toFixed(2)}`);
}

/**
 * Process a single settlement period
 */
async function processPeriod(period: number): Promise<{
  period: number;
  records: number;
  volume: number;
  payment: number;
}> {
  try {
    // Get data from Elexon API
    const records = await fetchBidsOffers(TARGET_DATE, period);
    
    // Filter valid records (negative volume, has SoFlag or CadlFlag)
    const validRecords = records.filter(record =>
      record.volume < 0 && (record.soFlag || record.cadlFlag)
    );
    
    if (validRecords.length === 0) {
      return { period, records: 0, volume: 0, payment: 0 };
    }
    
    // Calculate totals for this period
    let periodVolume = 0;
    let periodPayment = 0;
    
    // Process each record
    for (const record of validRecords) {
      const volume = Math.abs(record.volume);
      const payment = volume * record.originalPrice * -1; // Make payment negative
      
      periodVolume += volume;
      periodPayment += payment;
      
      // Insert record into database
      await db.insert(curtailmentRecords).values({
        settlementDate: TARGET_DATE,
        settlementPeriod: period,
        farmId: record.id,
        leadPartyName: record.leadPartyName || '',
        volume: record.volume.toString(),
        payment: payment.toString(),
        originalPrice: record.originalPrice.toString(), 
        finalPrice: record.finalPrice.toString(), // Add final price
        createdAt: new Date()
      });
    }
    
    console.log(`  Period ${period}: ${validRecords.length} records, ${periodVolume.toFixed(2)} MWh, £${periodPayment.toFixed(2)}`);
    
    return {
      period,
      records: validRecords.length,
      volume: periodVolume,
      payment: periodPayment
    };
  } catch (error) {
    console.error(`Error processing period ${period}:`, error);
    return { period, records: 0, volume: 0, payment: 0 };
  }
}

/**
 * Update all summary tables
 */
async function updateSummaries(): Promise<void> {
  console.log("\nStep 3: Updating summary tables...");
  
  // Update daily summary
  await updateDailySummary();
  
  // Update monthly summary
  await updateMonthlySummary();
  
  // Update yearly summary
  await updateYearlySummary();
}

/**
 * Update daily summary
 */
async function updateDailySummary(): Promise<void> {
  console.log("Updating daily summary...");
  
  // Calculate totals from curtailment records
  const totals = await db
    .select({
      totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
      totalPayment: sql<string>`SUM(payment::numeric)`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
  
  if (!totals[0] || !totals[0].totalVolume) {
    console.log("No curtailment records found, skipping daily summary");
    return;
  }
  
  // Insert or update daily summary
  await db.insert(dailySummaries).values({
    summaryDate: TARGET_DATE,
    totalCurtailedEnergy: totals[0].totalVolume,
    totalPayment: totals[0].totalPayment,
    updatedAt: new Date()
  }).onConflictDoUpdate({
    target: [dailySummaries.summaryDate],
    set: {
      totalCurtailedEnergy: totals[0].totalVolume,
      totalPayment: totals[0].totalPayment,
      updatedAt: new Date()
    }
  });
  
  console.log(`Updated daily summary: ${totals[0].totalVolume} MWh, £${totals[0].totalPayment}`);
}

/**
 * Update monthly summary
 */
async function updateMonthlySummary(): Promise<void> {
  console.log("Updating monthly summary...");
  
  // Calculate monthly totals from daily summaries
  const monthlyTotals = await db
    .select({
      totalCurtailedEnergy: sql<string>`SUM(total_curtailed_energy::numeric)`,
      totalPayment: sql<string>`SUM(total_payment::numeric)`
    })
    .from(dailySummaries)
    .where(sql`summary_date::text LIKE ${YEAR_MONTH + '-%'}`);
  
  if (!monthlyTotals[0] || !monthlyTotals[0].totalCurtailedEnergy) {
    console.log("No daily summaries found for this month, skipping monthly summary");
    return;
  }
  
  // Insert or update monthly summary
  await db.insert(monthlySummaries).values({
    yearMonth: YEAR_MONTH,
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
  
  console.log(`Updated monthly summary for ${YEAR_MONTH}: ${monthlyTotals[0].totalCurtailedEnergy} MWh, £${monthlyTotals[0].totalPayment}`);
}

/**
 * Update yearly summary
 */
async function updateYearlySummary(): Promise<void> {
  console.log("Updating yearly summary...");
  
  // Calculate yearly totals from monthly summaries
  const yearlyTotals = await db
    .select({
      totalCurtailedEnergy: sql<string>`SUM(total_curtailed_energy::numeric)`,
      totalPayment: sql<string>`SUM(total_payment::numeric)`
    })
    .from(monthlySummaries)
    .where(sql`year_month::text LIKE ${YEAR + '-%'}`);
  
  if (!yearlyTotals[0] || !yearlyTotals[0].totalCurtailedEnergy) {
    console.log("No monthly summaries found for this year, skipping yearly summary");
    return;
  }
  
  // Insert or update yearly summary
  await db.insert(yearlySummaries).values({
    year: YEAR,
    totalCurtailedEnergy: yearlyTotals[0].totalCurtailedEnergy,
    totalPayment: yearlyTotals[0].totalPayment,
    updatedAt: new Date()
  }).onConflictDoUpdate({
    target: [yearlySummaries.year],
    set: {
      totalCurtailedEnergy: yearlyTotals[0].totalCurtailedEnergy,
      totalPayment: yearlyTotals[0].totalPayment,
      updatedAt: new Date()
    }
  });
  
  console.log(`Updated yearly summary for ${YEAR}: ${yearlyTotals[0].totalCurtailedEnergy} MWh, £${yearlyTotals[0].totalPayment}`);
}

// Execute the main function
reprocessData();