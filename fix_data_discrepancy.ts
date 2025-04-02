/**
 * Fix Data Discrepancy
 * 
 * This script fixes the discrepancy in the March 28, 2025 data by updating
 * the elexon.ts service code to include both soFlag and cadlFlag records,
 * then reprocessing the data for that date.
 */

import { db } from "./db";
import { curtailmentRecords, dailySummaries, monthlySummaries, yearlySummaries } from "./db/schema";
import { eq, sql } from "drizzle-orm";
import axios from "axios";
import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const TARGET_DATE = '2025-03-28';
const ELEXON_BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1";
const BMU_MAPPING_PATH = path.join(__dirname, "server/data/bmuMapping.json");

// Utility function to delay between API calls
async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Step 1: Modify the elexon.ts service file
async function updateElexonService(): Promise<void> {
  console.log('Step 1: Updating Elexon service to include cadlFlag records...');
  
  try {
    const elexonServicePath = path.join(__dirname, 'server/services/elexon.ts');
    const content = await fs.readFile(elexonServicePath, 'utf8');
    
    // Check current implementation
    const soFlagOnlyPattern = /record\.soFlag/g;
    const correctedPattern = /(record\.soFlag \|\| record\.cadlFlag)/g;
    
    if (content.match(soFlagOnlyPattern) && !content.match(correctedPattern)) {
      console.log('Found soFlag-only filtering. Updating to include cadlFlag...');
      
      // Update the filtering logic to include cadlFlag
      const updatedContent = content
        .replace(
          'record.volume < 0 && record.soFlag && validWindFarmIds.has(record.id)',
          'record.volume < 0 && (record.soFlag || record.cadlFlag) && validWindFarmIds.has(record.id)'
        )
        .replace(
          'record.volume < 0 && record.soFlag && validWindFarmIds.has(record.id)',
          'record.volume < 0 && (record.soFlag || record.cadlFlag) && validWindFarmIds.has(record.id)'
        );
      
      await fs.writeFile(elexonServicePath, updatedContent, 'utf8');
      console.log('Elexon service updated successfully');
    } else if (content.match(correctedPattern)) {
      console.log('Elexon service is already correctly configured to include cadlFlag records');
    } else {
      console.log('Could not locate filter patterns in Elexon service. Manual inspection required.');
    }
  } catch (error) {
    console.error('Error updating Elexon service:', error);
    throw error;
  }
}

// Step 2: Clear existing data for March 28 to avoid duplicates
async function clearExistingData(): Promise<void> {
  console.log(`\nStep 2: Clearing existing data for ${TARGET_DATE}...`);
  
  try {
    // Clear curtailment records
    const result = await db.delete(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log(`Cleared ${result.count} curtailment records for ${TARGET_DATE}`);
    
    // Clear historical bitcoin calculations
    const { historicalBitcoinCalculations } = await import('./db/schema');
    const btcResult = await db.delete(historicalBitcoinCalculations)
      .where(eq(historicalBitcoinCalculations.date, TARGET_DATE));
    
    console.log(`Cleared ${btcResult.count} Bitcoin calculation records for ${TARGET_DATE}`);
    
    // Clear daily summary
    await db.delete(dailySummaries)
      .where(eq(dailySummaries.summaryDate, TARGET_DATE));
    
    console.log(`Cleared daily summary for ${TARGET_DATE}`);
  } catch (error) {
    console.error('Error clearing existing data:', error);
    throw error;
  }
}

// Step 3: Reprocess all periods using the correct filtering
async function reprocessAllPeriods(): Promise<{energy: string, payment: string}> {
  console.log(`\nStep 3: Reprocessing all 48 periods for ${TARGET_DATE}...`);
  
  try {
    // Import the optimized critical date processor
    const { processBatch, loadBmuMappings } = await import('./optimized_critical_date_processor');
    
    // Process all 48 periods
    const { windFarmIds, bmuLeadPartyMap } = await loadBmuMappings();
    
    // Process in batches to avoid timeout
    const batches = [
      { start: 1, end: 12 },
      { start: 13, end: 24 },
      { start: 25, end: 36 },
      { start: 37, end: 48 }
    ];
    
    for (const batch of batches) {
      console.log(`Processing periods ${batch.start}-${batch.end}...`);
      await processBatch(
        TARGET_DATE,
        batch.start,
        batch.end,
        windFarmIds,
        bmuLeadPartyMap
      );
      await delay(1000); // Brief delay between batches
    }
    
    // Get the totals after reprocessing
    const totals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
        totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log(`Reprocessing complete with new totals:`);
    console.log(`- Energy: ${totals[0].totalCurtailedEnergy} MWh`);
    console.log(`- Payment: £${totals[0].totalPayment}`);
    
    return {
      energy: totals[0].totalCurtailedEnergy,
      payment: totals[0].totalPayment
    };
  } catch (error) {
    console.error('Error reprocessing periods:', error);
    throw error;
  }
}

// Step 4: Update summaries
async function updateSummaries(energy: string, payment: string): Promise<void> {
  console.log(`\nStep 4: Updating summary tables...`);
  
  try {
    // Create daily summary
    await db.insert(dailySummaries).values({
      summaryDate: TARGET_DATE,
      totalCurtailedEnergy: energy,
      totalPayment: payment,
      totalWindGeneration: '0', // We don't have this data yet
      windOnshoreGeneration: '0',
      windOffshoreGeneration: '0',
      lastUpdated: new Date()
    });
    
    console.log(`Created daily summary for ${TARGET_DATE}`);
    
    // Update monthly summary
    const yearMonth = TARGET_DATE.substring(0, 7);
    const monthlyTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(${dailySummaries.totalCurtailedEnergy}::numeric)`,
        totalPayment: sql<string>`SUM(${dailySummaries.totalPayment}::numeric)`
      })
      .from(dailySummaries)
      .where(sql`date_trunc('month', ${dailySummaries.summaryDate}::date) = date_trunc('month', ${yearMonth + '-01'}::date)`);
    
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
      
      console.log(`Updated monthly summary for ${yearMonth}`);
    }
    
    // Update yearly summary
    const year = TARGET_DATE.substring(0, 4);
    const yearlyTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(${dailySummaries.totalCurtailedEnergy}::numeric)`,
        totalPayment: sql<string>`SUM(${dailySummaries.totalPayment}::numeric)`
      })
      .from(dailySummaries)
      .where(sql`date_trunc('year', ${dailySummaries.summaryDate}::date) = date_trunc('year', ${year + '-01-01'}::date)`);
    
    if (yearlyTotals[0].totalCurtailedEnergy && yearlyTotals[0].totalPayment) {
      await db.insert(yearlySummaries).values({
        year,
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
      
      console.log(`Updated yearly summary for ${year}`);
    }
  } catch (error) {
    console.error('Error updating summaries:', error);
    throw error;
  }
}

// Step 5: Update Bitcoin calculations
async function updateBitcoinCalculations(): Promise<void> {
  console.log(`\nStep 5: Updating Bitcoin calculations...`);
  
  try {
    const minerModels = ['S19J_PRO', 'S9', 'M20S'];
    const { processSingleDay } = await import('./server/services/bitcoinService');
    
    for (const minerModel of minerModels) {
      await processSingleDay(TARGET_DATE, minerModel);
      console.log(`- Processed Bitcoin calculations for ${minerModel}`);
    }
    
    console.log('Bitcoin calculations updated successfully');
    
    // Update monthly and yearly Bitcoin summaries
    const { updateBitcoinMonthlySummary, updateBitcoinYearlySummary } = await import('./server/services/bitcoinService');
    const yearMonth = TARGET_DATE.substring(0, 7);
    const year = TARGET_DATE.substring(0, 4);
    
    console.log(`Updating monthly Bitcoin summary for ${yearMonth}...`);
    for (const minerModel of minerModels) {
      await updateBitcoinMonthlySummary(yearMonth, minerModel);
    }
    
    console.log(`Updating yearly Bitcoin summary for ${year}...`);
    for (const minerModel of minerModels) {
      await updateBitcoinYearlySummary(year, minerModel);
    }
    
    console.log('Monthly and yearly Bitcoin summaries updated successfully');
  } catch (error) {
    console.error('Error updating Bitcoin calculations:', error);
    throw error;
  }
}

// Step 6: Verify the data
async function verifyData(): Promise<void> {
  console.log(`\nStep 6: Verifying updated data...`);
  
  try {
    // Verify curtailment records
    const recordCount = await db
      .select({ count: sql<number>`COUNT(*)` })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    // Get unique periods to ensure we have all 48
    const periods = await db
      .select({ period: curtailmentRecords.settlementPeriod })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE))
      .groupBy(curtailmentRecords.settlementPeriod);
    
    // Get totals
    const totals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
        totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log(`Verification Results for ${TARGET_DATE}:`);
    console.log(`- Total Records: ${recordCount[0].count}`);
    console.log(`- Settlement Periods: ${periods.length}/48`);
    console.log(`- Total Energy: ${totals[0].totalCurtailedEnergy} MWh`);
    console.log(`- Total Payment: £${totals[0].totalPayment}`);
    
    // Display target values for comparison
    const targetEnergy = 99904.22;
    const targetPayment = -3784089.62;
    
    const energyDiff = targetEnergy - parseFloat(totals[0].totalCurtailedEnergy);
    const paymentDiff = targetPayment - parseFloat(totals[0].totalPayment);
    
    console.log(`\nComparison to Target Values:`);
    console.log(`- Target Energy: ${targetEnergy} MWh (Diff: ${energyDiff.toFixed(2)} MWh)`);
    console.log(`- Target Payment: £${targetPayment} (Diff: £${paymentDiff.toFixed(2)})`);
    
    // Check Bitcoin calculations
    const { historicalBitcoinCalculations } = await import('./db/schema');
    const btcCalculations = await db
      .select({ count: sql<number>`COUNT(*)` })
      .from(historicalBitcoinCalculations)
      .where(eq(historicalBitcoinCalculations.date, TARGET_DATE));
    
    console.log(`- Bitcoin Calculations: ${btcCalculations[0].count}`);
    
    // Check summary records
    const dailySummaryExists = await db
      .select({ id: dailySummaries.id })
      .from(dailySummaries)
      .where(eq(dailySummaries.summaryDate, TARGET_DATE));
    
    console.log(`- Daily Summary Exists: ${dailySummaryExists.length > 0 ? 'Yes' : 'No'}`);
  } catch (error) {
    console.error('Error verifying data:', error);
    throw error;
  }
}

// Main function
async function main(): Promise<void> {
  console.log(`=== Fixing Data Discrepancy for ${TARGET_DATE} ===`);
  console.log(`Started at: ${new Date().toISOString()}`);
  
  try {
    // Step 1: Update Elexon service
    await updateElexonService();
    
    // Step 2: Clear existing data
    await clearExistingData();
    
    // Step 3: Reprocess all periods
    const { energy, payment } = await reprocessAllPeriods();
    
    // Step 4: Update summaries
    await updateSummaries(energy, payment);
    
    // Step 5: Update Bitcoin calculations
    await updateBitcoinCalculations();
    
    // Step 6: Verify data
    await verifyData();
    
    console.log(`\nData fix completed successfully at ${new Date().toISOString()}`);
  } catch (error) {
    console.error('Error during data fix process:', error);
    process.exit(1);
  }
}

// Execute main function
main().catch(error => {
  console.error('Unhandled error:', error);
  process.exit(1);
});