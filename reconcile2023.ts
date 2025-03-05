/**
 * Reconcile 2023 Data
 * 
 * This script analyzes the data reconciliation between curtailment_records and 
 * historicalBitcoinCalculations tables for all 2023 data, identifying and fixing
 * any missing Bitcoin calculations.
 * 
 * For each curtailment_record, there should be 3 corresponding historicalBitcoinCalculations
 * (one for each miner model: S19J_PRO, M20S, and S9).
 */

import { db } from "./db";
import { curtailmentRecords, historicalBitcoinCalculations } from "./db/schema";
import { and, count, eq, sql } from "drizzle-orm";
import { minerModels } from "./server/types/bitcoin";
import { processDailyCurtailment } from "./server/services/curtailment";

// Utility functions
interface ReconciliationStats {
  date: string;
  totalCurtailmentRecords: number;
  totalPeriods: number;
  totalFarms: number;
  missingCalculations: {
    [key: string]: { // miner model
      count: number;
      periods: number[];
    }
  },
  fixed: boolean;
}

/**
 * Sleep for specified milliseconds
 */
async function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Get all dates in 2023 with curtailment records
 */
async function get2023Dates(): Promise<string[]> {
  console.log("Fetching all 2023 dates with curtailment records...");
  
  const result = await db.execute(sql`
    SELECT DISTINCT settlement_date::text
    FROM curtailment_records 
    WHERE settlement_date >= '2023-01-01' 
    AND settlement_date <= '2023-12-31'
    ORDER BY settlement_date
  `);
  
  const dates = result.rows.map((row: any) => row.settlement_date as string);
  console.log(`Found ${dates.length} dates in 2023 with curtailment records`);
  return dates;
}

/**
 * Analyze a specific date to check for missing Bitcoin calculations
 */
async function analyzeDate(date: string): Promise<ReconciliationStats> {
  console.log(`Analyzing date ${date}...`);
  
  // Get total number of curtailment records for this date
  const curtailmentResult = await db.select({
    count: count(),
  }).from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date));
  
  const totalCurtailmentRecords = curtailmentResult[0].count;
  
  // Get distinct periods and farms for this date
  const periodsResult = await db.execute(sql`
    SELECT COUNT(DISTINCT settlement_period) as periods, COUNT(DISTINCT farm_id) as farms
    FROM curtailment_records
    WHERE settlement_date = ${date}
  `);
  
  const totalPeriods = parseInt(periodsResult.rows[0].periods as string, 10);
  const totalFarms = parseInt(periodsResult.rows[0].farms as string, 10);
  
  // Check calculations for each miner model
  const stats: ReconciliationStats = {
    date,
    totalCurtailmentRecords,
    totalPeriods,
    totalFarms,
    missingCalculations: {},
    fixed: false
  };
  
  // Check for each miner model
  for (const minerModel of Object.keys(minerModels)) {
    // Count bitcoin calculations for this date and model
    const calculationsResult = await db.select({
      count: count(),
    }).from(historicalBitcoinCalculations)
      .where(and(
        eq(historicalBitcoinCalculations.settlementDate, date),
        eq(historicalBitcoinCalculations.minerModel, minerModel)
      ));
    
    const calculationsCount = calculationsResult[0].count;
    const missingCount = totalCurtailmentRecords - calculationsCount;
    
    if (missingCount > 0) {
      // Get missing periods
      const periodsWithMissingCalculations = await db.execute(sql`
        SELECT DISTINCT c.settlement_period
        FROM curtailment_records c
        LEFT JOIN historical_bitcoin_calculations h
        ON c.settlement_date = h.settlement_date
        AND c.settlement_period = h.settlement_period
        AND c.farm_id = h.farm_id
        AND h.miner_model = ${minerModel}
        WHERE c.settlement_date = ${date}
        AND h.id IS NULL
        ORDER BY c.settlement_period
      `);
      
      const missingPeriods = periodsWithMissingCalculations.rows.map((row: any) => 
        parseInt(row.settlement_period, 10)
      );
      
      stats.missingCalculations[minerModel] = {
        count: missingCount,
        periods: missingPeriods
      };
    }
  }
  
  return stats;
}

/**
 * Fix missing calculations for a date
 */
async function fixMissingCalculations(date: string, stats: ReconciliationStats): Promise<boolean> {
  console.log(`Fixing missing calculations for ${date}...`);
  
  try {
    // Reprocess the entire day's calculations through the curtailment service
    await processDailyCurtailment(date);
    
    // Verify that the fix worked
    const verificationStats = await analyzeDate(date);
    
    // Check if all missing calculations were fixed
    let allFixed = true;
    for (const model of Object.keys(minerModels)) {
      if (verificationStats.missingCalculations[model]?.count > 0) {
        allFixed = false;
        console.error(`Failed to fix all missing calculations for ${date}, model ${model}`);
        console.error(`  Still missing: ${verificationStats.missingCalculations[model].count} records`);
      }
    }
    
    if (allFixed) {
      console.log(`Successfully fixed all missing calculations for ${date}`);
      return true;
    }
    
    return false;
  } catch (error) {
    console.error(`Error fixing calculations for ${date}:`, error);
    return false;
  }
}

/**
 * Main function to reconcile 2023 data
 */
async function reconcile2023Data() {
  console.log("Starting 2023 data reconciliation...");
  
  // Get all dates in 2023 with curtailment records
  const dates = await get2023Dates();
  
  // Summary stats
  let totalMissingCalculations = 0;
  let missingByModel: Record<string, number> = {};
  const datesToFix: string[] = [];
  
  // Initialize model counters
  for (const model of Object.keys(minerModels)) {
    missingByModel[model] = 0;
  }
  
  // Analyze all dates
  console.log("Analyzing all 2023 dates for missing Bitcoin calculations...");
  for (let i = 0; i < dates.length; i++) {
    const date = dates[i];
    const stats = await analyzeDate(date);
    
    // Track missing calculations
    let dateHasMissing = false;
    
    for (const [model, data] of Object.entries(stats.missingCalculations)) {
      if (data.count > 0) {
        totalMissingCalculations += data.count;
        missingByModel[model] += data.count;
        dateHasMissing = true;
        
        console.log(`${date}: Missing ${data.count} calculations for ${model}`);
        console.log(`  Missing periods: ${data.periods.join(', ')}`);
      }
    }
    
    if (dateHasMissing) {
      datesToFix.push(date);
    }
    
    // Simple progress indicator
    if ((i + 1) % 10 === 0 || i === dates.length - 1) {
      console.log(`Progress: ${i + 1}/${dates.length} dates analyzed`);
    }
    
    // Avoid overloading the database with too many queries at once
    await sleep(500);
  }
  
  // Print summary of analysis
  console.log("\n===== ANALYSIS SUMMARY =====");
  console.log(`Total dates analyzed: ${dates.length}`);
  console.log(`Dates with missing calculations: ${datesToFix.length}`);
  console.log(`Total missing calculations: ${totalMissingCalculations}`);
  console.log("Missing by model:");
  for (const [model, count] of Object.entries(missingByModel)) {
    console.log(`  ${model}: ${count}`);
  }
  
  // Fix missing calculations if found
  if (datesToFix.length > 0) {
    console.log("\n===== FIXING MISSING CALCULATIONS =====");
    console.log(`Will process ${datesToFix.length} dates with missing calculations`);
    
    const fixedDates: string[] = [];
    const failedDates: string[] = [];
    
    for (let i = 0; i < datesToFix.length; i++) {
      const date = datesToFix[i];
      console.log(`Processing date ${i + 1}/${datesToFix.length}: ${date}`);
      
      const stats = await analyzeDate(date);
      const success = await fixMissingCalculations(date, stats);
      
      if (success) {
        fixedDates.push(date);
      } else {
        failedDates.push(date);
      }
      
      // Progress update
      console.log(`Progress: ${i + 1}/${datesToFix.length} dates processed`);
      console.log(`Success: ${fixedDates.length}, Failed: ${failedDates.length}`);
      
      // Avoid overloading the database with too many operations at once
      await sleep(2000);
    }
    
    // Final summary
    console.log("\n===== RECONCILIATION COMPLETE =====");
    console.log(`Total dates processed: ${datesToFix.length}`);
    console.log(`Successfully fixed: ${fixedDates.length}`);
    console.log(`Failed to fix: ${failedDates.length}`);
    
    if (failedDates.length > 0) {
      console.log("Failed dates:");
      failedDates.forEach(date => console.log(`  ${date}`));
    }
  } else {
    console.log("\n===== RECONCILIATION COMPLETE =====");
    console.log("No missing calculations found. All 2023 data is already reconciled!");
  }
}

// Run the reconciliation
reconcile2023Data()
  .catch(error => {
    console.error("Error during reconciliation:", error);
    process.exit(1);
  })
  .finally(() => {
    console.log("Reconciliation script finished");
  });