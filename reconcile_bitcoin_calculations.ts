/**
 * This script identifies and analyzes missing bitcoin calculations in relation to curtailment records.
 * It provides detailed reports on which dates, periods, and farm_ids are missing calculations.
 */

import { db } from "@db";
import { curtailmentRecords, historicalBitcoinCalculations } from "@db/schema";
import { sql, and, eq, between } from "drizzle-orm";
import { format, parseISO, eachMonthOfInterval } from "date-fns";

// Configuration
const MINER_MODELS = ["S19J_PRO", "M20S", "S9"];
const DEFAULT_MINER_MODEL = "S19J_PRO";

// Date range for analysis - focusing on February 2025
const START_DATE = "2025-02-01"; // Focus on February 2025
const END_DATE = "2025-02-28";   // End of February 2025

interface ReconciliationSummary {
  totalCurtailmentRecords: number;
  totalBitcoinCalculations: number;
  missingCalculations: number;
  recordsByMinerModel: Record<string, number>;
  missingByMonth: Record<string, number>;
  missingByMinerModel: Record<string, number>;
  sampleMissingRecords: any[];
}

/**
 * Get summary statistics for the reconciliation
 */
async function getReconciliationSummary(): Promise<ReconciliationSummary> {
  // Get total counts
  const curtailmentCount = await db
    .select({ count: sql<number>`COUNT(*)` })
    .from(curtailmentRecords);

  const bitcoinCount = await db
    .select({ count: sql<number>`COUNT(*)` })
    .from(historicalBitcoinCalculations);

  // Get counts by miner model
  const modelCounts = await db
    .select({
      minerModel: historicalBitcoinCalculations.minerModel,
      count: sql<number>`COUNT(*)`
    })
    .from(historicalBitcoinCalculations)
    .groupBy(historicalBitcoinCalculations.minerModel);

  // Count missing calculations for each miner model
  const missingByModel: Record<string, number> = {};
  for (const model of MINER_MODELS) {
    const missing = await db.execute(sql`
      WITH curtailment_keys AS (
        SELECT DISTINCT settlement_date, settlement_period, farm_id
        FROM curtailment_records
      ),
      bitcoin_keys AS (
        SELECT DISTINCT settlement_date, settlement_period, farm_id 
        FROM historical_bitcoin_calculations
        WHERE miner_model = ${model}
      )
      SELECT COUNT(*) as missing_count
      FROM curtailment_keys c
      WHERE NOT EXISTS (
        SELECT 1 FROM bitcoin_keys b
        WHERE b.settlement_date = c.settlement_date
        AND b.settlement_period = c.settlement_period
        AND b.farm_id = c.farm_id
      )
    `);
    missingByModel[model] = Number(missing.rows[0]?.missing_count || 0);
  }

  // Get missing calculations by month
  const startDate = parseISO(START_DATE);
  const endDate = parseISO(END_DATE);
  const months = eachMonthOfInterval({ start: startDate, end: endDate });
  
  const missingByMonth: Record<string, number> = {};
  for (const month of months) {
    const monthStr = format(month, "yyyy-MM");
    const firstDay = format(month, "yyyy-MM-01");
    const lastDay = format(month, "yyyy-MM-") + new Date(month.getFullYear(), month.getMonth() + 1, 0).getDate();
    
    const missing = await db.execute(sql`
      WITH curtailment_keys AS (
        SELECT DISTINCT settlement_date, settlement_period, farm_id
        FROM curtailment_records
        WHERE settlement_date BETWEEN ${firstDay} AND ${lastDay}
      ),
      bitcoin_keys AS (
        SELECT DISTINCT settlement_date, settlement_period, farm_id 
        FROM historical_bitcoin_calculations
        WHERE settlement_date BETWEEN ${firstDay} AND ${lastDay}
        AND miner_model = ${DEFAULT_MINER_MODEL}
      )
      SELECT COUNT(*) as missing_count
      FROM curtailment_keys c
      WHERE NOT EXISTS (
        SELECT 1 FROM bitcoin_keys b
        WHERE b.settlement_date = c.settlement_date
        AND b.settlement_period = c.settlement_period
        AND b.farm_id = c.farm_id
      )
    `);
    missingByMonth[monthStr] = Number(missing.rows[0]?.missing_count || 0);
  }

  // Get a sample of missing records for inspection
  const sampleMissing = await db.execute(sql`
    WITH curtailment_keys AS (
      SELECT DISTINCT settlement_date, settlement_period, farm_id
      FROM curtailment_records
    ),
    bitcoin_keys AS (
      SELECT DISTINCT settlement_date, settlement_period, farm_id 
      FROM historical_bitcoin_calculations
      WHERE miner_model = ${DEFAULT_MINER_MODEL}
    )
    SELECT c.settlement_date, c.settlement_period, c.farm_id
    FROM curtailment_keys c
    WHERE NOT EXISTS (
      SELECT 1 FROM bitcoin_keys b
      WHERE b.settlement_date = c.settlement_date
      AND b.settlement_period = c.settlement_period
      AND b.farm_id = c.farm_id
    )
    ORDER BY c.settlement_date DESC, c.settlement_period DESC
    LIMIT 10
  `);

  return {
    totalCurtailmentRecords: Number(curtailmentCount[0]?.count || 0),
    totalBitcoinCalculations: Number(bitcoinCount[0]?.count || 0),
    missingCalculations: missingByModel[DEFAULT_MINER_MODEL] || 0,
    recordsByMinerModel: Object.fromEntries(modelCounts.map(mc => [mc.minerModel, mc.count])),
    missingByMonth: Object.entries(missingByMonth)
      .filter(([_, count]) => count > 0)
      .reduce((acc, [month, count]) => ({ ...acc, [month]: count }), {}),
    missingByMinerModel: missingByModel,
    sampleMissingRecords: sampleMissing.rows
  };
}

/**
 * Find days with the most missing calculations
 */
async function findProblemDays(): Promise<any[]> {
  return await db.execute(sql`
    WITH curtailment_keys AS (
      SELECT settlement_date, settlement_period, farm_id
      FROM curtailment_records
    ),
    bitcoin_keys AS (
      SELECT settlement_date, settlement_period, farm_id 
      FROM historical_bitcoin_calculations
      WHERE miner_model = ${DEFAULT_MINER_MODEL}
    ),
    missing_records AS (
      SELECT c.settlement_date
      FROM curtailment_keys c
      WHERE NOT EXISTS (
        SELECT 1 FROM bitcoin_keys b
        WHERE b.settlement_date = c.settlement_date
        AND b.settlement_period = c.settlement_period
        AND b.farm_id = c.farm_id
      )
    )
    SELECT 
      settlement_date, 
      COUNT(*) as missing_count
    FROM missing_records
    GROUP BY settlement_date
    ORDER BY missing_count DESC
    LIMIT 10
  `);
}

/**
 * Analyze a specific date for missing calculations
 */
async function analyzeDateMissingCalculations(date: string): Promise<any> {
  // Get curtailment records for the date
  const curtailmentForDate = await db
    .select({
      count: sql<number>`COUNT(*)`,
      distinctPeriods: sql<number>`COUNT(DISTINCT settlement_period)`,
      distinctFarms: sql<number>`COUNT(DISTINCT farm_id)`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date));

  // Get bitcoin calculations for the date
  const bitcoinForDate = await db
    .select({
      count: sql<number>`COUNT(*)`,
      distinctPeriods: sql<number>`COUNT(DISTINCT settlement_period)`,
      distinctFarms: sql<number>`COUNT(DISTINCT farm_id)`
    })
    .from(historicalBitcoinCalculations)
    .where(
      and(
        eq(historicalBitcoinCalculations.settlementDate, date),
        eq(historicalBitcoinCalculations.minerModel, DEFAULT_MINER_MODEL)
      )
    );

  // Find missing calculations for the date
  const missingForDate = await db.execute(sql`
    WITH curtailment_keys AS (
      SELECT DISTINCT settlement_period, farm_id
      FROM curtailment_records
      WHERE settlement_date = ${date}
    ),
    bitcoin_keys AS (
      SELECT DISTINCT settlement_period, farm_id 
      FROM historical_bitcoin_calculations
      WHERE settlement_date = ${date}
      AND miner_model = ${DEFAULT_MINER_MODEL}
    )
    SELECT c.settlement_period, c.farm_id
    FROM curtailment_keys c
    WHERE NOT EXISTS (
      SELECT 1 FROM bitcoin_keys b
      WHERE b.settlement_period = c.settlement_period
      AND b.farm_id = c.farm_id
    )
    ORDER BY c.settlement_period
  `);

  return {
    date,
    curtailmentRecords: curtailmentForDate[0],
    bitcoinCalculations: bitcoinForDate[0],
    missingRecords: missingForDate.rows,
    missingCount: missingForDate.rows.length
  };
}

/**
 * Main function to run the reconciliation
 */
async function runReconciliation() {
  console.log("=== Bitcoin Calculation Reconciliation Analysis ===");
  console.log(`Analyzing data from ${START_DATE} to ${END_DATE}`);
  
  // Get overall summary
  console.log("\nGenerating overall reconciliation summary...");
  const summary = await getReconciliationSummary();
  
  console.log("\n=== Summary Statistics ===");
  console.log(`Total Curtailment Records: ${summary.totalCurtailmentRecords}`);
  console.log(`Total Bitcoin Calculations: ${summary.totalBitcoinCalculations}`);
  console.log(`Missing Calculations (${DEFAULT_MINER_MODEL}): ${summary.missingCalculations}`);
  
  console.log("\n=== Bitcoin Calculations by Miner Model ===");
  for (const [model, count] of Object.entries(summary.recordsByMinerModel)) {
    console.log(`${model}: ${count}`);
  }
  
  console.log("\n=== Missing Calculations by Miner Model ===");
  for (const [model, count] of Object.entries(summary.missingByMinerModel)) {
    console.log(`${model}: ${count}`);
  }
  
  console.log("\n=== Months with Missing Calculations ===");
  const monthEntries = Object.entries(summary.missingByMonth);
  if (monthEntries.length === 0) {
    console.log("No months with missing calculations found.");
  } else {
    for (const [month, count] of monthEntries) {
      console.log(`${month}: ${count} missing calculations`);
    }
  }
  
  console.log("\n=== Sample of Missing Records ===");
  if (summary.sampleMissingRecords.length === 0) {
    console.log("No missing records found.");
  } else {
    summary.sampleMissingRecords.forEach(record => {
      console.log(`Date: ${record.settlement_date}, Period: ${record.settlement_period}, Farm ID: ${record.farm_id}`);
    });
  }
  
  // Find days with the most missing calculations
  console.log("\n=== Days with Most Missing Calculations ===");
  const problemDays = await findProblemDays();
  
  if (problemDays.rows.length === 0) {
    console.log("No days with missing calculations found.");
  } else {
    for (const day of problemDays.rows) {
      console.log(`${day.settlement_date}: ${day.missing_count} missing calculations`);
    }
    
    // Analyze the top problem day in detail
    if (problemDays.rows.length > 0) {
      const topProblemDate = problemDays.rows[0].settlement_date;
      console.log(`\n=== Detailed Analysis for ${topProblemDate} ===`);
      const dateAnalysis = await analyzeDateMissingCalculations(topProblemDate);
      
      console.log(`Curtailment Records: ${dateAnalysis.curtailmentRecords.count}`);
      console.log(`Distinct Periods: ${dateAnalysis.curtailmentRecords.distinctPeriods}`);
      console.log(`Distinct Farms: ${dateAnalysis.curtailmentRecords.distinctFarms}`);
      
      console.log(`\nBitcoin Calculations: ${dateAnalysis.bitcoinCalculations.count}`);
      console.log(`Distinct Periods: ${dateAnalysis.bitcoinCalculations.distinctPeriods}`);
      console.log(`Distinct Farms: ${dateAnalysis.bitcoinCalculations.distinctFarms}`);
      
      console.log(`\nMissing Records: ${dateAnalysis.missingCount}`);
      if (dateAnalysis.missingCount > 0) {
        console.log("Sample of missing records (period, farm_id):");
        for (let i = 0; i < Math.min(5, dateAnalysis.missingRecords.length); i++) {
          const record = dateAnalysis.missingRecords[i];
          console.log(`Period: ${record.settlement_period}, Farm ID: ${record.farm_id}`);
        }
      }
    }
  }
  
  console.log("\n=== Reconciliation Analysis Complete ===");
}

// Run the reconciliation if this script is executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
  runReconciliation()
    .then(() => {
      console.log("Analysis complete. Exiting...");
      process.exit(0);
    })
    .catch(error => {
      console.error("Error during reconciliation analysis:", error);
      process.exit(1);
    });
}

export { runReconciliation };