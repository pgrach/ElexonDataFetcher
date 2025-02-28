/**
 * Specialized tool to fix December 2023 reconciliation issues.
 * This script targets the specific dates in December 2023 that have missing Bitcoin calculations.
 */

import { db } from "./db";
import { sql } from "drizzle-orm";
import { auditAndFixBitcoinCalculations } from "./server/services/historicalReconciliation";

const DECEMBER_2023_DATES = [
  "2023-12-01", "2023-12-02", "2023-12-03", "2023-12-04", "2023-12-05",
  "2023-12-06", "2023-12-07", "2023-12-08", "2023-12-09", "2023-12-10",
  "2023-12-11", "2023-12-12", "2023-12-13", "2023-12-14", "2023-12-15",
  "2023-12-16", "2023-12-17", "2023-12-18", "2023-12-19", "2023-12-20",
  "2023-12-21", "2023-12-22", "2023-12-23", "2023-12-24", "2023-12-25",
  "2023-12-26", "2023-12-27", "2023-12-28", "2023-12-29", "2023-12-30",
  "2023-12-31"
];

interface DateReconciliationStatus {
  date: string;
  calculationCount: number;
  expectedCount: number;
  completionPercentage: number;
}

async function getDecemberReconciliationStatus(): Promise<{
  overall: {
    curtailmentRecords: number;
    uniqueCombinations: number;
    totalCalculations: number;
    expectedCalculations: number;
    reconciliationPercentage: number;
  },
  byMinerModel: Record<string, number>;
  byDate: DateReconciliationStatus[];
}> {
  // Get overall December 2023 statistics
  const overallResult = await db.execute(sql`
    WITH december_curtailment AS (
      SELECT 
        COUNT(*) as total_records,
        COUNT(DISTINCT (settlement_date || '-' || settlement_period || '-' || farm_id)) as unique_combinations
      FROM curtailment_records
      WHERE settlement_date >= '2023-12-01' AND settlement_date <= '2023-12-31'
    ),
    december_bitcoin AS (
      SELECT
        miner_model,
        COUNT(*) as calculation_count
      FROM historical_bitcoin_calculations
      WHERE settlement_date >= '2023-12-01' AND settlement_date <= '2023-12-31'
      GROUP BY miner_model
    )
    SELECT 
      c.total_records as curtailment_records,
      c.unique_combinations as unique_combinations,
      COALESCE(b.miner_model, 'TOTAL') as miner_model,
      COALESCE(b.calculation_count, SUM(b.calculation_count) OVER ()) as calculation_count,
      c.unique_combinations * 3 as expected_calculations,
      ROUND((COALESCE(b.calculation_count, SUM(b.calculation_count) OVER ()) * 100.0) / 
        NULLIF(c.unique_combinations * 3, 0), 2) as reconciliation_percentage
    FROM december_curtailment c
    LEFT JOIN december_bitcoin b ON 1=1
    GROUP BY c.total_records, c.unique_combinations, b.miner_model, b.calculation_count
  `);
  
  // Get statistics by date
  const byDateResult = await db.execute(sql`
    WITH date_combinations AS (
      SELECT 
        settlement_date,
        COUNT(DISTINCT (settlement_period || '-' || farm_id)) as unique_combinations
      FROM curtailment_records
      WHERE settlement_date >= '2023-12-01' AND settlement_date <= '2023-12-31'
      GROUP BY settlement_date
    ),
    date_calculations AS (
      SELECT 
        d.settlement_date,
        COUNT(DISTINCT b.id) as calculation_count,
        d.unique_combinations * 3 as expected_count
      FROM date_combinations d
      LEFT JOIN historical_bitcoin_calculations b 
        ON d.settlement_date = b.settlement_date
      GROUP BY d.settlement_date, d.unique_combinations
    )
    SELECT 
      settlement_date::text as date,
      calculation_count,
      expected_count,
      ROUND((calculation_count * 100.0) / NULLIF(expected_count, 0), 2) as completion_percentage
    FROM date_calculations
    ORDER BY settlement_date
  `);
  
  // Extract overall statistics
  const totalRow = overallResult.rows.find(row => row.miner_model === 'TOTAL') || overallResult.rows[0];
  
  const overall = {
    curtailmentRecords: Number(totalRow.curtailment_records || 0),
    uniqueCombinations: Number(totalRow.unique_combinations || 0),
    totalCalculations: Number(totalRow.calculation_count || 0),
    expectedCalculations: Number(totalRow.expected_calculations || 0),
    reconciliationPercentage: Number(totalRow.reconciliation_percentage || 0)
  };
  
  // Extract statistics by miner model
  const byMinerModel: Record<string, number> = {};
  overallResult.rows.forEach(row => {
    if (row.miner_model !== 'TOTAL') {
      byMinerModel[String(row.miner_model)] = Number(row.calculation_count || 0);
    }
  });
  
  // Extract statistics by date
  const byDate: DateReconciliationStatus[] = byDateResult.rows.map(row => ({
    date: String(row.date),
    calculationCount: Number(row.calculation_count || 0),
    expectedCount: Number(row.expected_count || 0),
    completionPercentage: Number(row.completion_percentage || 0)
  }));
  
  return { overall, byMinerModel, byDate };
}

async function printDecemberReconciliationReport() {
  console.log("=== December 2023 Reconciliation Report ===\n");
  
  const status = await getDecemberReconciliationStatus();
  
  console.log("Overall Status:");
  console.log(`Curtailment Records: ${status.overall.curtailmentRecords}`);
  console.log(`Unique Period-Farm Combinations: ${status.overall.uniqueCombinations}`);
  console.log(`Bitcoin Calculations: ${status.overall.totalCalculations}`);
  console.log(`Expected Calculations: ${status.overall.expectedCalculations}`);
  console.log(`Reconciliation: ${status.overall.reconciliationPercentage}%\n`);
  
  console.log("By Miner Model:");
  Object.entries(status.byMinerModel).forEach(([model, count]) => {
    console.log(`- ${model}: ${count} calculations`);
  });
  
  console.log("\nDates with Missing Calculations:");
  
  const incompleteDates = status.byDate.filter(date => date.completionPercentage < 100);
  
  if (incompleteDates.length === 0) {
    console.log("✅ No dates with missing calculations found!");
  } else {
    const totalMissing = incompleteDates.reduce((sum, date) => sum + (date.expectedCount - date.calculationCount), 0);
    console.log(`Found ${incompleteDates.length} dates with missing calculations (${totalMissing} calculations missing total)`);
    console.log("\nTop 10 dates with most missing calculations:");
    
    // Sort by completion percentage ascending
    incompleteDates
      .sort((a, b) => a.completionPercentage - b.completionPercentage)
      .slice(0, 10)
      .forEach(date => {
        const missing = date.expectedCount - date.calculationCount;
        console.log(`- ${date.date}: ${date.calculationCount}/${date.expectedCount} (${date.completionPercentage}%) - Missing ${missing} calculations`);
      });
  }
  
  return { status, incompleteDates: incompleteDates };
}

async function fixMissingCalculationsInBatches(limit: number = 5) {
  try {
    console.log("\n=== Starting Batch Processing for December 2023 ===\n");
    
    // Get initial status
    const { status, incompleteDates } = await printDecemberReconciliationReport();
    
    if (incompleteDates.length === 0) {
      console.log("\n✅ All December 2023 dates are fully reconciled! No action needed.");
      return;
    }
    
    // Sort dates by completion percentage to prioritize the most incomplete dates
    const datesToProcess = incompleteDates
      .sort((a, b) => a.completionPercentage - b.completionPercentage)
      .slice(0, limit)
      .map(d => d.date);
    
    console.log(`\nProcessing ${datesToProcess.length} dates (batch size: ${limit})...\n`);
    
    let successful = 0;
    let failed = 0;
    
    for (const date of datesToProcess) {
      try {
        console.log(`Processing ${date}...`);
        const result = await auditAndFixBitcoinCalculations(date);
        
        if (result.success) {
          console.log(`✅ ${date}: Fixed - ${result.message}\n`);
          successful++;
        } else {
          console.log(`❌ ${date}: Failed - ${result.message}\n`);
          failed++;
        }
      } catch (error) {
        console.error(`Error processing ${date}:`, error);
        failed++;
      }
    }
    
    // Get final reconciliation status
    console.log("\nChecking final reconciliation status...\n");
    const final = await getDecemberReconciliationStatus();
    
    console.log("=== Batch Processing Summary ===");
    console.log(`Dates Processed: ${datesToProcess.length}`);
    console.log(`Successful: ${successful}`);
    console.log(`Failed: ${failed}`);
    console.log(`Initial December 2023 Reconciliation: ${status.overall.reconciliationPercentage}%`);
    console.log(`Final December 2023 Reconciliation: ${final.overall.reconciliationPercentage}%`);
    console.log(`Improvement: ${(final.overall.reconciliationPercentage - status.overall.reconciliationPercentage).toFixed(2)}%`);
    
    if (incompleteDates.length > limit) {
      console.log(`\nThere are still ${incompleteDates.length - limit} dates that need reconciliation.`);
      console.log(`Run this script again to process the next batch of dates.`);
    } else {
      console.log(`\nAll identified dates with missing calculations have been processed.`);
    }
    
  } catch (error) {
    console.error("Error during batch processing:", error);
    throw error;
  }
}

async function main() {
  const command = process.argv[2]?.toLowerCase();
  const batchSizeArg = process.argv[3];
  const batchSize = batchSizeArg ? parseInt(batchSizeArg, 10) : 5;
  
  switch (command) {
    case "status":
      await printDecemberReconciliationReport();
      break;
      
    case "fix":
      await fixMissingCalculationsInBatches(batchSize);
      break;
      
    default:
      console.log("December 2023 Reconciliation Tool\n");
      console.log("Commands:");
      console.log("  status         - Show December 2023 reconciliation status");
      console.log("  fix [batch_size] - Fix missing calculations (default batch size: 5)");
      console.log("\nExamples:");
      console.log("  npx tsx fix_december_2023.ts status");
      console.log("  npx tsx fix_december_2023.ts fix");
      console.log("  npx tsx fix_december_2023.ts fix 10");
      
      await printDecemberReconciliationReport();
  }
}

// Run the main function if script is executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
  main()
    .then(() => {
      console.log("\n=== December 2023 Reconciliation Tool Complete ===");
      process.exit(0);
    })
    .catch(error => {
      console.error("Fatal error:", error);
      process.exit(1);
    });
}