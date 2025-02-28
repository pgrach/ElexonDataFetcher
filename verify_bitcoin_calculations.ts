/**
 * This script verifies the completeness of Bitcoin calculations across all time periods.
 * It generates detailed reports and identifies any remaining issues after fixes are applied.
 */

import { db } from "@db";
import { curtailmentRecords, historicalBitcoinCalculations } from "@db/schema";
import { sql, and, eq, between, desc } from "drizzle-orm";
import { format, parseISO, eachMonthOfInterval, addDays, subDays } from "date-fns";
import fs from "fs/promises";
import path from "path";

// Configuration
const REPORT_FILE = "bitcoin_calculation_verification_report.json";
const MINER_MODELS = ["S19J_PRO", "M20S", "S9"];
const DEFAULT_MODEL = "S19J_PRO";

interface MonthVerification {
  yearMonth: string;
  curtailmentCount: number;
  bitcoinCounts: Record<string, number>;
  status: 'Complete' | 'Incomplete' | 'Missing';
  percentComplete: Record<string, number>;
}

interface DayVerification {
  date: string;
  curtailmentCount: number;
  bitcoinCounts: Record<string, number>;
  status: 'Complete' | 'Incomplete' | 'Missing';
  percentComplete: Record<string, number>;
}

interface VerificationReport {
  generatedAt: string;
  summary: {
    totalCurtailmentRecords: number;
    totalMonths: number;
    completeMonths: number;
    incompleteMonths: number;
    missingMonths: number;
    completionPercentage: number;
  };
  monthDetails: MonthVerification[];
  incompleteOrMissingDays: DayVerification[];
}

/**
 * Generate a comprehensive verification report for all time periods
 */
async function generateVerificationReport(): Promise<VerificationReport> {
  console.log("Generating verification report...");
  const startTime = Date.now();
  
  // Get total curtailment records
  const totalCurtailmentResult = await db
    .select({ count: sql<number>`COUNT(*)` })
    .from(curtailmentRecords);
  
  const totalCurtailmentRecords = totalCurtailmentResult[0]?.count || 0;
  
  // Get month-by-month data
  const monthResult = await db.execute(sql`
    WITH curtailment_months AS (
      SELECT 
        TO_CHAR(settlement_date, 'YYYY-MM') as year_month,
        COUNT(*) as curtailment_count
      FROM curtailment_records
      GROUP BY TO_CHAR(settlement_date, 'YYYY-MM')
    )
    SELECT 
      year_month,
      curtailment_count
    FROM curtailment_months
    ORDER BY year_month
  `);
  
  // Process each month
  const monthDetails: MonthVerification[] = [];
  let completeMonths = 0;
  let incompleteMonths = 0;
  let missingMonths = 0;
  
  for (const row of monthResult.rows) {
    const yearMonth = row.year_month;
    const curtailmentCount = parseInt(row.curtailment_count);
    
    // Get Bitcoin counts for each model
    const bitcoinCounts: Record<string, number> = {};
    const percentComplete: Record<string, number> = {};
    
    for (const model of MINER_MODELS) {
      const bitcoinResult = await db.execute(sql`
        SELECT COUNT(*) as count
        FROM historical_bitcoin_calculations
        WHERE TO_CHAR(settlement_date, 'YYYY-MM') = ${yearMonth}
          AND miner_model = ${model}
      `);
      
      const count = parseInt(bitcoinResult.rows[0].count) || 0;
      bitcoinCounts[model] = count;
      percentComplete[model] = curtailmentCount > 0 
        ? Math.round((count / curtailmentCount) * 100) 
        : 0;
    }
    
    // Determine status based on DEFAULT_MODEL
    let status: 'Complete' | 'Incomplete' | 'Missing';
    if (bitcoinCounts[DEFAULT_MODEL] === 0) {
      status = 'Missing';
      missingMonths++;
    } else if (bitcoinCounts[DEFAULT_MODEL] < curtailmentCount) {
      status = 'Incomplete';
      incompleteMonths++;
    } else {
      status = 'Complete';
      completeMonths++;
    }
    
    monthDetails.push({
      yearMonth,
      curtailmentCount,
      bitcoinCounts,
      status,
      percentComplete
    });
  }
  
  // Get details for incomplete or missing days
  const incompleteOrMissingDays: DayVerification[] = [];
  
  // Find days with incomplete or missing calculations
  const dayResult = await db.execute(sql`
    WITH curtailment_days AS (
      SELECT 
        settlement_date::text as date,
        COUNT(*) as curtailment_count
      FROM curtailment_records
      GROUP BY settlement_date
    ),
    bitcoin_days AS (
      SELECT 
        settlement_date::text as date,
        miner_model,
        COUNT(*) as bitcoin_count
      FROM historical_bitcoin_calculations
      WHERE miner_model = ${DEFAULT_MODEL}
      GROUP BY settlement_date, miner_model
    )
    SELECT 
      c.date,
      c.curtailment_count,
      b.bitcoin_count
    FROM curtailment_days c
    LEFT JOIN bitcoin_days b ON c.date = b.date
    WHERE b.bitcoin_count IS NULL OR b.bitcoin_count < c.curtailment_count
    ORDER BY c.date
    LIMIT 100
  `);
  
  // Process each problematic day
  for (const row of dayResult.rows) {
    const date = row.date;
    const curtailmentCount = parseInt(row.curtailment_count);
    
    // Get Bitcoin counts for each model
    const bitcoinCounts: Record<string, number> = {};
    const percentComplete: Record<string, number> = {};
    
    for (const model of MINER_MODELS) {
      const bitcoinResult = await db.execute(sql`
        SELECT COUNT(*) as count
        FROM historical_bitcoin_calculations
        WHERE settlement_date::text = ${date}
          AND miner_model = ${model}
      `);
      
      const count = parseInt(bitcoinResult.rows[0].count) || 0;
      bitcoinCounts[model] = count;
      percentComplete[model] = curtailmentCount > 0 
        ? Math.round((count / curtailmentCount) * 100) 
        : 0;
    }
    
    // Determine status
    let status: 'Complete' | 'Incomplete' | 'Missing';
    if (bitcoinCounts[DEFAULT_MODEL] === 0) {
      status = 'Missing';
    } else if (bitcoinCounts[DEFAULT_MODEL] < curtailmentCount) {
      status = 'Incomplete';
    } else {
      status = 'Complete';
    }
    
    incompleteOrMissingDays.push({
      date,
      curtailmentCount,
      bitcoinCounts,
      status,
      percentComplete
    });
  }
  
  // Generate the full report
  const report: VerificationReport = {
    generatedAt: new Date().toISOString(),
    summary: {
      totalCurtailmentRecords,
      totalMonths: monthDetails.length,
      completeMonths,
      incompleteMonths,
      missingMonths,
      completionPercentage: Math.round((completeMonths / monthDetails.length) * 100)
    },
    monthDetails,
    incompleteOrMissingDays
  };
  
  const duration = (Date.now() - startTime) / 1000;
  console.log(`Report generation completed in ${duration.toFixed(1)} seconds.`);
  
  return report;
}

/**
 * Save the verification report to a file
 */
async function saveReport(report: VerificationReport): Promise<void> {
  await fs.writeFile(REPORT_FILE, JSON.stringify(report, null, 2), 'utf-8');
  console.log(`Report saved to ${REPORT_FILE}`);
}

/**
 * Print a summary of the verification report to the console
 */
function printReportSummary(report: VerificationReport): void {
  console.log("\n=== Bitcoin Calculation Verification Summary ===");
  console.log(`Generated at: ${report.generatedAt}`);
  console.log(`Total curtailment records: ${report.summary.totalCurtailmentRecords.toLocaleString()}`);
  console.log(`Total months: ${report.summary.totalMonths}`);
  console.log(`Complete months: ${report.summary.completeMonths} (${report.summary.completionPercentage}%)`);
  console.log(`Incomplete months: ${report.summary.incompleteMonths}`);
  console.log(`Missing months: ${report.summary.missingMonths}`);
  
  if (report.summary.incompleteMonths > 0 || report.summary.missingMonths > 0) {
    console.log("\nIncomplete or missing months:");
    for (const month of report.monthDetails) {
      if (month.status !== 'Complete') {
        console.log(`- ${month.yearMonth}: ${month.status}, ${month.percentComplete[DEFAULT_MODEL]}% complete`);
      }
    }
  }
  
  if (report.incompleteOrMissingDays.length > 0) {
    console.log("\nSample of incomplete or missing days:");
    for (let i = 0; i < Math.min(5, report.incompleteOrMissingDays.length); i++) {
      const day = report.incompleteOrMissingDays[i];
      console.log(`- ${day.date}: ${day.status}, ${day.percentComplete[DEFAULT_MODEL]}% complete`);
    }
    
    if (report.incompleteOrMissingDays.length > 5) {
      console.log(`... and ${report.incompleteOrMissingDays.length - 5} more days`);
    }
  }
  
  if (report.summary.completionPercentage === 100) {
    console.log("\n🎉 All Bitcoin calculations are complete! 🎉");
  } else {
    console.log(`\nOverall completion: ${report.summary.completionPercentage}%`);
    console.log("Recommendation: Run the fix_all_bitcoin_calculations.ts script to address remaining issues.");
  }
}

/**
 * Main function to verify Bitcoin calculations
 */
async function verifyBitcoinCalculations() {
  console.log("=== Starting Bitcoin Calculation Verification ===");
  
  // Generate the verification report
  const report = await generateVerificationReport();
  
  // Save the report to a file
  await saveReport(report);
  
  // Print a summary to the console
  printReportSummary(report);
  
  console.log("\n=== Verification Complete ===");
}

/**
 * Entry point for the script
 */
async function main() {
  try {
    await verifyBitcoinCalculations();
    process.exit(0);
  } catch (error) {
    console.error("Fatal error:", error);
    process.exit(1);
  }
}

// Run the script if executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}