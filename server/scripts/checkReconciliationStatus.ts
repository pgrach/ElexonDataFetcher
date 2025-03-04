/**
 * Comprehensive Reconciliation Status Check
 * 
 * This script provides a unified view of data reconciliation status across all years.
 * It checks for missing Bitcoin calculations in 2023, 2024, and 2025 datasets.
 */

import { db } from "@db";
import { curtailmentRecords, historicalBitcoinCalculations } from "@db/schema";
import { eq, and, sql, like, between, count } from "drizzle-orm";
import { format } from "date-fns";

const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

interface YearSummary {
  year: string;
  totalDates: number;
  totalCurtailmentRecords: number;
  totalBitcoinCalculations: number;
  expectedCalculations: number;
  missingCalculations: number;
  completionPercentage: number;
  datesWithIssues: number;
}

interface DateSummary {
  date: string;
  curtailmentRecords: number;
  bitcoinCalculations: number;
  expectedCalculations: number;
  missingCalculations: number;
  completionPercentage: number;
}

/**
 * Format a number with commas
 */
function formatNumber(num: number): string {
  return num.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

/**
 * Format a percentage
 */
function formatPercentage(value: number): string {
  return value.toFixed(2) + "%";
}

/**
 * Check reconciliation status for a specific year
 */
async function checkYearStatus(year: string): Promise<YearSummary> {
  console.log(`\nAnalyzing ${year} data...`);
  
  // Get count of dates in the year with curtailment records
  const datesQuery = await db
    .select({
      count: sql<number>`COUNT(DISTINCT settlement_date)`
    })
    .from(curtailmentRecords)
    .where(like(sql`settlement_date::text`, `${year}-%`));
  
  const totalDates = datesQuery[0]?.count || 0;
  
  // Get total curtailment records for the year
  const curtailmentQuery = await db
    .select({
      count: sql<number>`COUNT(*)`
    })
    .from(curtailmentRecords)
    .where(like(sql`settlement_date::text`, `${year}-%`));
  
  const totalCurtailmentRecords = curtailmentQuery[0]?.count || 0;
  
  // Expected calculations (3 per curtailment record - one for each miner model)
  const expectedCalculations = totalCurtailmentRecords * MINER_MODELS.length;
  
  // Get total Bitcoin calculations for the year
  const calculationsQuery = await db
    .select({
      count: sql<number>`COUNT(*)`
    })
    .from(historicalBitcoinCalculations)
    .where(like(sql`settlement_date::text`, `${year}-%`));
  
  const totalBitcoinCalculations = calculationsQuery[0]?.count || 0;
  
  // Calculate missing
  const missingCalculations = expectedCalculations - totalBitcoinCalculations;
  
  // Calculate completion percentage
  const completionPercentage = expectedCalculations > 0 
    ? (totalBitcoinCalculations / expectedCalculations) * 100
    : 100;
  
  // Find dates with issues (missing calculations)
  const datesWithIssuesQuery = await db
    .execute(sql`
      WITH curtailment_counts AS (
        SELECT
          settlement_date,
          COUNT(*) AS curtailment_count
        FROM curtailment_records
        WHERE settlement_date::text LIKE ${year + '-%'}
        GROUP BY settlement_date
      ),
      
      calculation_counts AS (
        SELECT
          settlement_date,
          COUNT(*) AS calculation_count
        FROM historical_bitcoin_calculations
        WHERE settlement_date::text LIKE ${year + '-%'}
        GROUP BY settlement_date
      )
      
      SELECT COUNT(*) AS dates_with_issues
      FROM curtailment_counts c
      LEFT JOIN calculation_counts b
      ON c.settlement_date = b.settlement_date
      WHERE c.curtailment_count * ${MINER_MODELS.length} > COALESCE(b.calculation_count, 0)
    `);
  
  const datesWithIssues = Number(datesWithIssuesQuery[0]?.dates_with_issues || 0);
  
  return {
    year,
    totalDates,
    totalCurtailmentRecords,
    totalBitcoinCalculations,
    expectedCalculations,
    missingCalculations,
    completionPercentage,
    datesWithIssues
  };
}

/**
 * Get dates with reconciliation issues for a specific year
 */
async function getDatesWithIssues(year: string, limit: number = 10): Promise<DateSummary[]> {
  const result = await db
    .execute(sql`
      WITH curtailment_counts AS (
        SELECT
          settlement_date,
          COUNT(*) AS curtailment_count
        FROM curtailment_records
        WHERE settlement_date::text LIKE ${year + '-%'}
        GROUP BY settlement_date
      ),
      
      calculation_counts AS (
        SELECT
          settlement_date,
          COUNT(*) AS calculation_count
        FROM historical_bitcoin_calculations
        WHERE settlement_date::text LIKE ${year + '-%'}
        GROUP BY settlement_date
      )
      
      SELECT 
        c.settlement_date::text as date,
        c.curtailment_count,
        COALESCE(b.calculation_count, 0) as calculation_count,
        c.curtailment_count * ${MINER_MODELS.length} as expected_calculations,
        c.curtailment_count * ${MINER_MODELS.length} - COALESCE(b.calculation_count, 0) as missing_calculations,
        CASE 
          WHEN c.curtailment_count = 0 THEN 100
          ELSE (COALESCE(b.calculation_count, 0)::float / (c.curtailment_count * ${MINER_MODELS.length})) * 100
        END as completion_percentage
      FROM curtailment_counts c
      LEFT JOIN calculation_counts b
      ON c.settlement_date = b.settlement_date
      WHERE c.curtailment_count * ${MINER_MODELS.length} > COALESCE(b.calculation_count, 0)
      ORDER BY missing_calculations DESC
      LIMIT ${limit}
    `);
  
  return result.map(row => ({
    date: row.date,
    curtailmentRecords: Number(row.curtailment_count),
    bitcoinCalculations: Number(row.calculation_count),
    expectedCalculations: Number(row.expected_calculations),
    missingCalculations: Number(row.missing_calculations),
    completionPercentage: Number(row.completion_percentage)
  }));
}

/**
 * Main function to check reconciliation status across all years
 */
async function checkReconciliationStatus() {
  console.log("=== Comprehensive Reconciliation Status Check ===");
  
  // Check status for each year
  const years = ['2023', '2024', '2025'];
  const yearSummaries: YearSummary[] = [];
  
  for (const year of years) {
    const yearSummary = await checkYearStatus(year);
    yearSummaries.push(yearSummary);
  }
  
  // Print overall summary table
  console.log("\n=== Overall Reconciliation Status ===");
  console.log("Year | Dates | Curtailment Records | Bitcoin Calculations | Expected | Missing | Completion | Issues");
  console.log("-----|-------|---------------------|----------------------|----------|---------|------------|-------");
  
  let totalCurtailmentRecords = 0;
  let totalBitcoinCalculations = 0;
  let totalExpectedCalculations = 0;
  
  for (const summary of yearSummaries) {
    totalCurtailmentRecords += summary.totalCurtailmentRecords;
    totalBitcoinCalculations += summary.totalBitcoinCalculations;
    totalExpectedCalculations += summary.expectedCalculations;
    
    console.log(
      `${summary.year} | ${formatNumber(summary.totalDates)} | ${formatNumber(summary.totalCurtailmentRecords)} | ` +
      `${formatNumber(summary.totalBitcoinCalculations)} | ${formatNumber(summary.expectedCalculations)} | ` +
      `${formatNumber(summary.missingCalculations)} | ${formatPercentage(summary.completionPercentage)} | ` +
      `${summary.datesWithIssues}`
    );
  }
  
  // Print total
  const overallCompletion = totalExpectedCalculations > 0 
    ? (totalBitcoinCalculations / totalExpectedCalculations) * 100
    : 100;
  
  console.log("-----|-------|---------------------|----------------------|----------|---------|------------|-------");
  console.log(
    `Total | - | ${formatNumber(totalCurtailmentRecords)} | ` +
    `${formatNumber(totalBitcoinCalculations)} | ${formatNumber(totalExpectedCalculations)} | ` +
    `${formatNumber(totalExpectedCalculations - totalBitcoinCalculations)} | ${formatPercentage(overallCompletion)} | -`
  );
  
  // Show sample of dates with issues for each year
  for (const year of years) {
    const yearSummary = yearSummaries.find(s => s.year === year);
    
    if (yearSummary && yearSummary.datesWithIssues > 0) {
      console.log(`\n=== Top Issues in ${year} (${yearSummary.datesWithIssues} dates with issues) ===`);
      
      const datesWithIssues = await getDatesWithIssues(year, 5);
      
      if (datesWithIssues.length > 0) {
        console.log("Date | Curtailment Records | Bitcoin Calculations | Expected | Missing | Completion");
        console.log("-----|---------------------|----------------------|----------|---------|------------");
        
        for (const dateSummary of datesWithIssues) {
          console.log(
            `${dateSummary.date} | ${formatNumber(dateSummary.curtailmentRecords)} | ` +
            `${formatNumber(dateSummary.bitcoinCalculations)} | ${formatNumber(dateSummary.expectedCalculations)} | ` +
            `${formatNumber(dateSummary.missingCalculations)} | ${formatPercentage(dateSummary.completionPercentage)}`
          );
        }
      }
    }
  }
  
  // Print recommendations
  console.log("\n=== Recommendations ===");
  
  for (const year of years) {
    const yearSummary = yearSummaries.find(s => s.year === year);
    
    if (yearSummary && yearSummary.datesWithIssues > 0) {
      const percent = (yearSummary.datesWithIssues / yearSummary.totalDates) * 100;
      
      if (percent > 50) {
        console.log(`- ${year}: CRITICAL - ${formatPercentage(percent)} of dates have missing calculations. Run reconcile${year}Data.ts.`);
      } else if (percent > 10) {
        console.log(`- ${year}: WARNING - ${formatPercentage(percent)} of dates have missing calculations. Run reconcile${year}Data.ts.`);
      } else {
        console.log(`- ${year}: MINOR - ${formatPercentage(percent)} of dates have missing calculations. Run reconcile${year}Data.ts.`);
      }
    } else {
      console.log(`- ${year}: GOOD - No reconciliation issues detected.`);
    }
  }
  
  console.log("\n=== Status Check Complete ===");
}

// Run the status check if this script is executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
  checkReconciliationStatus()
    .then(() => {
      console.log('Status check completed successfully');
      process.exit(0);
    })
    .catch(error => {
      console.error('Error during status check:', error);
      process.exit(1);
    });
}

export { checkReconciliationStatus };