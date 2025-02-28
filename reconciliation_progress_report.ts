/**
 * Reconciliation Progress Report Generator
 * 
 * This script generates a detailed report on the current status of reconciliation
 * between curtailment records and Bitcoin calculations.
 */

import pg from 'pg';
const { Pool } = pg;

// Get the database connection string from environment
const dbUrl = process.env.DATABASE_URL;
if (!dbUrl) {
  console.error('DATABASE_URL environment variable is not set');
  process.exit(1);
}

// Create a connection pool
const pool = new Pool({
  connectionString: dbUrl,
});

interface DateReconciliationStatus {
  date: string;
  expectedCalculations: number;
  actualCalculations: number;
  completionPercentage: number;
}

interface ReconciliationSummary {
  totalExpectedCalculations: number;
  totalActualCalculations: number;
  overallCompletionPercentage: number;
  datesSummary: {
    totalDates: number;
    completelyReconciledDates: number;
    partiallyReconciledDates: number;
    unreconciledDates: number;
  };
  monthsSummary: {
    [key: string]: {
      expectedCalculations: number;
      actualCalculations: number;
      completionPercentage: number;
    };
  };
  topMissingDates: DateReconciliationStatus[];
}

async function generateReport(): Promise<ReconciliationSummary> {
  const client = await pool.connect();
  
  try {
    // Get overall totals
    const overallQuery = `
      WITH curtailment_stats AS (
        SELECT 
          COUNT(*) AS total_records,
          COUNT(DISTINCT (settlement_date, settlement_period, farm_id)) AS unique_combinations
        FROM curtailment_records
      ),
      calculation_stats AS (
        SELECT 
          COUNT(*) AS total_calculations
        FROM historical_bitcoin_calculations
      ),
      expected_stats AS (
        SELECT 
          COUNT(DISTINCT miner_model) AS miner_model_count
        FROM historical_bitcoin_calculations
      )
      SELECT 
        cs.unique_combinations * es.miner_model_count AS expected_calculations,
        bcs.total_calculations AS actual_calculations,
        CASE 
          WHEN cs.unique_combinations * es.miner_model_count = 0 THEN 0
          ELSE ROUND((bcs.total_calculations::numeric / (cs.unique_combinations * es.miner_model_count)) * 100, 2)
        END AS completion_percentage
      FROM 
        curtailment_stats cs,
        calculation_stats bcs,
        expected_stats es;
    `;
    
    const overallResult = await client.query(overallQuery);
    const { expected_calculations, actual_calculations, completion_percentage } = overallResult.rows[0];
    
    // Get dates summary
    const datesSummaryQuery = `
      WITH date_reconciliation AS (
        SELECT 
          cr.settlement_date, 
          COUNT(DISTINCT (cr.settlement_period, cr.farm_id)) * 3 AS expected_count,
          COUNT(DISTINCT (hbc.settlement_period, hbc.farm_id, hbc.miner_model)) AS actual_count
        FROM 
          curtailment_records cr
        LEFT JOIN 
          historical_bitcoin_calculations hbc ON cr.settlement_date = hbc.settlement_date
          AND cr.settlement_period = hbc.settlement_period
          AND cr.farm_id = hbc.farm_id
        GROUP BY 
          cr.settlement_date
      )
      SELECT 
        COUNT(*) AS total_dates,
        COUNT(*) FILTER (WHERE expected_count = actual_count AND expected_count > 0) AS completely_reconciled,
        COUNT(*) FILTER (WHERE actual_count > 0 AND actual_count < expected_count) AS partially_reconciled,
        COUNT(*) FILTER (WHERE actual_count = 0) AS unreconciled
      FROM 
        date_reconciliation;
    `;
    
    const datesSummaryResult = await client.query(datesSummaryQuery);
    const { total_dates, completely_reconciled, partially_reconciled, unreconciled } = datesSummaryResult.rows[0];
    
    // Get top missing dates
    const topMissingDatesQuery = `
      WITH required_combinations AS (
        SELECT 
          cr.settlement_date, 
          COUNT(DISTINCT (cr.settlement_period || '-' || cr.farm_id)) * 3 AS expected_count
        FROM 
          curtailment_records cr
        GROUP BY 
          cr.settlement_date
      ),
      actual_calculations AS (
        SELECT 
          settlement_date, 
          COUNT(*) AS actual_count
        FROM 
          historical_bitcoin_calculations
        GROUP BY 
          settlement_date
      )
      SELECT 
        r.settlement_date,
        r.expected_count AS expected_calculations,
        COALESCE(a.actual_count, 0) AS actual_calculations,
        CASE 
          WHEN r.expected_count = 0 THEN 0
          ELSE ROUND((COALESCE(a.actual_count, 0)::numeric / r.expected_count) * 100, 2)
        END AS completion_percentage
      FROM 
        required_combinations r
      LEFT JOIN 
        actual_calculations a ON r.settlement_date = a.settlement_date
      WHERE 
        COALESCE(a.actual_count, 0) < r.expected_count
      ORDER BY 
        (r.expected_count - COALESCE(a.actual_count, 0)) DESC
      LIMIT 10;
    `;
    
    const topMissingDatesResult = await client.query(topMissingDatesQuery);
    
    // Get monthly summary
    const monthlySummaryQuery = `
      WITH monthly_stats AS (
        SELECT 
          TO_CHAR(cr.settlement_date, 'YYYY-MM') AS year_month,
          COUNT(DISTINCT (cr.settlement_date, cr.settlement_period, cr.farm_id)) * 3 AS expected_count,
          COUNT(DISTINCT (hbc.settlement_date, hbc.settlement_period, hbc.farm_id, hbc.miner_model)) AS actual_count
        FROM 
          curtailment_records cr
        LEFT JOIN 
          historical_bitcoin_calculations hbc 
          ON cr.settlement_date = hbc.settlement_date
          AND cr.settlement_period = hbc.settlement_period
          AND cr.farm_id = hbc.farm_id
        GROUP BY 
          TO_CHAR(cr.settlement_date, 'YYYY-MM')
      )
      SELECT 
        year_month,
        expected_count AS expected_calculations,
        actual_count AS actual_calculations,
        CASE 
          WHEN expected_count = 0 THEN 0
          ELSE ROUND((actual_count::numeric / expected_count) * 100, 2)
        END AS completion_percentage
      FROM 
        monthly_stats
      ORDER BY 
        year_month;
    `;
    
    const monthlySummaryResult = await client.query(monthlySummaryQuery);
    
    // Format months summary
    const monthsSummary: Record<string, any> = {};
    monthlySummaryResult.rows.forEach((row) => {
      monthsSummary[row.year_month] = {
        expectedCalculations: parseInt(row.expected_calculations),
        actualCalculations: parseInt(row.actual_calculations),
        completionPercentage: parseFloat(row.completion_percentage)
      };
    });
    
    return {
      totalExpectedCalculations: parseInt(expected_calculations),
      totalActualCalculations: parseInt(actual_calculations),
      overallCompletionPercentage: parseFloat(completion_percentage),
      datesSummary: {
        totalDates: parseInt(total_dates),
        completelyReconciledDates: parseInt(completely_reconciled),
        partiallyReconciledDates: parseInt(partially_reconciled),
        unreconciledDates: parseInt(unreconciled)
      },
      monthsSummary,
      topMissingDates: topMissingDatesResult.rows.map(row => ({
        date: row.settlement_date,
        expectedCalculations: parseInt(row.expected_calculations),
        actualCalculations: parseInt(row.actual_calculations),
        completionPercentage: parseFloat(row.completion_percentage)
      }))
    };
  } finally {
    client.release();
  }
}

function formatPercentage(value: number): string {
  return `${value.toFixed(2)}%`;
}

function formatNumber(value: number): string {
  return value.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ',');
}

async function displayReport() {
  console.log("=== Reconciliation Progress Report ===\n");
  
  try {
    const report = await generateReport();
    
    // Display overall summary
    console.log(`Overall Progress: ${formatPercentage(report.overallCompletionPercentage)}`);
    console.log(`Calculations: ${formatNumber(report.totalActualCalculations)} / ${formatNumber(report.totalExpectedCalculations)}`);
    
    // Display dates summary
    console.log("\nDates Summary:");
    console.log(`Total Dates: ${report.datesSummary.totalDates}`);
    console.log(`100% Reconciled: ${report.datesSummary.completelyReconciledDates} (${formatPercentage(report.datesSummary.completelyReconciledDates / report.datesSummary.totalDates * 100)})`);
    console.log(`Partially Reconciled: ${report.datesSummary.partiallyReconciledDates} (${formatPercentage(report.datesSummary.partiallyReconciledDates / report.datesSummary.totalDates * 100)})`);
    console.log(`Not Reconciled: ${report.datesSummary.unreconciledDates} (${formatPercentage(report.datesSummary.unreconciledDates / report.datesSummary.totalDates * 100)})`);
    
    // Display top missing dates
    console.log("\nTop 10 Dates with Missing Calculations:");
    report.topMissingDates.forEach((date, index) => {
      console.log(`${index + 1}. ${date.date}: ${formatNumber(date.actualCalculations)} / ${formatNumber(date.expectedCalculations)} (${formatPercentage(date.completionPercentage)})`);
    });
    
    // Display monthly summary
    console.log("\nMonthly Summary:");
    Object.entries(report.monthsSummary).forEach(([month, stats]) => {
      const { expectedCalculations, actualCalculations, completionPercentage } = stats as any;
      console.log(`${month}: ${formatNumber(actualCalculations)} / ${formatNumber(expectedCalculations)} (${formatPercentage(completionPercentage)})`);
    });
    
  } catch (error) {
    console.error("Error generating report:", error);
  } finally {
    // Close the pool
    await pool.end();
  }
}

// Run the main function if script is executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
  displayReport()
    .then(() => {
      console.log("\n=== Report Generation Complete ===");
      process.exit(0);
    })
    .catch(error => {
      console.error("Fatal error:", error);
      process.exit(1);
    });
}