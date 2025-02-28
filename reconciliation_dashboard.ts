/**
 * Reconciliation Dashboard
 * 
 * A user-friendly dashboard to view the status of reconciliation between
 * curtailment_records and historical_bitcoin_calculations tables.
 */

import { db } from './db';
import { sql } from 'drizzle-orm';
import { format } from 'date-fns';
import * as fs from 'fs';

const LOG_FILE = 'reconciliation_dashboard.log';

/**
 * Helper function to safely cast query results to an array of records
 */
function safeResultArray<T = Record<string, any>>(result: any): Array<T> {
  return result as unknown as Array<T>;
}

// Color constants for console output
const COLORS = {
  reset: '\x1b[0m',
  red: '\x1b[31m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  blue: '\x1b[34m',
  magenta: '\x1b[35m',
  cyan: '\x1b[36m',
  white: '\x1b[37m',
  gray: '\x1b[90m',
  brightGreen: '\x1b[92m',
  brightYellow: '\x1b[93m',
  brightBlue: '\x1b[94m',
  brightMagenta: '\x1b[95m',
  brightCyan: '\x1b[96m',
};

/**
 * Logging function with error handling
 */
function log(message: string, level: 'info' | 'warning' | 'error' | 'success' = 'info'): void {
  const timestamp = new Date().toISOString();
  let color = COLORS.reset;
  
  switch(level) {
    case 'error':
      color = COLORS.red;
      break;
    case 'warning':
      color = COLORS.yellow;
      break;
    case 'success':
      color = COLORS.green;
      break;
    default:
      color = COLORS.cyan;
  }
  
  const formatted = `${color}[${timestamp}] [${level.toUpperCase()}] ${message}${COLORS.reset}`;
  
  try {
    console.log(formatted);
  } catch (err) {
    // Handle stdout pipe errors silently
    if (err && (err as any).code !== 'EPIPE') {
      process.stderr.write(`Error writing to console: ${err}\n`);
    }
  }
  
  try {
    fs.appendFileSync(LOG_FILE, `[${timestamp}] [${level.toUpperCase()}] ${message}\n`);
  } catch (err) {
    // Handle file write errors
    process.stderr.write(`Error writing to log file: ${err}\n`);
  }
}

/**
 * Format a number with commas
 */
function formatNumber(value: number): string {
  return value.toLocaleString();
}

/**
 * Format a percentage with 2 decimal places and % symbol
 */
function formatPercentage(value: number): string {
  return `${value.toFixed(2)}%`;
}

/**
 * Get overall reconciliation status
 */
async function getOverallStatus() {
  try {
    log('Fetching overall reconciliation status...');
    
    const result = await db.execute(sql`
      WITH expected_calcs AS (
        SELECT COUNT(*) AS count
        FROM curtailment_records
      ),
      actual_calcs AS (
        SELECT COUNT(*) AS count
        FROM historical_bitcoin_calculations
      )
      SELECT 
        e.count AS expected,
        a.count AS actual,
        ROUND((a.count::numeric / e.count::numeric) * 100, 2) AS completion_percentage
      FROM expected_calcs e, actual_calcs a
    `);
    
    // Use our helper function to safely cast the result
    const resultArray = safeResultArray(result);
    
    if (resultArray.length === 0 || !resultArray[0]) {
      log('No reconciliation data found', 'warning');
      return { expected: 0, actual: 0, completion_percentage: 0 };
    }
    
    const { expected, actual, completion_percentage } = resultArray[0];
    
    log('Overall Reconciliation Status:', 'success');
    log(`Expected calculations: ${formatNumber(expected)}`, 'info');
    log(`Actual calculations: ${formatNumber(actual)}`, 'info');
    log(`Completion percentage: ${formatPercentage(completion_percentage)}`, 'info');
    
    return { expected, actual, completion_percentage };
  } catch (error) {
    log(`Error fetching overall status: ${error}`, 'error');
    throw error;
  }
}

/**
 * Get reconciliation status by miner model
 */
async function getStatusByMinerModel() {
  try {
    log('Fetching reconciliation status by miner model...');
    
    const result = await db.execute(sql`
      WITH expected_calcs AS (
        SELECT COUNT(*) AS total
        FROM curtailment_records
      ),
      miner_status AS (
        SELECT
          miner_model,
          COUNT(*) AS count
        FROM historical_bitcoin_calculations
        GROUP BY miner_model
      )
      SELECT
        m.miner_model,
        m.count,
        (SELECT total FROM expected_calcs) AS expected_total,
        ROUND((m.count::numeric / (SELECT total FROM expected_calcs)::numeric) * 100, 2) AS percentage
      FROM miner_status m
      ORDER BY m.count DESC
    `);
    
    log('Status By Miner Model:', 'success');
    const resultArray = safeResultArray(result);
    
    if (resultArray.length === 0) {
      log('No miner model data found', 'warning');
      return [];
    }
    
    resultArray.forEach(row => {
      log(`${row.miner_model}: ${formatNumber(row.count)} / ${formatNumber(row.expected_total)} (${formatPercentage(row.percentage)})`, 'info');
    });
    
    return resultArray;
  } catch (error) {
    log(`Error fetching status by miner model: ${error}`, 'error');
    throw error;
  }
}

/**
 * Get reconciliation progress by month
 */
async function getStatusByMonth() {
  try {
    log('Fetching reconciliation status by month...');
    
    const result = await db.execute(sql`
      WITH monthly_expected AS (
        SELECT
          TO_CHAR(date, 'YYYY-MM') AS month,
          COUNT(*) AS expected
        FROM curtailment_records
        GROUP BY TO_CHAR(date, 'YYYY-MM')
      ),
      monthly_actual AS (
        SELECT
          TO_CHAR(date, 'YYYY-MM') AS month,
          COUNT(*) AS actual
        FROM historical_bitcoin_calculations
        GROUP BY TO_CHAR(date, 'YYYY-MM')
      )
      SELECT
        e.month,
        e.expected,
        COALESCE(a.actual, 0) AS actual,
        ROUND((COALESCE(a.actual, 0)::numeric / e.expected::numeric) * 100, 2) AS completion_percentage
      FROM monthly_expected e
      LEFT JOIN monthly_actual a ON e.month = a.month
      ORDER BY e.month DESC
      LIMIT 24
    `);
    
    log('Monthly Reconciliation Progress (Last 24 months):', 'success');
    const resultArray = safeResultArray(result);
    
    if (resultArray.length === 0) {
      log('No monthly reconciliation data found', 'warning');
      return [];
    }
    
    resultArray.forEach(row => {
      const completion = row.completion_percentage;
      const level = completion >= 95 ? 'success' : (completion >= 50 ? 'warning' : 'error');
      log(`${row.month}: ${formatNumber(row.actual)} / ${formatNumber(row.expected)} (${formatPercentage(row.completion_percentage)})`, level);
    });
    
    return resultArray;
  } catch (error) {
    log(`Error fetching status by month: ${error}`, 'error');
    throw error;
  }
}

/**
 * Get top problematic dates with missing calculations
 */
async function getTopMissingDates(limit: number = 10) {
  try {
    log(`Fetching top ${limit} dates with missing calculations...`);
    
    const result = await db.execute(sql`
      WITH date_expected AS (
        SELECT
          date,
          COUNT(*) AS expected
        FROM curtailment_records
        GROUP BY date
      ),
      date_actual AS (
        SELECT
          date,
          COUNT(*) AS actual
        FROM historical_bitcoin_calculations
        GROUP BY date
      )
      SELECT
        e.date,
        e.expected,
        COALESCE(a.actual, 0) AS actual,
        e.expected - COALESCE(a.actual, 0) AS missing,
        ROUND((COALESCE(a.actual, 0)::numeric / e.expected::numeric) * 100, 2) AS completion
      FROM date_expected e
      LEFT JOIN date_actual a ON e.date = a.date
      WHERE e.expected > COALESCE(a.actual, 0)
      ORDER BY missing DESC, e.date DESC
      LIMIT ${limit}
    `);
    
    log(`Top ${limit} Dates with Missing Calculations:`, 'warning');
    const resultArray = safeResultArray(result);
    
    if (resultArray.length === 0) {
      log('No missing calculations found', 'success');
      return [];
    }
    
    resultArray.forEach((row, index) => {
      log(`${index + 1}. ${format(new Date(row.date), 'yyyy-MM-dd')}: ${formatNumber(row.missing)} missing (${formatNumber(row.actual)} / ${formatNumber(row.expected)}, ${formatPercentage(row.completion)})`, 'warning');
    });
    
    return resultArray;
  } catch (error) {
    log(`Error fetching top missing dates: ${error}`, 'error');
    throw error;
  }
}

/**
 * Get recent dates status
 */
async function getRecentDatesStatus(days: number = 7) {
  try {
    log(`Fetching status for the last ${days} days...`);
    
    const result = await db.execute(sql`
      WITH date_expected AS (
        SELECT
          date,
          COUNT(*) AS expected
        FROM curtailment_records
        WHERE date >= CURRENT_DATE - INTERVAL '${days} days'
        GROUP BY date
      ),
      date_actual AS (
        SELECT
          date,
          COUNT(*) AS actual
        FROM historical_bitcoin_calculations
        WHERE date >= CURRENT_DATE - INTERVAL '${days} days'
        GROUP BY date
      )
      SELECT
        e.date,
        e.expected,
        COALESCE(a.actual, 0) AS actual,
        e.expected - COALESCE(a.actual, 0) AS missing,
        ROUND((COALESCE(a.actual, 0)::numeric / e.expected::numeric) * 100, 2) AS completion
      FROM date_expected e
      LEFT JOIN date_actual a ON e.date = a.date
      ORDER BY e.date DESC
    `);
    
    log(`Status for the Last ${days} Days:`, 'info');
    const resultArray = safeResultArray(result);
    
    if (resultArray.length === 0) {
      log(`No data found for the last ${days} days`, 'warning');
      return [];
    }
    
    resultArray.forEach(row => {
      const completion = row.completion;
      const level = completion >= 95 ? 'success' : (completion >= 50 ? 'warning' : 'error');
      log(`${format(new Date(row.date), 'yyyy-MM-dd')}: ${formatNumber(row.actual)} / ${formatNumber(row.expected)} (${formatPercentage(row.completion)})`, level);
    });
    
    return resultArray;
  } catch (error) {
    log(`Error fetching recent dates status: ${error}`, 'error');
    throw error;
  }
}

/**
 * Print database statistics
 */
async function getDatabaseStatistics() {
  try {
    log('Fetching database statistics...');
    
    const tableStats = await db.execute(sql`
      SELECT
        relname AS table_name,
        n_live_tup AS row_count,
        pg_size_pretty(pg_total_relation_size(c.oid)) AS table_size
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE relkind = 'r'
        AND n.nspname = 'public'
        AND relname IN ('curtailment_records', 'historical_bitcoin_calculations', 'bitcoin_monthly_summaries')
      ORDER BY n_live_tup DESC
    `);
    
    log('Database Table Statistics:', 'info');
    const statsArray = safeResultArray(tableStats);
    
    if (statsArray.length === 0) {
      log('No database statistics found', 'warning');
      return [];
    }
    
    statsArray.forEach(row => {
      log(`${row.table_name}: ${formatNumber(row.row_count)} rows (${row.table_size})`, 'info');
    });
    
    return statsArray;
  } catch (error) {
    log(`Error fetching database statistics: ${error}`, 'error');
    throw error;
  }
}

/**
 * Main function to generate the dashboard
 */
async function generateDashboard() {
  log('Generating Reconciliation Dashboard', 'info');
  log('===============================', 'info');
  
  try {
    await getOverallStatus();
    log('', 'info');
    
    await getStatusByMinerModel();
    log('', 'info');
    
    await getTopMissingDates();
    log('', 'info');
    
    await getRecentDatesStatus();
    log('', 'info');
    
    await getStatusByMonth();
    log('', 'info');
    
    await getDatabaseStatistics();
    log('', 'info');
    
    log('Dashboard generation complete', 'success');
  } catch (error) {
    log(`Error generating dashboard: ${error}`, 'error');
  } finally {
    process.exit(0);
  }
}

// Only run if this file is executed directly (ESM compatible)
const isMainModule = import.meta.url === `file://${process.argv[1]}`;
if (isMainModule) {
  generateDashboard();
}

export {
  getOverallStatus,
  getStatusByMinerModel,
  getStatusByMonth,
  getTopMissingDates,
  getRecentDatesStatus,
  getDatabaseStatistics
};