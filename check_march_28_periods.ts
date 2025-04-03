/**
 * Display March 28 Settlement Periods Detail
 * 
 * This script provides a detailed breakdown of all the settlement periods
 * we have for March 28, 2025, showing which periods have data and how much.
 */

import { db } from './db';
import { sql } from 'drizzle-orm';

const TARGET_DATE = '2025-03-28';

// Color console output
const colors = {
  info: '\x1b[36m',    // Cyan
  success: '\x1b[32m', // Green
  warning: '\x1b[33m', // Yellow
  error: '\x1b[31m',   // Red
  reset: '\x1b[0m'     // Reset
};

async function displayPeriodBreakdown() {
  console.log(`\n=== March 28, 2025 Settlement Periods Breakdown ===\n`);
  
  try {
    // Get period stats - all periods with data
    const periodStats = await db.execute(sql`
      SELECT 
        settlement_period,
        COUNT(*) AS records,
        SUM(volume) AS volume,
        SUM(payment) AS payment
      FROM curtailment_records
      WHERE settlement_date = ${TARGET_DATE}
      GROUP BY settlement_period
      ORDER BY settlement_period
    `);
    
    if (!periodStats.rows || periodStats.rows.length === 0) {
      console.log(`${colors.error}No data found for ${TARGET_DATE}${colors.reset}`);
      return;
    }
    
    // Get the overall stats
    const overallStats = await db.execute(sql`
      SELECT 
        COUNT(DISTINCT settlement_period) AS periods_count,
        COUNT(*) AS total_records,
        SUM(volume) AS total_volume,
        SUM(payment) AS total_payment
      FROM curtailment_records
      WHERE settlement_date = ${TARGET_DATE}
    `);
    
    const overall = overallStats.rows[0];
    
    // Display data in a tabular format
    console.log(`Period | Records |   Volume (MWh)  |   Payment (£)   |`);
    console.log(`-------|---------|-----------------|-----------------+`);
    
    // Create a Set of populated periods
    const populatedPeriods = new Set<number>();
    
    periodStats.rows.forEach((row) => {
      const period = Number(row.settlement_period);
      populatedPeriods.add(period);
      
      const records = Number(row.records);
      const volume = Number(row.volume);
      const payment = Number(row.payment);
      
      console.log(
        `  ${period.toString().padStart(2, ' ')}   | ${records.toString().padStart(7, ' ')} | ${volume.toFixed(2).padStart(15, ' ')} | ${payment.toFixed(2).padStart(15, ' ')} |`
      );
    });
    
    console.log(`-------|---------|-----------------|-----------------+`);
    console.log(
      `Total  | ${overall.total_records.toString().padStart(7, ' ')} | ${Number(overall.total_volume).toFixed(2).padStart(15, ' ')} | ${Number(overall.total_payment).toFixed(2).padStart(15, ' ')} |`
    );
    
    // Display periods coverage summary
    console.log(`\n${colors.info}Settlement Periods Coverage:${colors.reset}`);
    console.log(`${colors.success}${populatedPeriods.size} out of 48 periods have data (${(populatedPeriods.size / 48 * 100).toFixed(1)}% coverage)${colors.reset}`);
    
    // Show missing periods
    const missingPeriods: number[] = [];
    for (let i = 1; i <= 48; i++) {
      if (!populatedPeriods.has(i)) {
        missingPeriods.push(i);
      }
    }
    
    if (missingPeriods.length > 0) {
      console.log(`\n${colors.warning}Missing Periods:${colors.reset} ${missingPeriods.join(', ')}`);
    }
    
    // Display hourly coverage
    console.log(`\n${colors.info}Hourly Coverage:${colors.reset}`);
    for (let hour = 0; hour < 24; hour++) {
      const period1 = hour * 2 + 1;
      const period2 = hour * 2 + 2;
      
      const hasPeriod1 = populatedPeriods.has(period1);
      const hasPeriod2 = populatedPeriods.has(period2);
      
      let status = '';
      if (hasPeriod1 && hasPeriod2) {
        status = `${colors.success}[Complete]${colors.reset}`;
      } else if (hasPeriod1 || hasPeriod2) {
        status = `${colors.warning}[Partial]${colors.reset}`;
      } else {
        status = `${colors.error}[Missing]${colors.reset}`;
      }
      
      const hourFormatted = hour.toString().padStart(2, '0');
      console.log(`Hour ${hourFormatted}:00 (Periods ${period1},${period2}): ${status}`);
    }
    
  } catch (error) {
    console.error(`${colors.error}Error: ${error}${colors.reset}`);
  }
}

// Execute the function
displayPeriodBreakdown();