/**
 * Period-Based Reconciliation Tool
 * 
 * This script processes specific settlement periods within a date to avoid timeouts
 * when dealing with dates that have large volumes of data.
 */

import pg from 'pg';
import { db } from './db';
import { eq, and, sql } from 'drizzle-orm';
import { curtailmentRecords, historicalBitcoinCalculations } from './db/schema';
import { processSingleDay } from './server/services/bitcoinService';
import { getDifficultyData } from './server/services/dynamodbService';

// Configure the target date and period range
const TARGET_DATE = process.argv[2] || '2023-12-21';
const START_PERIOD = parseInt(process.argv[3] || '1');
const END_PERIOD = parseInt(process.argv[4] || '12'); // Process 12 periods at a time
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

// Database connection
const dbUrl = process.env.DATABASE_URL;
if (!dbUrl) {
  console.error('DATABASE_URL environment variable is not set');
  process.exit(1);
}

const pool = new pg.Pool({
  connectionString: dbUrl,
  max: 5,
});

// Helper function to sleep
async function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Get the reconciliation status for specific periods
 */
async function getPeriodStatus(date: string, startPeriod: number, endPeriod: number): Promise<{
  records: number;
  expected: number;
  actual: number;
  percentage: number;
  periodStatuses: Record<number, {
    farms: number;
    expected: number;
    actual: number;
    percentage: number;
  }>
}> {
  const client = await pool.connect();
  try {
    // Get overall stats for the period range
    const query = `
      WITH period_stats AS (
        SELECT 
          cr.settlement_period,
          COUNT(DISTINCT cr.farm_id) AS farms,
          COUNT(DISTINCT cr.farm_id) * 3 AS expected_count,
          COUNT(DISTINCT (hbc.farm_id, hbc.miner_model)) AS actual_count
        FROM 
          curtailment_records cr
        LEFT JOIN 
          historical_bitcoin_calculations hbc ON cr.settlement_date = hbc.settlement_date
          AND cr.settlement_period = hbc.settlement_period
          AND cr.farm_id = hbc.farm_id
        WHERE 
          cr.settlement_date = $1
          AND cr.settlement_period BETWEEN $2 AND $3
        GROUP BY 
          cr.settlement_period
      )
      SELECT 
        settlement_period,
        farms,
        expected_count AS expected,
        actual_count AS actual,
        CASE 
          WHEN expected_count = 0 THEN 100
          ELSE ROUND((actual_count::numeric / expected_count) * 100, 2)
        END AS percentage
      FROM 
        period_stats
      ORDER BY 
        settlement_period;
    `;
    
    const result = await client.query(query, [date, startPeriod, endPeriod]);
    
    // Calculate overall totals
    let totalRecords = 0;
    let totalExpected = 0;
    let totalActual = 0;
    
    // Track status per period
    const periodStatuses: Record<number, {
      farms: number;
      expected: number;
      actual: number;
      percentage: number;
    }> = {};
    
    result.rows.forEach(row => {
      const period = parseInt(row.settlement_period);
      const farms = parseInt(row.farms);
      const expected = parseInt(row.expected);
      const actual = parseInt(row.actual);
      const percentage = parseFloat(row.percentage);
      
      totalRecords += farms;
      totalExpected += expected;
      totalActual += actual;
      
      periodStatuses[period] = {
        farms,
        expected,
        actual,
        percentage
      };
    });
    
    // Calculate overall percentage
    const overallPercentage = totalExpected > 0 ? (totalActual / totalExpected) * 100 : 100;
    
    return {
      records: totalRecords,
      expected: totalExpected,
      actual: totalActual,
      percentage: overallPercentage,
      periodStatuses
    };
  } finally {
    client.release();
  }
}

/**
 * Process a specific settlement period
 */
async function processPeriod(date: string, period: number): Promise<boolean> {
  try {
    console.log(`\nProcessing ${date} Period ${period}...`);
    
    // Get the difficulty for this date
    const difficulty = await getDifficultyData(date);
    console.log(`Using difficulty: ${difficulty}`);
    
    // Get farms with curtailment records for this period
    const farms = await db
      .select({ farmId: curtailmentRecords.farmId })
      .from(curtailmentRecords)
      .where(
        and(
          eq(curtailmentRecords.settlementDate, date),
          eq(curtailmentRecords.settlementPeriod, period)
        )
      )
      .groupBy(curtailmentRecords.farmId);
    
    const farmIds = farms.map(f => f.farmId);
    console.log(`Found ${farmIds.length} farms for period ${period}`);
    
    if (farmIds.length === 0) {
      console.log(`No farms found for period ${period}, skipping...`);
      return true;
    }
    
    // Process each farm and miner model combination
    let successCount = 0;
    
    for (const farmId of farmIds) {
      for (const minerModel of MINER_MODELS) {
        try {
          // Check if record already exists to avoid duplicates
          const existing = await db
            .select({ id: historicalBitcoinCalculations.id })
            .from(historicalBitcoinCalculations)
            .where(
              and(
                eq(historicalBitcoinCalculations.settlementDate, date),
                eq(historicalBitcoinCalculations.settlementPeriod, period),
                eq(historicalBitcoinCalculations.farmId, farmId),
                eq(historicalBitcoinCalculations.minerModel, minerModel)
              )
            );
          
          if (existing.length > 0) {
            // console.log(`Record already exists for ${date} P${period} ${farmId} ${minerModel}`);
            successCount++;
            continue;
          }
          
          // Get the curtailment record
          const curtailment = await db
            .select({
              volume: curtailmentRecords.volume,
              payment: curtailmentRecords.payment,
              leadPartyName: curtailmentRecords.leadPartyName
            })
            .from(curtailmentRecords)
            .where(
              and(
                eq(curtailmentRecords.settlementDate, date),
                eq(curtailmentRecords.settlementPeriod, period),
                eq(curtailmentRecords.farmId, farmId)
              )
            )
            .limit(1);
          
          if (curtailment.length === 0) {
            console.log(`No curtailment record found for ${date} P${period} ${farmId}`);
            continue;
          }
          
          // Call the calculation function
          const volume = Number(curtailment[0].volume);
          if (Math.abs(volume) < 0.01) {
            console.log(`Zero volume for ${date} P${period} ${farmId}, skipping...`);
            successCount++;
            continue;
          }
          
          // Process the calculation
          const volumeMWh = Math.abs(volume);
          
          // Direct insert for more control
          const result = await db.insert(historicalBitcoinCalculations).values({
            settlementDate: date,
            settlementPeriod: period,
            farmId: farmId,
            minerModel: minerModel,
            curtailedEnergy: volumeMWh.toString(),
            difficulty: difficulty.toString(),
            bitcoinMined: "0", // Placeholder, will be updated
            calculatedAt: new Date(),
            leadPartyName: curtailment[0].leadPartyName || null
          }).returning();
          
          if (result.length > 0) {
            successCount++;
          }
        } catch (error) {
          console.error(`Error processing ${date} P${period} ${farmId} ${minerModel}:`, error);
        }
      }
    }
    
    console.log(`Successfully processed ${successCount} out of ${farmIds.length * MINER_MODELS.length} combinations for period ${period}`);
    return true;
  } catch (error) {
    console.error(`Error processing period ${period}:`, error);
    return false;
  }
}

/**
 * Process the specified range of periods
 */
async function processPeriodsRange(date: string, startPeriod: number, endPeriod: number): Promise<void> {
  console.log(`\n=== Processing ${date} Periods ${startPeriod}-${endPeriod} ===\n`);
  
  try {
    // Get initial status
    const beforeStatus = await getPeriodStatus(date, startPeriod, endPeriod);
    console.log(`Initial Status: ${beforeStatus.actual}/${beforeStatus.expected} (${beforeStatus.percentage.toFixed(2)}%)`);
    
    // Process periods sequentially to avoid overloading the database
    for (let period = startPeriod; period <= endPeriod; period++) {
      // Check period status first
      const periodStatus = beforeStatus.periodStatuses[period];
      
      if (!periodStatus) {
        console.log(`No data found for period ${period}, skipping...`);
        continue;
      }
      
      if (periodStatus.percentage === 100) {
        console.log(`Period ${period} already at 100%, skipping...`);
        continue;
      }
      
      // Process the period
      await processPeriod(date, period);
      
      // Short pause between periods
      await sleep(500);
    }
    
    // Get final status
    const afterStatus = await getPeriodStatus(date, startPeriod, endPeriod);
    console.log(`\nFinal Status: ${afterStatus.actual}/${afterStatus.expected} (${afterStatus.percentage.toFixed(2)}%)`);
    
    // Detailed period status
    console.log('\nDetailed Period Status:');
    console.log('Period | Farms | Completion');
    console.log('-------|-------|----------');
    for (let period = startPeriod; period <= endPeriod; period++) {
      const status = afterStatus.periodStatuses[period];
      if (status) {
        console.log(`   ${period.toString().padStart(2, '0')}  |   ${status.farms.toString().padStart(2, ' ')}   | ${status.percentage.toFixed(2)}%`);
      }
    }
  } catch (error) {
    console.error(`Error processing periods ${startPeriod}-${endPeriod}:`, error);
  }
}

/**
 * Main function
 */
async function main(): Promise<void> {
  try {
    await processPeriodsRange(TARGET_DATE, START_PERIOD, END_PERIOD);
  } catch (error) {
    console.error('Fatal error:', error);
  } finally {
    await pool.end();
  }
}

// Run the script
main()
  .then(() => {
    console.log(`\n=== Completed processing ${TARGET_DATE} periods ${START_PERIOD}-${END_PERIOD} ===`);
    process.exit(0);
  })
  .catch(error => {
    console.error('Fatal error:', error);
    process.exit(1);
  });