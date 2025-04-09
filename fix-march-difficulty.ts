/**
 * Bitcoin March 2025 Difficulty Correction Script
 * 
 * This script identifies and fixes inconsistencies in difficulty values
 * used for historical Bitcoin calculations in March 2025. It updates
 * the historical_bitcoin_calculations table with the correct difficulty
 * values from DynamoDB and recalculates the Bitcoin mined values accordingly.
 * 
 * After updating the historical calculations, it also updates all related
 * summary tables (daily, monthly, and yearly summaries) to maintain
 * consistency across the system.
 */

import { getDifficultyData } from './server/services/dynamodbService';
import { calculateBitcoin } from './server/utils/bitcoin';
import { db } from './db';
import { 
  historicalBitcoinCalculations, 
  bitcoinMonthlySummaries, 
  bitcoinYearlySummaries,
  curtailmentRecords
} from './db/schema';
import { sql, eq, and } from 'drizzle-orm';
import { format, parse } from 'date-fns';

// Dates with known inconsistencies from our analysis
const INCONSISTENT_DATES = [
  '2025-03-04',  // DB has 108105433845147, DynamoDB has 110568428300952
  '2025-03-20',  // DB has 55633605879865, DynamoDB has 112149504190349
  '2025-03-28'   // DB has mixed values, standardize to DynamoDB value 113757508810853
];

// All dates in March for updating monthly summaries
const MARCH_DATES = Array.from({ length: 31 }, (_, i) => `2025-03-${String(i + 1).padStart(2, '0')}`);

interface DifficultyUpdate {
  date: string;
  oldDifficulty: string;
  newDifficulty: string;
  recordsUpdated: number;
}

// Track updates for reporting
const updates: DifficultyUpdate[] = [];

/**
 * The main function that orchestrates the update process
 */
async function fixMarchDifficulty() {
  console.log('Starting Bitcoin difficulty correction for March 2025...');
  console.log('This script will update historical calculations with correct difficulty values from DynamoDB.');
  console.log('===============================================================================');

  try {
    // First, update difficulty values in historical_bitcoin_calculations
    for (const date of INCONSISTENT_DATES) {
      await updateDifficultyForDate(date);
    }

    // Then update all daily summaries for March
    console.log('\nUpdating daily summaries...');
    for (const date of MARCH_DATES) {
      await updateDailySummary(date);
    }

    // Update monthly summary for March 2025
    console.log('\nUpdating monthly summary for March 2025...');
    await updateMonthlySummary('2025-03');

    // Update yearly summary for 2025
    console.log('\nUpdating yearly summary for 2025...');
    await updateYearlySummary('2025');

    // Print summary of updates
    console.log('\n===============================================================================');
    console.log('SUMMARY OF DIFFICULTY UPDATES:');
    console.log('===============================================================================');
    console.log('Date       | Old Difficulty      | New Difficulty      | Records Updated');
    console.log('-----------|---------------------|---------------------|----------------');
    
    for (const update of updates) {
      console.log(
        `${update.date} | ${update.oldDifficulty.padEnd(19)} | ${update.newDifficulty.padEnd(19)} | ${update.recordsUpdated}`
      );
    }
    
    console.log('===============================================================================');
    console.log('All Bitcoin calculations for March 2025 have been updated successfully!');
    
  } catch (error) {
    console.error('Error updating Bitcoin difficulty:', error);
    process.exit(1);
  }
}

/**
 * Update the difficulty value for a specific date
 */
async function updateDifficultyForDate(date: string): Promise<void> {
  try {
    console.log(`\nProcessing ${date}...`);
    
    // Step 1: Get the current difficulty used in the database
    const currentDifficultyResult = await db.execute(sql`
      SELECT DISTINCT difficulty 
      FROM historical_bitcoin_calculations 
      WHERE settlement_date = ${date}
    `);
    
    const currentDifficulties: string[] = [];
    
    // Handle the result properly
    if (Array.isArray(currentDifficultyResult) && currentDifficultyResult.length > 0) {
      for (let i = 0; i < currentDifficultyResult.length; i++) {
        const row = currentDifficultyResult[i];
        if (row && (row as any).difficulty) {
          currentDifficulties.push((row as any).difficulty);
        }
      }
    } else {
      console.log(`Unexpected result format for ${date}`);
    }
    
    if (currentDifficulties.length === 0) {
      console.log(`No records found for ${date}`);
      return;
    }
    
    console.log(`Current difficulties in database for ${date}: ${currentDifficulties.join(', ')}`);
    
    // Step 2: Get the correct difficulty from DynamoDB
    console.log(`Fetching correct difficulty from DynamoDB for ${date}...`);
    const correctDifficulty = await getDifficultyData(date);
    console.log(`Correct difficulty from DynamoDB: ${correctDifficulty}`);
    
    // Check if already correct
    if (currentDifficulties.length === 1 && currentDifficulties[0] === String(correctDifficulty)) {
      console.log(`Difficulty already correct for ${date}, no update needed.`);
      return;
    }
    
    // Step 3: Update historical_bitcoin_calculations with the correct difficulty
    console.log(`Updating records for ${date} with correct difficulty ${correctDifficulty}...`);
    
    // First, gather all records that need updating to recalculate Bitcoin mined
    const recordsToUpdate = await db.execute(sql`
      SELECT 
        id, 
        settlement_date, 
        settlement_period, 
        farm_id,
        miner_model,
        bitcoin_mined,
        difficulty
      FROM historical_bitcoin_calculations
      WHERE settlement_date = ${date}
    `);
    
    let totalUpdated = 0;
    
    // Process records in batches to recalculate Bitcoin mined
    for (const record of recordsToUpdate) {
      const { 
        id, 
        settlement_date, 
        settlement_period, 
        farm_id,
        miner_model,
        bitcoin_mined,
        difficulty: oldDifficulty
      } = record as any;
      
      if (farm_id) {
        // Get curtailed energy for this record to recalculate
        const energyResult = await db.execute(sql`
          SELECT ABS(volume::numeric) as energy
          FROM curtailment_records
          WHERE 
            settlement_date = ${settlement_date}
            AND settlement_period = ${settlement_period}
            AND farm_id = ${farm_id}
        `);
        
        if (energyResult.length > 0) {
          const energy = Number((energyResult[0] as any).energy || 0);
          
          // Recalculate Bitcoin mined with correct difficulty
          const newBitcoinMined = calculateBitcoin(energy, miner_model, correctDifficulty);
          
          // Update the record
          await db.execute(sql`
            UPDATE historical_bitcoin_calculations
            SET 
              difficulty = ${String(correctDifficulty)},
              bitcoin_mined = ${String(newBitcoinMined)}
            WHERE id = ${id}
          `);
          
          totalUpdated++;
        }
      }
    }
    
    // Store the update for reporting
    updates.push({
      date,
      oldDifficulty: currentDifficulties.join(' or '),
      newDifficulty: String(correctDifficulty),
      recordsUpdated: totalUpdated
    });
    
    console.log(`Updated ${totalUpdated} records for ${date}`);
    
  } catch (error) {
    console.error(`Error updating difficulty for ${date}:`, error);
    throw error;
  }
}

/**
 * Update daily summary for a specific date
 */
async function updateDailySummary(date: string): Promise<void> {
  try {
    // Get unique miner models for this date
    const minerModelsResult = await db.execute(sql`
      SELECT DISTINCT miner_model
      FROM historical_bitcoin_calculations
      WHERE settlement_date = ${date}
    `);
    
    const minerModels: string[] = [];
    for (const row of minerModelsResult) {
      if ((row as any).miner_model) {
        minerModels.push((row as any).miner_model);
      }
    }
    
    if (minerModels.length === 0) {
      console.log(`No records found for ${date}`);
      return;
    }
    
    for (const minerModel of minerModels) {
      // Calculate summary for each miner model
      const result = await db.execute(sql`
        SELECT
          SUM(bitcoin_mined::NUMERIC) as total_bitcoin,
          MIN(difficulty::NUMERIC) as difficulty
        FROM
          historical_bitcoin_calculations
        WHERE
          settlement_date = ${date}
          AND miner_model = ${minerModel}
      `);
      
      const data = result[0] as any;
      
      if (!data || !data.total_bitcoin) {
        console.log(`No Bitcoin data found for ${date} and ${minerModel}`);
        continue;
      }
      
      // Delete existing summary if any
      await db.execute(sql`
        DELETE FROM bitcoin_daily_summaries
        WHERE summary_date = ${date}
        AND miner_model = ${minerModel}
      `);
      
      // Insert new summary
      await db.execute(sql`
        INSERT INTO bitcoin_daily_summaries 
        (summary_date, miner_model, bitcoin_mined, average_difficulty, updated_at)
        VALUES (
          ${date},
          ${minerModel},
          ${data.total_bitcoin.toString()},
          ${data.difficulty.toString()},
          ${new Date().toISOString()}
        )
      `);
      
      console.log(`Updated daily summary for ${date} and ${minerModel}: ${data.total_bitcoin} BTC`);
    }
  } catch (error) {
    console.error(`Error updating daily summary for ${date}:`, error);
    throw error;
  }
}

/**
 * Update monthly summary for a specific year-month
 */
async function updateMonthlySummary(yearMonth: string): Promise<void> {
  try {
    // Extract year and month from YYYY-MM format
    const [year, month] = yearMonth.split('-');
    
    if (!year || !month) {
      throw new Error(`Invalid year-month format: ${yearMonth}, expected 'YYYY-MM'`);
    }
    
    // Get unique miner models for this month
    const minerModelsResult = await db.execute(sql`
      SELECT DISTINCT miner_model
      FROM historical_bitcoin_calculations
      WHERE EXTRACT(YEAR FROM settlement_date) = ${Number(year)}
      AND EXTRACT(MONTH FROM settlement_date) = ${Number(month)}
    `);
    
    const minerModels: string[] = [];
    for (const row of minerModelsResult) {
      if ((row as any).miner_model) {
        minerModels.push((row as any).miner_model);
      }
    }
    
    if (minerModels.length === 0) {
      console.log(`No records found for ${yearMonth}`);
      return;
    }
    
    for (const minerModel of minerModels) {
      // Calculate start and end date for the month
      const startDate = `${year}-${month}-01`;
      const endDate = new Date(Number(year), Number(month), 0).toISOString().split('T')[0];
      
      // Query the historical Bitcoin calculations for the month
      const result = await db.execute(sql`
        SELECT
          SUM(bitcoin_mined::NUMERIC) as total_bitcoin,
          COUNT(DISTINCT settlement_date) as days_count
        FROM
          historical_bitcoin_calculations
        WHERE
          settlement_date >= ${startDate}
          AND settlement_date <= ${endDate}
          AND miner_model = ${minerModel}
      `);
      
      const data = result[0] as any;
      
      if (!data || !data.total_bitcoin) {
        console.log(`No Bitcoin data found for ${yearMonth} and ${minerModel}`);
        continue;
      }
      
      // Delete existing summary if any
      await db.execute(sql`
        DELETE FROM bitcoin_monthly_summaries
        WHERE year_month = ${yearMonth}
        AND miner_model = ${minerModel}
      `);
      
      // Insert new summary
      await db.execute(sql`
        INSERT INTO bitcoin_monthly_summaries 
        (year_month, miner_model, bitcoin_mined, updated_at)
        VALUES (
          ${yearMonth},
          ${minerModel},
          ${data.total_bitcoin.toString()},
          ${new Date().toISOString()}
        )
      `);
      
      console.log(`Updated monthly summary for ${yearMonth} and ${minerModel}: ${data.total_bitcoin} BTC`);
    }
  } catch (error) {
    console.error(`Error updating monthly summary for ${yearMonth}:`, error);
    throw error;
  }
}

/**
 * Update yearly summary for a specific year
 */
async function updateYearlySummary(year: string): Promise<void> {
  try {
    // Get all unique miner models in the monthly summaries for this year
    const yearPrefix = `${year}-`;
    const minerModelsResult = await db.execute(sql`
      SELECT DISTINCT miner_model
      FROM bitcoin_monthly_summaries
      WHERE year_month LIKE ${yearPrefix + '%'}
    `);
    
    // Convert result to array of miner models
    const minerModels: string[] = [];
    for (let i = 0; i < minerModelsResult.length; i++) {
      const row = minerModelsResult[i] as any;
      if (row.miner_model) {
        minerModels.push(row.miner_model);
      }
    }
    
    if (minerModels.length === 0) {
      console.log(`No miner models found for ${year}`);
      return;
    }
    
    // Process each miner model
    for (const minerModel of minerModels) {
      // Query the monthly summaries for the year for this miner model
      const monthlyResult = await db.execute(sql`
        SELECT
          SUM(bitcoin_mined::NUMERIC) as total_bitcoin,
          COUNT(*) as months_count
        FROM
          bitcoin_monthly_summaries
        WHERE
          year_month LIKE ${yearPrefix + '%'}
          AND miner_model = ${minerModel}
      `);
      
      let data: any = null;
      if (monthlyResult.length > 0) {
        data = monthlyResult[0] as any;
      }
      
      if (!data || !data.total_bitcoin) {
        console.log(`No monthly summary data found for ${year} and ${minerModel}`);
        continue;
      }
      
      // Delete existing yearly summary if any
      await db.execute(sql`
        DELETE FROM bitcoin_yearly_summaries
        WHERE year = ${year}
        AND miner_model = ${minerModel}
      `);
      
      // Insert new yearly summary
      await db.execute(sql`
        INSERT INTO bitcoin_yearly_summaries 
        (year, miner_model, bitcoin_mined, months_count, updated_at)
        VALUES (
          ${year},
          ${minerModel},
          ${data.total_bitcoin.toString()},
          ${data.months_count || 0},
          ${new Date().toISOString()}
        )
      `);
      
      console.log(`Updated yearly summary for ${year} and ${minerModel}: ${data.total_bitcoin} BTC`);
    }
  } catch (error) {
    console.error(`Error updating yearly summary for ${year}:`, error);
    throw error;
  }
}

// Execute the main function
fixMarchDifficulty().then(() => {
  console.log('Script completed successfully!');
  process.exit(0);
}).catch(error => {
  console.error('Script failed:', error);
  process.exit(1);
});