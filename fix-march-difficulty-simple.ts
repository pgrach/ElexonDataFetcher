/**
 * Bitcoin March 2025 Difficulty Correction Script (Simplified)
 * 
 * This script focuses on fixing the three dates with inconsistencies (March 4, March 20, March 28)
 * and updates the difficulty values in historical_bitcoin_calculations to match DynamoDB values.
 */

import { getDifficultyData } from './server/services/dynamodbService';
import { calculateBitcoin } from './server/utils/bitcoin';
import { db } from './db';
import { sql } from 'drizzle-orm';

// Dates with known inconsistencies from our analysis
const INCONSISTENT_DATES = [
  '2025-03-04',  // DB has 108105433845147, DynamoDB has 110568428300952
  '2025-03-20',  // DB has 55633605879865, DynamoDB has 112149504190349
  '2025-03-28'   // DB has mixed values, standardize to DynamoDB value 113757508810853
];

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
    // Update the three problematic dates
    for (const date of INCONSISTENT_DATES) {
      await updateDifficultyForDate(date);
    }

    // Update the monthly summary for March 2025
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
      SELECT DISTINCT difficulty::text as difficulty 
      FROM historical_bitcoin_calculations 
      WHERE settlement_date = ${date}
    `);
    
    // Extract unique difficulties
    const currentDifficulties: string[] = [];
    if (Array.isArray(currentDifficultyResult.rows)) {
      for (const row of currentDifficultyResult.rows) {
        if (row.difficulty && !currentDifficulties.includes(row.difficulty)) {
          currentDifficulties.push(row.difficulty);
        }
      }
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
    
    // First, gather all records that need updating
    const recordsResult = await db.execute(sql`
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
    
    // Process each record to recalculate Bitcoin mined
    if (Array.isArray(recordsResult.rows)) {
      for (const record of recordsResult.rows) {
        if (record.farm_id) {
          // Get curtailed energy for this record to recalculate
          const energyResult = await db.execute(sql`
            SELECT ABS(volume::numeric) as energy
            FROM curtailment_records
            WHERE 
              settlement_date = ${record.settlement_date}
              AND settlement_period = ${record.settlement_period}
              AND farm_id = ${record.farm_id}
          `);
          
          if (energyResult.rows && energyResult.rows.length > 0) {
            const energy = Number(energyResult.rows[0].energy || 0);
            
            // Recalculate Bitcoin mined with correct difficulty
            const newBitcoinMined = calculateBitcoin(energy, record.miner_model, correctDifficulty);
            
            // Update the record
            await db.execute(sql`
              UPDATE historical_bitcoin_calculations
              SET 
                difficulty = ${String(correctDifficulty)},
                bitcoin_mined = ${String(newBitcoinMined)}
              WHERE id = ${record.id}
            `);
            
            totalUpdated++;
          }
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
 * Update monthly summary for March 2025
 */
async function updateMonthlySummary(yearMonth: string): Promise<void> {
  try {
    // Extract year and month from YYYY-MM format
    const [year, month] = yearMonth.split('-');
    
    if (!year || !month) {
      throw new Error(`Invalid year-month format: ${yearMonth}, expected 'YYYY-MM'`);
    }
    
    // Get all miner models
    const minerModelsResult = await db.execute(sql`
      SELECT DISTINCT miner_model
      FROM historical_bitcoin_calculations
      WHERE EXTRACT(YEAR FROM settlement_date) = ${Number(year)}
      AND EXTRACT(MONTH FROM settlement_date) = ${Number(month)}
    `);
    
    const minerModels: string[] = [];
    if (minerModelsResult.rows) {
      for (const row of minerModelsResult.rows) {
        if (row.miner_model) {
          minerModels.push(row.miner_model);
        }
      }
    }
    
    if (minerModels.length === 0) {
      console.log(`No records found for ${yearMonth}`);
      return;
    }
    
    // Process each miner model
    for (const minerModel of minerModels) {
      // Calculate start and end date for the month
      const startDate = `${year}-${month}-01`;
      // Get the last day of the month (accounting for leap years)
      const endDate = new Date(Number(year), Number(month), 0).toISOString().split('T')[0];
      
      // Query for total Bitcoin mined in the month
      const bitcoinResult = await db.execute(sql`
        SELECT SUM(bitcoin_mined::NUMERIC) as total_bitcoin
        FROM historical_bitcoin_calculations
        WHERE settlement_date >= ${startDate}
        AND settlement_date <= ${endDate}
        AND miner_model = ${minerModel}
      `);
      
      if (bitcoinResult.rows && bitcoinResult.rows.length > 0) {
        const totalBitcoin = bitcoinResult.rows[0].total_bitcoin;
        
        if (totalBitcoin) {
          // Delete existing summary
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
              ${totalBitcoin.toString()},
              ${new Date().toISOString()}
            )
          `);
          
          console.log(`Updated monthly summary for ${yearMonth} and ${minerModel}: ${totalBitcoin} BTC`);
        } else {
          console.log(`No Bitcoin data found for ${yearMonth} and ${minerModel}`);
        }
      }
    }
  } catch (error) {
    console.error(`Error updating monthly summary for ${yearMonth}:`, error);
    throw error;
  }
}

/**
 * Update yearly summary for 2025
 */
async function updateYearlySummary(year: string): Promise<void> {
  try {
    // Get all miner models for the year
    const yearPrefix = `${year}-`;
    const minerModelsResult = await db.execute(sql`
      SELECT DISTINCT miner_model
      FROM bitcoin_monthly_summaries
      WHERE year_month LIKE ${yearPrefix + '%'}
    `);
    
    const minerModels: string[] = [];
    if (minerModelsResult.rows) {
      for (const row of minerModelsResult.rows) {
        if (row.miner_model) {
          minerModels.push(row.miner_model);
        }
      }
    }
    
    if (minerModels.length === 0) {
      console.log(`No miner models found for ${year}`);
      return;
    }
    
    // Process each miner model
    for (const minerModel of minerModels) {
      // Get total Bitcoin from monthly summaries
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
      
      if (monthlyResult.rows && monthlyResult.rows.length > 0) {
        const totalBitcoin = monthlyResult.rows[0].total_bitcoin;
        const monthsCount = monthlyResult.rows[0].months_count;
        
        if (totalBitcoin) {
          // Delete existing yearly summary
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
              ${totalBitcoin.toString()},
              ${monthsCount || 0},
              ${new Date().toISOString()}
            )
          `);
          
          console.log(`Updated yearly summary for ${year} and ${minerModel}: ${totalBitcoin} BTC`);
        } else {
          console.log(`No monthly summary data found for ${year} and ${minerModel}`);
        }
      }
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