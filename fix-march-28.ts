/**
 * Fix Bitcoin difficulty for March 28, 2025
 * 
 * This script standardizes the difficulty value for March 28 in historical_bitcoin_calculations
 * to match the correct value from DynamoDB, and recalculates the bitcoin_mined accordingly.
 */

import { getDifficultyData } from './server/services/dynamodbService';
import { calculateBitcoin } from './server/utils/bitcoin';
import { db } from './db';
import { sql } from 'drizzle-orm';

const DATE_TO_UPDATE = '2025-03-28';

async function fixMarchDifficulty() {
  console.log(`Starting Bitcoin difficulty correction for ${DATE_TO_UPDATE}...`);

  try {
    // Get the current difficulty in the database
    const currentDifficultyResult = await db.execute(sql`
      SELECT DISTINCT difficulty::text as difficulty 
      FROM historical_bitcoin_calculations 
      WHERE settlement_date = ${DATE_TO_UPDATE}
    `);
    
    if (!currentDifficultyResult.rows || currentDifficultyResult.rows.length === 0) {
      console.log(`No records found for ${DATE_TO_UPDATE}`);
      return;
    }
    
    const currentDifficulties = currentDifficultyResult.rows.map(row => row.difficulty);
    console.log(`Current difficulties in database: ${currentDifficulties.join(', ')}`);
    
    // Get the correct difficulty from DynamoDB
    console.log(`Fetching correct difficulty from DynamoDB...`);
    const correctDifficulty = await getDifficultyData(DATE_TO_UPDATE);
    console.log(`Correct difficulty from DynamoDB: ${correctDifficulty}`);
    
    if (currentDifficulties.length === 1 && currentDifficulties[0] === String(correctDifficulty)) {
      console.log(`Difficulty already correct, no update needed.`);
      return;
    }
    
    // Update the difficulty in all records for this date
    console.log(`Updating difficulty values in the database...`);
    const updateResult = await db.execute(sql`
      UPDATE historical_bitcoin_calculations
      SET difficulty = ${String(correctDifficulty)}
      WHERE settlement_date = ${DATE_TO_UPDATE}
    `);
    
    console.log(`Updated difficulty for all records on ${DATE_TO_UPDATE}`);
    
    // Recalculate Bitcoin mined for each record
    console.log(`Recalculating Bitcoin mined values...`);
    
    const recordsResult = await db.execute(sql`
      SELECT 
        id, 
        settlement_date, 
        settlement_period, 
        farm_id,
        miner_model
      FROM historical_bitcoin_calculations
      WHERE settlement_date = ${DATE_TO_UPDATE}
    `);
    
    let updatedCount = 0;
    
    // Process each record to recalculate Bitcoin mined
    if (recordsResult.rows) {
      for (const record of recordsResult.rows) {
        if (record.farm_id) {
          // Get energy for this record
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
            
            // Recalculate Bitcoin mined
            const newBitcoinMined = calculateBitcoin(energy, record.miner_model, correctDifficulty);
            
            // Update the record
            await db.execute(sql`
              UPDATE historical_bitcoin_calculations
              SET bitcoin_mined = ${String(newBitcoinMined)}
              WHERE id = ${record.id}
            `);
            
            updatedCount++;
            
            // Log progress every 100 records
            if (updatedCount % 100 === 0) {
              console.log(`Updated ${updatedCount} records...`);
            }
          }
        }
      }
    }
    
    console.log(`\nUpdate completed! Recalculated Bitcoin mined for ${updatedCount} records.`);
    
    // Update daily summary
    console.log(`\nUpdating daily summary for ${DATE_TO_UPDATE}...`);
    await updateDailySummary(DATE_TO_UPDATE);
    
    // Update monthly summary
    console.log(`\nUpdating monthly summary for 2025-03...`);
    await updateMonthlySummary('2025-03');
    
    // Update yearly summary
    console.log(`\nUpdating yearly summary for 2025...`);
    await updateYearlySummary('2025');
    
    console.log(`\nAll updates completed successfully!`);
    
  } catch (error) {
    console.error('Error updating Bitcoin difficulty:', error);
    process.exit(1);
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
    if (minerModelsResult.rows) {
      for (const row of minerModelsResult.rows) {
        if (row.miner_model) {
          minerModels.push(row.miner_model);
        }
      }
    }
    
    if (minerModels.length === 0) {
      console.log(`No records found for ${date}`);
      return;
    }
    
    // Process each miner model
    for (const minerModel of minerModels) {
      // Calculate daily totals
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
      
      if (result.rows && result.rows.length > 0) {
        const totalBitcoin = result.rows[0].total_bitcoin;
        const difficulty = result.rows[0].difficulty;
        
        if (totalBitcoin && difficulty) {
          // Delete existing summary
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
              ${totalBitcoin.toString()},
              ${difficulty.toString()},
              ${new Date().toISOString()}
            )
          `);
          
          console.log(`Updated daily summary for ${date} and ${minerModel}: ${totalBitcoin} BTC`);
        }
      }
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
      const endDate = new Date(Number(year), Number(month), 0).toISOString().split('T')[0];
      
      // Calculate monthly total
      const result = await db.execute(sql`
        SELECT
          SUM(bitcoin_mined::NUMERIC) as total_bitcoin
        FROM
          historical_bitcoin_calculations
        WHERE
          settlement_date >= ${startDate}
          AND settlement_date <= ${endDate}
          AND miner_model = ${minerModel}
      `);
      
      if (result.rows && result.rows.length > 0) {
        const totalBitcoin = result.rows[0].total_bitcoin;
        
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
        }
      }
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
      // Calculate yearly total from monthly summaries
      const result = await db.execute(sql`
        SELECT
          SUM(bitcoin_mined::NUMERIC) as total_bitcoin,
          COUNT(*) as months_count
        FROM
          bitcoin_monthly_summaries
        WHERE
          year_month LIKE ${yearPrefix + '%'}
          AND miner_model = ${minerModel}
      `);
      
      if (result.rows && result.rows.length > 0) {
        const totalBitcoin = result.rows[0].total_bitcoin;
        const monthsCount = result.rows[0].months_count;
        
        if (totalBitcoin) {
          // Delete existing summary
          await db.execute(sql`
            DELETE FROM bitcoin_yearly_summaries
            WHERE year = ${year}
            AND miner_model = ${minerModel}
          `);
          
          // Insert new summary
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