/**
 * Fix Bitcoin Calculations Script
 * 
 * This script provides a unified approach to fix Bitcoin calculations for any date and miner model.
 * It recalculates the bitcoin_mined values using the correct difficulty value and updates
 * all related summaries (daily, monthly, yearly).
 */

import { calculateBitcoin } from './server/utils/bitcoin';
import { db } from './db';
import { sql } from 'drizzle-orm';

// Default configuration
const DEFAULT_BITCOIN_PRICE = 65000;

/**
 * Fix Bitcoin calculations for a specific date and miner model
 */
async function fixBitcoinCalculations(date: string, minerModel: string, correctDifficulty: number, bitcoinPrice = DEFAULT_BITCOIN_PRICE) {
  try {
    console.log(`=== Fixing Bitcoin calculations for ${minerModel} on ${date} ===`);
    
    // Verify the difficulty is correct first
    const difficultyResult = await db.execute(sql`
      SELECT DISTINCT difficulty
      FROM historical_bitcoin_calculations
      WHERE settlement_date = ${date}
      AND miner_model = ${minerModel}
      LIMIT 1
    `);
    
    const currentDifficulty = difficultyResult.rows[0]?.difficulty;
    console.log(`Current difficulty for ${date}: ${currentDifficulty}`);
    
    if (currentDifficulty !== String(correctDifficulty)) {
      console.log(`Difficulty value for ${date} needs to be corrected first.`);
      console.log(`Updating to correct difficulty: ${correctDifficulty}`);
      
      await db.execute(sql`
        UPDATE historical_bitcoin_calculations
        SET difficulty = ${String(correctDifficulty)}
        WHERE settlement_date = ${date}
        AND miner_model = ${minerModel}
      `);
      
      console.log('Difficulty updated successfully');
    } else {
      console.log('Difficulty value is already correct');
    }
    
    // Show summary before update
    const beforeSummary = await db.execute(sql`
      SELECT 
        COUNT(*) as record_count,
        SUM(bitcoin_mined::numeric) as total_bitcoin
      FROM historical_bitcoin_calculations
      WHERE settlement_date = ${date}
      AND miner_model = ${minerModel}
    `);
    
    console.log(`\nBefore update - ${minerModel} on ${date}:`);
    console.log(`Records: ${beforeSummary.rows[0].record_count}`);
    console.log(`Total Bitcoin: ${beforeSummary.rows[0].total_bitcoin}`);
    
    // Recalculate Bitcoin mined for each record
    console.log(`\nRecalculating Bitcoin mined values...`);
    
    // Get energy data for all records
    const records = await db.execute(sql`
      SELECT 
        hbc.id, 
        ABS(cr.volume::numeric) as energy,
        hbc.miner_model
      FROM historical_bitcoin_calculations hbc
      JOIN curtailment_records cr ON 
        hbc.settlement_date = cr.settlement_date
        AND hbc.settlement_period = cr.settlement_period
        AND hbc.farm_id = cr.farm_id
      WHERE 
        hbc.settlement_date = ${date}
        AND hbc.miner_model = ${minerModel}
    `);
    
    // Process in batches of 50
    const BATCH_SIZE = 50;
    const batches = Math.ceil(records.rows.length / BATCH_SIZE);
    
    for (let i = 0; i < batches; i++) {
      const batch = records.rows.slice(i * BATCH_SIZE, (i + 1) * BATCH_SIZE);
      console.log(`Processing batch ${i+1}/${batches} (${batch.length} records)`);
      
      // Process this batch
      for (const record of batch) {
        const energy = Number(record.energy || 0);
        const newBitcoinMined = calculateBitcoin(energy, record.miner_model, correctDifficulty);
        
        // Update the record
        await db.execute(sql`
          UPDATE historical_bitcoin_calculations
          SET bitcoin_mined = ${String(newBitcoinMined)}
          WHERE id = ${record.id}
        `);
      }
    }
    
    // Show summary after update
    const afterSummary = await db.execute(sql`
      SELECT 
        COUNT(*) as record_count,
        SUM(bitcoin_mined::numeric) as total_bitcoin
      FROM historical_bitcoin_calculations
      WHERE settlement_date = ${date}
      AND miner_model = ${minerModel}
    `);
    
    console.log(`\nAfter update - ${minerModel} on ${date}:`);
    console.log(`Records: ${afterSummary.rows[0].record_count}`);
    console.log(`Total Bitcoin: ${afterSummary.rows[0].total_bitcoin}`);
    
    // Update daily summary
    console.log(`\nUpdating daily summary for ${date}...`);
    
    const dailyData = await db.execute(sql`
      SELECT
        SUM(bitcoin_mined::NUMERIC) as total_bitcoin,
        MIN(difficulty::NUMERIC) as difficulty
      FROM
        historical_bitcoin_calculations
      WHERE
        settlement_date = ${date}
        AND miner_model = ${minerModel}
    `);
    
    const valueAtMining = Number(dailyData.rows[0].total_bitcoin) * bitcoinPrice;
    
    console.log(`Using Bitcoin price of $${bitcoinPrice} for value_at_mining calculation`);
    console.log(`Value at mining: $${valueAtMining.toFixed(2)}`);
    
    // Delete existing summary if any
    await db.execute(sql`
      DELETE FROM bitcoin_daily_summaries
      WHERE summary_date = ${date}
      AND miner_model = ${minerModel}
    `);
    
    // Insert new summary
    await db.execute(sql`
      INSERT INTO bitcoin_daily_summaries (
        summary_date,
        miner_model,
        bitcoin_mined,
        value_at_mining,
        average_difficulty
      ) VALUES (
        ${date},
        ${minerModel},
        ${dailyData.rows[0].total_bitcoin},
        ${String(valueAtMining)},
        ${dailyData.rows[0].difficulty}
      )
    `);
    
    console.log(`Updated daily summary for ${date}`);
    
    // Update monthly summary for the relevant month
    const yearMonth = date.substring(0, 7);
    console.log(`\nUpdating monthly summary for ${yearMonth}...`);
    
    const monthlyData = await db.execute(sql`
      SELECT
        SUM(bitcoin_mined::NUMERIC) as total_bitcoin,
        SUM(value_at_mining::NUMERIC) as total_value
      FROM
        bitcoin_daily_summaries
      WHERE
        summary_date >= ${yearMonth + '-01'}
        AND summary_date <= ${yearMonth + '-31'}
        AND miner_model = ${minerModel}
    `);
    
    // Delete existing monthly summary
    await db.execute(sql`
      DELETE FROM bitcoin_monthly_summaries
      WHERE year_month = ${yearMonth}
      AND miner_model = ${minerModel}
    `);
    
    // Insert new monthly summary
    await db.execute(sql`
      INSERT INTO bitcoin_monthly_summaries (
        year_month,
        miner_model,
        bitcoin_mined,
        value_at_mining
      ) VALUES (
        ${yearMonth},
        ${minerModel},
        ${monthlyData.rows[0].total_bitcoin},
        ${monthlyData.rows[0].total_value}
      )
    `);
    
    console.log(`Updated monthly summary for ${yearMonth}`);
    
    // Update yearly summary for the relevant year
    const year = date.substring(0, 4);
    console.log(`\nUpdating yearly summary for ${year}...`);
    
    const yearlyData = await db.execute(sql`
      SELECT
        SUM(bitcoin_mined::NUMERIC) as total_bitcoin,
        SUM(value_at_mining::NUMERIC) as total_value
      FROM
        bitcoin_monthly_summaries
      WHERE
        year_month LIKE ${year + '-%'}
        AND miner_model = ${minerModel}
    `);
    
    // Delete existing yearly summary
    await db.execute(sql`
      DELETE FROM bitcoin_yearly_summaries
      WHERE year = ${year}
      AND miner_model = ${minerModel}
    `);
    
    // Insert new yearly summary
    await db.execute(sql`
      INSERT INTO bitcoin_yearly_summaries (
        year,
        miner_model,
        bitcoin_mined,
        value_at_mining
      ) VALUES (
        ${year},
        ${minerModel},
        ${yearlyData.rows[0].total_bitcoin},
        ${yearlyData.rows[0].total_value}
      )
    `);
    
    console.log(`Updated yearly summary for ${year}`);
    
    console.log('\n=== Fix completed successfully ===');
    return true;
  } catch (error) {
    console.error('Error fixing Bitcoin calculations:', error);
    return false;
  }
}

/**
 * Fix Bitcoin calculations for all active miner models on a specific date
 */
async function fixAllMinerModelsForDate(date: string, correctDifficulty: number, bitcoinPrice = DEFAULT_BITCOIN_PRICE) {
  console.log(`=== Fixing all miner models for ${date} ===`);
  
  // Get all miner models that have data for the given date
  const minerModelsResult = await db.execute(sql`
    SELECT DISTINCT miner_model
    FROM historical_bitcoin_calculations
    WHERE settlement_date = ${date}
    ORDER BY miner_model
  `);
  
  const minerModels = minerModelsResult.rows.map(row => row.miner_model);
  console.log(`Found ${minerModels.length} miner models for ${date}:`);
  console.log(minerModels.join(', '));
  
  const results = [];
  
  // Process each miner model
  for (const minerModel of minerModels) {
    console.log(`\n\n--- Processing ${minerModel} ---`);
    const success = await fixBitcoinCalculations(date, minerModel, correctDifficulty, bitcoinPrice);
    results.push({
      minerModel,
      success
    });
  }
  
  // Print summary
  console.log('\n=== Summary of fixes ===');
  for (const result of results) {
    console.log(`${result.minerModel}: ${result.success ? 'Success' : 'Failed'}`);
  }
  
  return results;
}

/**
 * Fix Bitcoin calculations for multiple dates
 * This is useful for applying fixes to a range of dates or non-consecutive dates
 */
async function fixMultipleDates(dates: string[], minerModel: string, difficultyMap: Record<string, number>, bitcoinPrice = DEFAULT_BITCOIN_PRICE) {
  console.log(`=== Fixing ${dates.length} dates for ${minerModel} ===`);
  
  const results = [];
  
  for (const date of dates) {
    const difficulty = difficultyMap[date];
    if (!difficulty) {
      console.error(`No difficulty provided for ${date}`);
      results.push({
        date,
        success: false,
        reason: 'No difficulty provided'
      });
      continue;
    }
    
    console.log(`\n\n--- Processing ${date} with difficulty ${difficulty} ---`);
    const success = await fixBitcoinCalculations(date, minerModel, difficulty, bitcoinPrice);
    results.push({
      date,
      success,
      difficulty
    });
  }
  
  // Print summary
  console.log('\n=== Summary of fixes ===');
  for (const result of results) {
    console.log(`${result.date}: ${result.success ? 'Success' : 'Failed'}`);
  }
  
  return results;
}

// Example usage
async function main() {
  // Example 1: Fix a specific date and miner model
  // await fixBitcoinCalculations('2025-03-31', 'S19J_PRO', 113757508810853);
  
  // Example 2: Fix all miner models for a specific date
  // await fixAllMinerModelsForDate('2025-03-31', 113757508810853);
  
  // Example 3: Fix multiple dates for a specific miner model
  // const difficultiesForMarch2025 = {
  //   '2025-03-04': 110568428300952,
  //   '2025-03-20': 112149504190349,
  //   '2025-03-28': 113757508810853,
  //   '2025-03-31': 113757508810853
  // };
  // await fixMultipleDates(['2025-03-04', '2025-03-20', '2025-03-28', '2025-03-31'], 'S19J_PRO', difficultiesForMarch2025);
  
  console.log('Run one of the example functions to fix Bitcoin calculations.');
}

if (require.main === module) {
  main().catch(console.error);
}

// Export functions for use in other scripts
export {
  fixBitcoinCalculations,
  fixAllMinerModelsForDate,
  fixMultipleDates
};