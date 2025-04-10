/**
 * Fix April 1, 2025 Bitcoin Calculations with DynamoDB Difficulty
 * 
 * This script retrieves the proper difficulty value from DynamoDB for April 1, 2025
 * and recalculates all Bitcoin mining data correctly, following the proper 
 * historical_bitcoin_calculations -> daily -> monthly hierarchy.
 */

import { db } from "../../db";
import { 
  historicalBitcoinCalculations, 
  curtailmentRecords,
  bitcoinDailySummaries,
  bitcoinMonthlySummaries
} from "../../db/schema";
import { eq, and, sql } from "drizzle-orm";
import { calculateBitcoin } from "../utils/bitcoin";
import { getDifficultyData } from "../services/dynamodbService";
import fs from 'fs';
import path from 'path';

// Target date for processing
const TARGET_DATE = "2025-04-01";

// Miner models to process
const MINER_MODELS = ["S19J_PRO", "S9", "M20S"];

// Create a log file
const LOG_DIR = path.join(process.cwd(), 'logs');
const LOG_FILE = path.join(LOG_DIR, `fix_april1_2025_dynamodb_${new Date().toISOString().replace(/:/g, '-')}.log`);

if (!fs.existsSync(LOG_DIR)) {
  fs.mkdirSync(LOG_DIR, { recursive: true });
}

// Log helper function
function logMessage(message: string) {
  const timestamp = new Date().toISOString();
  const logEntry = `[${timestamp}] ${message}\n`;
  
  // Log to console and file
  console.log(message);
  fs.appendFileSync(LOG_FILE, logEntry);
}

async function main() {
  try {
    logMessage(`\n===== FIXING BITCOIN CALCULATIONS FOR ${TARGET_DATE} WITH DYNAMODB DIFFICULTY =====\n`);

    // First, get the difficulty value from DynamoDB
    logMessage("Fetching difficulty value from DynamoDB...");
    const difficulty = await getDifficultyData(TARGET_DATE);
    logMessage(`Retrieved difficulty value from DynamoDB: ${difficulty}`);
    
    if (!difficulty || difficulty <= 0) {
      throw new Error(`Invalid difficulty value retrieved from DynamoDB: ${difficulty}`);
    }

    // Get the total curtailment energy for this date
    const totalResult = await db.execute(sql`
      SELECT 
        COUNT(*) as record_count,
        COUNT(DISTINCT farm_id) as farm_count,
        COUNT(DISTINCT settlement_period) as period_count,
        SUM(ABS(volume)) as total_energy
      FROM curtailment_records
      WHERE settlement_date = ${TARGET_DATE}
    `);
    
    const totalEnergy = parseFloat(totalResult.rows[0].total_energy);
    logMessage(`Found ${totalResult.rows[0].record_count} curtailment records from ${totalResult.rows[0].farm_count} farms across ${totalResult.rows[0].period_count} settlement periods`);
    logMessage(`Total curtailed energy: ${totalEnergy.toFixed(2)} MWh`);
    
    // Aggregate curtailment data by settlement_period and farm_id to handle duplicates
    logMessage("\nAggregating curtailment data to handle duplicates...");
    const aggregatedResult = await db.execute(sql`
      SELECT 
        settlement_period,
        farm_id,
        lead_party_name,
        SUM(ABS(volume)) as total_volume,
        COUNT(*) as record_count
      FROM curtailment_records
      WHERE settlement_date = ${TARGET_DATE}
      GROUP BY settlement_period, farm_id, lead_party_name
    `);
    
    const aggregatedRecords = aggregatedResult.rows;
    logMessage(`Aggregated to ${aggregatedRecords.length} unique period-farm combinations`);
    
    // Process each miner model
    for (const minerModel of MINER_MODELS) {
      logMessage(`\n--- Processing ${minerModel} ---`);
      
      // Check current state before making changes
      const beforeResult = await db.execute(sql`
        SELECT COUNT(*) as record_count, SUM(bitcoin_mined::numeric) as total_bitcoin
        FROM historical_bitcoin_calculations
        WHERE settlement_date = ${TARGET_DATE} AND miner_model = ${minerModel}
      `);
      
      logMessage(`Current state: ${beforeResult.rows[0]?.record_count || 0} records, ${beforeResult.rows[0]?.total_bitcoin || 0} BTC`);
      
      // Clear existing calculations for this date and miner model
      const deleteResult = await db.execute(sql`
        DELETE FROM historical_bitcoin_calculations
        WHERE settlement_date = ${TARGET_DATE} AND miner_model = ${minerModel}
      `);
      
      logMessage(`Deleted ${deleteResult.rowCount} existing records`);
      
      // Process aggregated records
      let totalBitcoin = 0;
      let insertedCount = 0;
      
      for (const record of aggregatedRecords) {
        const mwh = Number(record.total_volume);
        
        if (mwh <= 0 || isNaN(mwh)) {
          continue;
        }
        
        // Use the difficulty value from DynamoDB
        const bitcoinMined = calculateBitcoin(mwh, minerModel, difficulty);
        totalBitcoin += bitcoinMined;
        
        try {
          await db.execute(sql`
            INSERT INTO historical_bitcoin_calculations 
              (settlement_date, settlement_period, farm_id, miner_model, bitcoin_mined, difficulty, calculated_at)
            VALUES (
              ${TARGET_DATE}, 
              ${Number(record.settlement_period)}, 
              ${record.farm_id}, 
              ${minerModel}, 
              ${bitcoinMined.toString()}, 
              ${difficulty.toString()},
              NOW()
            )
          `);
          
          insertedCount++;
        } catch (err) {
          logMessage(`Error inserting record for period ${record.settlement_period}, farm ${record.farm_id}: ${err}`);
        }
      }
      
      logMessage(`Successfully processed ${insertedCount} aggregated records`);
      logMessage(`Total Bitcoin calculated: ${totalBitcoin.toFixed(8)} BTC`);
      
      // Verify the results
      const afterResult = await db.execute(sql`
        SELECT COUNT(*) as record_count, SUM(bitcoin_mined::numeric) as total_bitcoin
        FROM historical_bitcoin_calculations
        WHERE settlement_date = ${TARGET_DATE} AND miner_model = ${minerModel}
      `);
      
      logMessage(`Updated state: ${afterResult.rows[0]?.record_count || 0} records, ${afterResult.rows[0]?.total_bitcoin || 0} BTC`);
      
      // Update daily summary
      await db.execute(sql`
        INSERT INTO bitcoin_daily_summaries 
          (summary_date, miner_model, bitcoin_mined, updated_at)
        VALUES (
          ${TARGET_DATE},
          ${minerModel},
          ${totalBitcoin.toString()},
          NOW()
        )
        ON CONFLICT (summary_date, miner_model) 
        DO UPDATE SET 
          bitcoin_mined = ${totalBitcoin.toString()},
          updated_at = NOW()
      `);
      
      logMessage(`Updated daily summary for ${minerModel}: ${totalBitcoin.toFixed(8)} BTC`);
    }
    
    // Update monthly summaries for April 2025
    logMessage("\n--- Updating Monthly Summaries ---");
    const yearMonth = TARGET_DATE.substring(0, 7); // YYYY-MM
    
    for (const minerModel of MINER_MODELS) {
      // Calculate the monthly total from daily summaries
      const monthlyResult = await db.execute(sql`
        SELECT SUM(bitcoin_mined::numeric) as total
        FROM bitcoin_daily_summaries
        WHERE summary_date >= ${yearMonth + '-01'}
        AND summary_date <= ${yearMonth + '-30'}
        AND miner_model = ${minerModel}
      `);
      
      const totalMonthlyBitcoin = monthlyResult.rows[0]?.total;
      
      if (totalMonthlyBitcoin) {
        await db.execute(sql`
          INSERT INTO bitcoin_monthly_summaries 
            (year_month, miner_model, bitcoin_mined, updated_at)
          VALUES (
            ${yearMonth},
            ${minerModel},
            ${totalMonthlyBitcoin.toString()},
            NOW()
          )
          ON CONFLICT (year_month, miner_model) 
          DO UPDATE SET 
            bitcoin_mined = ${totalMonthlyBitcoin.toString()},
            updated_at = NOW()
        `);
        
        logMessage(`Updated monthly summary for ${minerModel}: ${totalMonthlyBitcoin} BTC`);
      } else {
        logMessage(`No monthly total calculated for ${minerModel} for ${yearMonth}`);
      }
    }
    
    logMessage("\n===== FIX COMPLETED SUCCESSFULLY =====");
    logMessage(`Check the log file for details: ${LOG_FILE}`);
    
    return 0;
  } catch (error) {
    logMessage(`ERROR FIXING BITCOIN CALCULATIONS: ${error}`);
    return 1;
  }
}

// First, test the DynamoDB connection and data retrieval
async function testDynamoDB() {
  try {
    logMessage("Testing DynamoDB connection and difficulty retrieval...");
    const difficultyFromDynamoDB = await getDifficultyData(TARGET_DATE);
    logMessage(`TEST - Retrieved difficulty value from DynamoDB for ${TARGET_DATE}: ${difficultyFromDynamoDB}`);
    logMessage(`TEST - Current database difficulty: 113757508810853`);
    
    // Test a couple of other dates
    const otherDate1 = "2025-03-01";
    const difficultyDate1 = await getDifficultyData(otherDate1);
    logMessage(`TEST - Retrieved difficulty value from DynamoDB for ${otherDate1}: ${difficultyDate1}`);
    
    const otherDate2 = "2025-02-15";
    const difficultyDate2 = await getDifficultyData(otherDate2);
    logMessage(`TEST - Retrieved difficulty value from DynamoDB for ${otherDate2}: ${difficultyDate2}`);
    
    return difficultyFromDynamoDB;
  } catch (error) {
    logMessage(`ERROR TESTING DYNAMODB: ${error}`);
    return null;
  }
}

// Run the test first, then fix if different
testDynamoDB()
  .then(difficultyFromDynamoDB => {
    if (!difficultyFromDynamoDB) {
      logMessage("Unable to retrieve difficulty from DynamoDB, aborting fix.");
      process.exit(1);
    }
    
    // Check if difficulty is different from what's in the database
    if (difficultyFromDynamoDB === 113757508810853) {
      logMessage("DynamoDB difficulty matches the current database value, no fix needed.");
      process.exit(0);
    }
    
    // If different, run the fix
    logMessage(`DynamoDB difficulty (${difficultyFromDynamoDB}) is different from database (113757508810853), proceeding with fix...`);
    
    return main();
  })
  .then(exitCode => {
    process.exit(exitCode);
  })
  .catch(error => {
    console.error("UNHANDLED ERROR:", error);
    process.exit(1);
  });