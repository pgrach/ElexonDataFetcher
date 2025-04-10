/**
 * Fix April 1, 2025 Bitcoin Calculations (Upsert version)
 * 
 * This script uses SQL UPSERT (ON CONFLICT DO UPDATE) to handle uniqueness constraints
 */

import { db } from "../../db";
import { sql } from "drizzle-orm";
import { calculateBitcoin } from "../utils/bitcoin";
import fs from 'fs';
import path from 'path';

// Target date for processing
const TARGET_DATE = "2025-04-01";

// Miner models to process
const MINER_MODELS = ["S19J_PRO", "S9", "M20S"];

// Known difficulty value from database
const DIFFICULTY = 113757508810853;

// Optional: Create a log directory and file
const LOG_DIR = path.join(process.cwd(), 'logs');
const LOG_FILE = path.join(LOG_DIR, `fix_april1_2025_${new Date().toISOString().replace(/:/g, '-')}.log`);

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
    logMessage(`\n===== FIXING BITCOIN CALCULATIONS FOR ${TARGET_DATE} =====\n`);

    // Get all curtailment records for this date
    const curtailmentResult = await db.execute(sql`
      SELECT 
        settlement_date,
        settlement_period, 
        farm_id, 
        lead_party_name,
        ABS(volume) as volume
      FROM curtailment_records
      WHERE settlement_date = ${TARGET_DATE}
    `);
    
    const records = curtailmentResult.rows;
    logMessage(`Found ${records.length} curtailment records for processing`);
    
    // Process each miner model
    for (const minerModel of MINER_MODELS) {
      logMessage(`\n--- Processing ${minerModel} ---`);
      
      // First, check how many records we currently have
      const existingResult = await db.execute(sql`
        SELECT COUNT(*) AS count, SUM(bitcoin_mined::numeric) AS total
        FROM historical_bitcoin_calculations
        WHERE settlement_date = ${TARGET_DATE} AND miner_model = ${minerModel}
      `);
      
      logMessage(`Current state for ${minerModel}: ${existingResult.rows[0]?.count || 0} records, ${existingResult.rows[0]?.total || 0} BTC`);
      
      // Process records and calculate Bitcoin for this miner model
      let processedCount = 0;
      let totalBitcoin = 0;
      
      // Since we're processing potentially hundreds of records, let's do it in batches
      const batchSize = 50;
      let batch = [];
      
      for (const record of records) {
        const mwh = Number(record.volume);
        
        // Skip records with zero or invalid energy
        if (mwh <= 0 || isNaN(mwh)) {
          continue;
        }
        
        // Calculate Bitcoin for this record
        const bitcoinMined = calculateBitcoin(mwh, minerModel, DIFFICULTY);
        totalBitcoin += bitcoinMined;
        
        // Add this record to the current batch
        batch.push({
          settlementDate: record.settlement_date,
          settlementPeriod: Number(record.settlement_period),
          farmId: record.farm_id,
          minerModel: minerModel,
          bitcoinMined: bitcoinMined.toString(),
          difficulty: DIFFICULTY.toString()
        });
        
        // When batch is full, process it
        if (batch.length >= batchSize) {
          await processBatch(batch);
          logMessage(`Processed batch of ${batch.length} records`);
          batch = [];
        }
        
        processedCount++;
      }
      
      // Process any remaining records
      if (batch.length > 0) {
        await processBatch(batch);
        logMessage(`Processed final batch of ${batch.length} records`);
      }
      
      logMessage(`Completed processing ${processedCount} records for ${minerModel}`);
      logMessage(`Total Bitcoin calculated: ${totalBitcoin.toFixed(8)}`);
      
      // Verify our calculations
      const verifyResult = await db.execute(sql`
        SELECT COUNT(*) AS count, SUM(bitcoin_mined::numeric) AS total
        FROM historical_bitcoin_calculations
        WHERE settlement_date = ${TARGET_DATE} AND miner_model = ${minerModel}
      `);
      
      logMessage(`Updated state for ${minerModel}: ${verifyResult.rows[0]?.count || 0} records, ${verifyResult.rows[0]?.total || 0} BTC`);
      
      // Update the daily summary for this miner model
      await db.execute(sql`
        INSERT INTO bitcoin_daily_summaries (summary_date, miner_model, bitcoin_mined, updated_at)
        VALUES (${TARGET_DATE}, ${minerModel}, 
          (SELECT SUM(bitcoin_mined::numeric)::text FROM historical_bitcoin_calculations 
           WHERE settlement_date = ${TARGET_DATE} AND miner_model = ${minerModel}), 
          NOW())
        ON CONFLICT (summary_date, miner_model) 
        DO UPDATE SET 
          bitcoin_mined = (SELECT SUM(bitcoin_mined::numeric)::text FROM historical_bitcoin_calculations 
                        WHERE settlement_date = ${TARGET_DATE} AND miner_model = ${minerModel}),
          updated_at = NOW()
      `);
      
      logMessage(`Updated daily summary for ${minerModel}`);
    }
    
    // Update monthly summaries for April 2025
    logMessage("\n--- Updating Monthly Summaries ---");
    const yearMonth = TARGET_DATE.substring(0, 7); // YYYY-MM
    
    for (const minerModel of MINER_MODELS) {
      // Recalculate the monthly totals
      const monthlyResult = await db.execute(sql`
        SELECT SUM(bitcoin_mined::numeric) AS total
        FROM historical_bitcoin_calculations
        WHERE settlement_date LIKE ${yearMonth + '-%'} AND miner_model = ${minerModel}
      `);
      
      const totalMonthlyBitcoin = monthlyResult.rows[0]?.total;
      
      if (totalMonthlyBitcoin) {
        // Update the monthly summary
        await db.execute(sql`
          INSERT INTO bitcoin_monthly_summaries (year_month, miner_model, bitcoin_mined, updated_at)
          VALUES (${yearMonth}, ${minerModel}, ${totalMonthlyBitcoin.toString()}, NOW())
          ON CONFLICT (year_month, miner_model) 
          DO UPDATE SET 
            bitcoin_mined = ${totalMonthlyBitcoin.toString()},
            updated_at = NOW()
        `);
        
        logMessage(`Updated monthly summary for ${minerModel}: ${totalMonthlyBitcoin} BTC`);
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

// Process a batch of records using upsert (INSERT ... ON CONFLICT DO UPDATE)
async function processBatch(records: any[]) {
  for (const record of records) {
    await db.execute(sql`
      INSERT INTO historical_bitcoin_calculations 
        (settlement_date, settlement_period, farm_id, miner_model, bitcoin_mined, difficulty)
      VALUES (
        ${record.settlementDate}, 
        ${record.settlementPeriod}, 
        ${record.farmId}, 
        ${record.minerModel}, 
        ${record.bitcoinMined}, 
        ${record.difficulty}
      )
      ON CONFLICT (settlement_date, settlement_period, farm_id, miner_model) 
      DO UPDATE SET 
        bitcoin_mined = ${record.bitcoinMined},
        difficulty = ${record.difficulty},
        calculated_at = NOW()
    `);
  }
}

// Run the fix
main()
  .then(exitCode => {
    process.exit(exitCode);
  })
  .catch(error => {
    console.error("UNHANDLED ERROR:", error);
    process.exit(1);
  });