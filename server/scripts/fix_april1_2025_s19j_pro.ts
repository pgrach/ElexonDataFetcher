/**
 * Fix April 1, 2025 Bitcoin Calculations - S19J_PRO only
 * 
 * This script focuses only on the S19J_PRO miner model for faster execution
 */

import { db } from "../../db";
import { sql } from "drizzle-orm";
import { calculateBitcoin } from "../utils/bitcoin";

// Target date for processing
const TARGET_DATE = "2025-04-01";

// Just process S19J_PRO for now
const MINER_MODEL = "S19J_PRO";

// Known difficulty value from database
const DIFFICULTY = 113757508810853;

async function main() {
  try {
    console.log(`\n===== FIXING BITCOIN CALCULATIONS FOR ${TARGET_DATE} (${MINER_MODEL}) =====\n`);

    // First, check current state
    const currentState = await db.execute(sql`
      SELECT COUNT(*) AS count, SUM(bitcoin_mined::numeric) AS total
      FROM historical_bitcoin_calculations
      WHERE settlement_date = ${TARGET_DATE} AND miner_model = ${MINER_MODEL}
    `);
    
    console.log(`Current state: ${currentState.rows[0]?.count || 0} records, ${currentState.rows[0]?.total || 0} BTC`);
    
    // Get all curtailment records for April 1
    const curtailmentResult = await db.execute(sql`
      SELECT settlement_period, farm_id, ABS(volume) as volume
      FROM curtailment_records
      WHERE settlement_date = ${TARGET_DATE}
    `);
    
    const records = curtailmentResult.rows;
    console.log(`Found ${records.length} curtailment records to process`);
    
    // Clear existing calculations for this date and miner model
    const deleteResult = await db.execute(sql`
      DELETE FROM historical_bitcoin_calculations
      WHERE settlement_date = ${TARGET_DATE} AND miner_model = ${MINER_MODEL}
    `);
    
    console.log(`Deleted ${deleteResult.rowCount} existing calculation records`);
    
    // Process records one by one to avoid batch-related issues
    let totalBitcoin = 0;
    let processedCount = 0;
    
    for (const record of records) {
      const mwh = Number(record.volume);
      
      // Skip records with zero or invalid energy
      if (mwh <= 0 || isNaN(mwh)) {
        continue;
      }
      
      // Calculate Bitcoin for this record
      const bitcoinMined = calculateBitcoin(mwh, MINER_MODEL, DIFFICULTY);
      totalBitcoin += bitcoinMined;
      
      try {
        // Insert record
        await db.execute(sql`
          INSERT INTO historical_bitcoin_calculations 
            (settlement_date, settlement_period, farm_id, miner_model, bitcoin_mined, difficulty, calculated_at)
          VALUES (
            ${TARGET_DATE}, 
            ${Number(record.settlement_period)}, 
            ${record.farm_id}, 
            ${MINER_MODEL}, 
            ${bitcoinMined.toString()}, 
            ${DIFFICULTY.toString()},
            NOW()
          )
        `);
        
        processedCount++;
        
        // Log progress occasionally
        if (processedCount % 50 === 0) {
          console.log(`Processed ${processedCount} records...`);
        }
      } catch (err) {
        console.error(`Error processing record (period: ${record.settlement_period}, farm: ${record.farm_id}):`, err);
      }
    }
    
    console.log(`Successfully processed ${processedCount} records`);
    console.log(`Total Bitcoin calculated: ${totalBitcoin.toFixed(8)}`);
    
    // Verify our calculations
    const verifyResult = await db.execute(sql`
      SELECT COUNT(*) AS count, SUM(bitcoin_mined::numeric) AS total
      FROM historical_bitcoin_calculations
      WHERE settlement_date = ${TARGET_DATE} AND miner_model = ${MINER_MODEL}
    `);
    
    console.log(`Verified state: ${verifyResult.rows[0]?.count || 0} records, ${verifyResult.rows[0]?.total || 0} BTC`);
    
    // Update the daily summary
    await db.execute(sql`
      INSERT INTO bitcoin_daily_summaries (summary_date, miner_model, bitcoin_mined, updated_at)
      VALUES (
        ${TARGET_DATE}, 
        ${MINER_MODEL}, 
        (SELECT SUM(bitcoin_mined::numeric)::text FROM historical_bitcoin_calculations 
         WHERE settlement_date = ${TARGET_DATE} AND miner_model = ${MINER_MODEL}), 
        NOW()
      )
      ON CONFLICT (summary_date, miner_model) 
      DO UPDATE SET 
        bitcoin_mined = (SELECT SUM(bitcoin_mined::numeric)::text FROM historical_bitcoin_calculations 
                      WHERE settlement_date = ${TARGET_DATE} AND miner_model = ${MINER_MODEL}),
        updated_at = NOW()
    `);
    
    console.log(`Updated daily summary for ${MINER_MODEL}`);
    
    // Update the monthly summary
    const yearMonth = TARGET_DATE.substring(0, 7); // YYYY-MM
    
    // Calculate monthly total
    const monthlyResult = await db.execute(sql`
      SELECT SUM(bitcoin_mined::numeric) AS total
      FROM historical_bitcoin_calculations
      WHERE settlement_date LIKE ${yearMonth + '-%'} AND miner_model = ${MINER_MODEL}
    `);
    
    const totalMonthlyBitcoin = monthlyResult.rows[0]?.total;
    
    if (totalMonthlyBitcoin) {
      await db.execute(sql`
        INSERT INTO bitcoin_monthly_summaries (year_month, miner_model, bitcoin_mined, updated_at)
        VALUES (${yearMonth}, ${MINER_MODEL}, ${totalMonthlyBitcoin.toString()}, NOW())
        ON CONFLICT (year_month, miner_model) 
        DO UPDATE SET 
          bitcoin_mined = ${totalMonthlyBitcoin.toString()},
          updated_at = NOW()
      `);
      
      console.log(`Updated monthly summary for ${MINER_MODEL}: ${totalMonthlyBitcoin} BTC`);
    }
    
    console.log("\n===== FIX COMPLETED SUCCESSFULLY =====");
    
    return 0;
  } catch (error) {
    console.error("ERROR FIXING BITCOIN CALCULATIONS:", error);
    return 1;
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