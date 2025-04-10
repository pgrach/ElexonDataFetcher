/**
 * Fix April 1, 2025 Bitcoin Calculations (Direct SQL version)
 * 
 * This script uses direct SQL for better control over the insert operations
 */

import { db } from "../../db";
import { sql } from "drizzle-orm";
import { calculateBitcoin } from "../utils/bitcoin";

// Target date for processing
const TARGET_DATE = "2025-04-01";

// Miner models to process
const MINER_MODELS = ["S19J_PRO", "S9", "M20S"];

// Known difficulty value from database
const DIFFICULTY = 113757508810853;

async function main() {
  try {
    console.log(`\n===== FIXING BITCOIN CALCULATIONS FOR ${TARGET_DATE} =====\n`);

    // Clear any existing calculations for this date (all miner models)
    const deleteResult = await db.execute(sql`
      DELETE FROM historical_bitcoin_calculations 
      WHERE settlement_date = ${TARGET_DATE}
    `);
    
    console.log(`Deleted ${deleteResult.rowCount} existing calculation records`);

    // Get curtailment records for this date
    const curtailmentResult = await db.execute(sql`
      SELECT 
        settlement_period,
        farm_id,
        lead_party_name,
        ABS(volume) as volume,
        COUNT(*) as record_count,
        SUM(ABS(volume)) as total_energy 
      FROM curtailment_records
      WHERE settlement_date = ${TARGET_DATE}
      GROUP BY settlement_period, farm_id, lead_party_name, volume
    `);
    
    const records = curtailmentResult.rows;
    console.log(`Found ${records.length} unique curtailment records`);
    
    // Process each miner model
    for (const minerModel of MINER_MODELS) {
      console.log(`\n--- Processing ${minerModel} ---`);
      
      let totalBitcoin = 0;
      let processedCount = 0;
      
      // Process each record and calculate Bitcoin
      for (const record of records) {
        const mwh = Number(record.volume);
        
        if (mwh <= 0 || isNaN(mwh)) {
          continue;
        }
        
        const bitcoinMined = calculateBitcoin(mwh, minerModel, DIFFICULTY);
        totalBitcoin += bitcoinMined;
        
        // Insert directly using SQL to avoid TypeScript typing issues
        await db.execute(sql`
          INSERT INTO historical_bitcoin_calculations 
          (settlement_date, settlement_period, farm_id, miner_model, bitcoin_mined, difficulty)
          VALUES (
            ${TARGET_DATE},
            ${Number(record.settlement_period)},
            ${record.farm_id},
            ${minerModel},
            ${bitcoinMined.toString()},
            ${DIFFICULTY.toString()}
          )
        `);
        
        processedCount++;
        
        // Log progress occasionally
        if (processedCount % 50 === 0) {
          console.log(`Processed ${processedCount} records...`);
        }
      }
      
      console.log(`Completed ${processedCount} records for ${minerModel}`);
      console.log(`Total Bitcoin calculated: ${totalBitcoin.toFixed(8)}`);
      
      // Update daily summaries
      await db.execute(sql`
        DELETE FROM bitcoin_daily_summaries
        WHERE summary_date = ${TARGET_DATE} AND miner_model = ${minerModel}
      `);
      
      await db.execute(sql`
        INSERT INTO bitcoin_daily_summaries
        (summary_date, miner_model, bitcoin_mined, updated_at)
        VALUES (
          ${TARGET_DATE},
          ${minerModel},
          ${totalBitcoin.toString()},
          NOW()
        )
      `);
      
      console.log(`Updated daily summary for ${minerModel}`);
    }
    
    // Update monthly summaries for April 2025
    console.log("\n--- Updating Monthly Summaries ---");
    const yearMonth = TARGET_DATE.substring(0, 7); // YYYY-MM
    
    for (const minerModel of MINER_MODELS) {
      // Calculate total Bitcoin for the month
      const monthResult = await db.execute(sql`
        SELECT SUM(bitcoin_mined::numeric) as total_bitcoin
        FROM historical_bitcoin_calculations
        WHERE settlement_date >= ${yearMonth + '-01'}
        AND settlement_date <= ${yearMonth + '-30'}
        AND miner_model = ${minerModel}
      `);
      
      const totalMonthlyBitcoin = monthResult.rows[0]?.total_bitcoin;
      
      if (totalMonthlyBitcoin) {
        // Update monthly summaries
        await db.execute(sql`
          DELETE FROM bitcoin_monthly_summaries
          WHERE year_month = ${yearMonth} AND miner_model = ${minerModel}
        `);
        
        await db.execute(sql`
          INSERT INTO bitcoin_monthly_summaries
          (year_month, miner_model, bitcoin_mined, updated_at)
          VALUES (
            ${yearMonth},
            ${minerModel},
            ${totalMonthlyBitcoin.toString()},
            NOW()
          )
        `);
        
        console.log(`Updated monthly summary for ${minerModel}: ${totalMonthlyBitcoin} BTC`);
      }
    }
    
    console.log("\n===== FIX COMPLETED =====");
    console.log(`All Bitcoin calculations for ${TARGET_DATE} have been successfully updated`);
    
    process.exit(0);
  } catch (error) {
    console.error("ERROR FIXING BITCOIN CALCULATIONS:", error);
    process.exit(1);
  }
}

// Run the fix
main()
  .catch(error => {
    console.error("UNHANDLED ERROR:", error);
    process.exit(1);
  });