/**
 * Update April 2025 Monthly Summary
 * 
 * This script updates the monthly summaries based on the daily summaries for April 2025.
 */

import { db } from "../../db";
import { sql } from "drizzle-orm";

// Target month for updates
const YEAR_MONTH = "2025-04";

// Miner models to process
const MINER_MODELS = ["S19J_PRO", "S9", "M20S"];

async function main() {
  try {
    console.log(`\n===== UPDATING MONTHLY SUMMARY FOR ${YEAR_MONTH} =====\n`);
    
    // Process each miner model
    for (const minerModel of MINER_MODELS) {
      console.log(`\n--- Processing ${minerModel} ---`);
      
      // Get current monthly summary
      const currentSummary = await db.execute(sql`
        SELECT bitcoin_mined::numeric as bitcoin_mined
        FROM bitcoin_monthly_summaries
        WHERE year_month = ${YEAR_MONTH} AND miner_model = ${minerModel}
      `);
      
      console.log(`Current ${minerModel} monthly summary: ${currentSummary.rows[0]?.bitcoin_mined || 0} BTC`);
      
      // Calculate total from daily summaries
      const dailyTotal = await db.execute(sql`
        SELECT SUM(bitcoin_mined::numeric) as total_bitcoin
        FROM bitcoin_daily_summaries
        WHERE summary_date >= ${YEAR_MONTH + '-01'}
        AND summary_date <= ${YEAR_MONTH + '-30'}
        AND miner_model = ${minerModel}
      `);
      
      const totalBitcoin = dailyTotal.rows[0]?.total_bitcoin;
      
      if (totalBitcoin) {
        console.log(`Calculated total from daily summaries: ${totalBitcoin} BTC`);
        
        // Update the monthly summary
        await db.execute(sql`
          INSERT INTO bitcoin_monthly_summaries
            (year_month, miner_model, bitcoin_mined, updated_at)
          VALUES (
            ${YEAR_MONTH},
            ${minerModel},
            ${totalBitcoin.toString()},
            NOW()
          )
          ON CONFLICT (year_month, miner_model)
          DO UPDATE SET
            bitcoin_mined = ${totalBitcoin.toString()},
            updated_at = NOW()
        `);
        
        console.log(`Updated monthly summary for ${minerModel}: ${totalBitcoin} BTC`);
      } else {
        console.log(`No daily data found for ${minerModel} for ${YEAR_MONTH}`);
      }
    }
    
    // Verify the updates
    console.log("\n--- Verification ---");
    const summary = await db.execute(sql`
      SELECT miner_model, bitcoin_mined::numeric as bitcoin_mined, updated_at
      FROM bitcoin_monthly_summaries
      WHERE year_month = ${YEAR_MONTH}
      ORDER BY miner_model
    `);
    
    console.log("Updated monthly summaries:");
    for (const row of summary.rows) {
      console.log(`${row.miner_model}: ${row.bitcoin_mined} BTC (updated at ${row.updated_at})`);
    }
    
    console.log("\n===== UPDATE COMPLETED =====");
    return 0;
  } catch (error) {
    console.error("ERROR UPDATING MONTHLY SUMMARY:", error);
    return 1;
  }
}

// Run the update
main()
  .then(exitCode => {
    process.exit(exitCode);
  })
  .catch(error => {
    console.error("UNHANDLED ERROR:", error);
    process.exit(1);
  });