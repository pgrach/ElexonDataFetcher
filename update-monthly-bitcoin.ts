/**
 * Update Monthly Bitcoin Summaries for April 2025
 * 
 * This script recalculates the monthly Bitcoin summaries for April 2025
 * based on the actual sum of daily values.
 */

import { db } from "./db";
import { eq, and, sql } from "drizzle-orm";

async function updateMonthlyBitcoinSummaries() {
  console.log("Updating monthly Bitcoin summaries for April 2025...");

  // Calculate the updated sum of daily bitcoin totals for April 2025
  const result = await db.execute(sql`
    SELECT 
        miner_model,
        SUM(bitcoin_mined::numeric) as total_bitcoin
    FROM 
        bitcoin_daily_summaries
    WHERE 
        TO_CHAR(summary_date, 'YYYY-MM') = '2025-04'
    GROUP BY 
        miner_model;
  `);

  console.log("Calculated updated Bitcoin totals:", result.rows);

  // Update each miner model
  for (const row of result.rows) {
    const { miner_model, total_bitcoin } = row;
    
    console.log(`Updating ${miner_model} with total: ${total_bitcoin}`);
    
    // Update the monthly summary
    await db.execute(sql`
      UPDATE bitcoin_monthly_summaries
      SET 
        bitcoin_mined = ${total_bitcoin},
        updated_at = NOW()
      WHERE 
        year_month = '2025-04' AND
        miner_model = ${miner_model}
    `);
    
    console.log(`Updated monthly summary for ${miner_model}`);
  }
  
  console.log("Monthly Bitcoin summaries updated successfully!");
}

// Run the function
updateMonthlyBitcoinSummaries()
  .then(() => {
    console.log("Script completed successfully");
    process.exit(0);
  })
  .catch((error) => {
    console.error("Error updating monthly summaries:", error);
    process.exit(1);
  });