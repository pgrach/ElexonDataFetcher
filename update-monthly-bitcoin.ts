/**
 * Update Monthly Bitcoin Summaries for April 2025
 * 
 * This script recalculates monthly Bitcoin summaries for April 2025 
 * to include the updated data for April 13, 2025.
 */

import { db } from "./db";
import { bitcoin_daily_summaries, bitcoin_monthly_summaries } from "./db/schema";
import { eq, and, sql } from "drizzle-orm";

async function updateMonthlyBitcoinSummaries() {
  console.log("Updating monthly Bitcoin summaries for April 2025...");
  
  // Get all miner models we need to process
  const minerModels = await db.select({
      model: bitcoin_daily_summaries.miner_model
    })
    .from(bitcoin_daily_summaries)
    .where(
      sql`to_char(${bitcoin_daily_summaries.summary_date}, 'YYYY-MM') = '2025-04'`
    )
    .groupBy(bitcoin_daily_summaries.miner_model);
  
  console.log(`Found ${minerModels.length} miner models to process`);
  
  // Process each miner model
  for (const { model } of minerModels) {
    console.log(`Processing monthly summary for ${model}...`);
    
    // Calculate total Bitcoin mined for April 2025 for this model
    const result = await db.select({
        total: sql`sum(${bitcoin_daily_summaries.bitcoin_mined})`
      })
      .from(bitcoin_daily_summaries)
      .where(
        and(
          sql`to_char(${bitcoin_daily_summaries.summary_date}, 'YYYY-MM') = '2025-04'`,
          eq(bitcoin_daily_summaries.miner_model, model)
        )
      );
    
    const totalBitcoin = result[0].total;
    console.log(`Total Bitcoin for ${model} in April 2025: ${totalBitcoin}`);
    
    // Update or insert monthly summary
    await db.insert(bitcoin_monthly_summaries)
      .values({
        year_month: '2025-04',
        miner_model: model,
        bitcoin_mined: totalBitcoin,
        created_at: new Date(),
        updated_at: new Date()
      })
      .onConflictDoUpdate({
        target: [
          bitcoin_monthly_summaries.year_month,
          bitcoin_monthly_summaries.miner_model
        ],
        set: {
          bitcoin_mined: totalBitcoin,
          updated_at: new Date()
        }
      });
    
    console.log(`Updated monthly summary for ${model}`);
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