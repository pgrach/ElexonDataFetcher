/**
 * Update Bitcoin Calculations for March 21, 2025
 * 
 * This script calculates and updates Bitcoin mining potential based on the
 * curtailment records for March 21, 2025.
 */

import { db } from "./db";
import { curtailmentRecords, dailySummaries, monthlySummaries, yearlySummaries, historicalBitcoinCalculations } from "./db/schema";
import { eq, sql } from "drizzle-orm";

const TARGET_DATE = '2025-03-21';

// Update Bitcoin calculations for the date
async function updateBitcoinCalculations(): Promise<void> {
  console.log(`Updating Bitcoin calculations for ${TARGET_DATE}...`);
  
  try {
    const minerModels = ['S19J_PRO', 'S9', 'M20S'];
    const { processSingleDay } = await import('./server/services/bitcoinService');
    
    for (const minerModel of minerModels) {
      await processSingleDay(TARGET_DATE, minerModel);
      console.log(`- Processed ${minerModel}`);
    }
    
    console.log('Bitcoin calculations updated successfully');

    // Update monthly Bitcoin summaries
    const yearMonth = TARGET_DATE.substring(0, 7);
    console.log(`Updating monthly Bitcoin summary for ${yearMonth}...`);
    
    for (const minerModel of minerModels) {
      console.log(`Calculating monthly Bitcoin summary for ${yearMonth} with ${minerModel}`);
      
      // Calculate the sum of Bitcoin mined for the month
      const monthlySummary = await db
        .select({
          bitcoinMined: sql<string>`SUM(bitcoin_mined::numeric)::text`
        })
        .from(historicalBitcoinCalculations)
        .where(
          sql`date_trunc('month', settlement_date::date) = 
              date_trunc('month', ${yearMonth + '-01'}::date) AND
              miner_model = ${minerModel}`
        );
      
      if (monthlySummary[0] && monthlySummary[0].bitcoinMined) {
        // Update the monthly summary
        await db.execute(
          sql`INSERT INTO monthly_bitcoin_summaries (year_month, miner_model, total_bitcoin_mined, updated_at)
              VALUES (${yearMonth}, ${minerModel}, ${monthlySummary[0].bitcoinMined}, NOW())
              ON CONFLICT (year_month, miner_model) 
              DO UPDATE SET
                total_bitcoin_mined = ${monthlySummary[0].bitcoinMined},
                updated_at = NOW()`
        );
        
        console.log(`Updated monthly summary for ${yearMonth}: ${monthlySummary[0].bitcoinMined} BTC`);
      }
    }
    
    // Update yearly Bitcoin summaries
    const year = TARGET_DATE.substring(0, 4);
    console.log(`Updating yearly Bitcoin summary for ${year}...`);
    console.log('=== Manual Yearly Bitcoin Summary Update ===');
    console.log(`Updating summaries for ${year}`);
    
    for (const minerModel of minerModels) {
      console.log(`- Processing ${minerModel}`);
      console.log(`Calculating yearly Bitcoin summary for ${year} with ${minerModel}`);
      
      // Get all monthly summaries for the year
      const monthlySummaries = await db
        .select({
          totalBitcoin: sql<string>`SUM(total_bitcoin_mined::numeric)::text`
        })
        .from(sql`monthly_bitcoin_summaries`)
        .where(
          sql`year_month LIKE ${year + '-%'} AND miner_model = ${minerModel}`
        );
      
      // Log how many monthly summaries we found
      const countMonthly = await db
        .select({
          count: sql<number>`COUNT(*)`
        })
        .from(sql`monthly_bitcoin_summaries`)
        .where(
          sql`year_month LIKE ${year + '-%'} AND miner_model = ${minerModel}`
        );
      
      console.log(`Found ${countMonthly[0].count} monthly summaries for ${year}`);
      
      if (monthlySummaries[0] && monthlySummaries[0].totalBitcoin) {
        // Update the yearly summary
        await db.execute(
          sql`INSERT INTO yearly_bitcoin_summaries (year, miner_model, total_bitcoin_mined, updated_at)
              VALUES (${year}, ${minerModel}, ${monthlySummaries[0].totalBitcoin}, NOW())
              ON CONFLICT (year, miner_model) 
              DO UPDATE SET
                total_bitcoin_mined = ${monthlySummaries[0].totalBitcoin},
                updated_at = NOW()`
        );
        
        console.log(`Updated yearly summary for ${year}: ${monthlySummaries[0].totalBitcoin} BTC with ${minerModel}`);
      }
    }
    
    // Verify final results
    const verification = await db
      .select({
        miner: sql<string>`miner_model`,
        bitcoin: sql<string>`total_bitcoin_mined`
      })
      .from(sql`yearly_bitcoin_summaries`)
      .where(sql`year = ${year}`);
    
    console.log('Verification Results for ' + year + ':');
    verification.forEach(v => {
      console.log(`- ${v.miner}: ${v.bitcoin} BTC`);
    });
    
    console.log('=== Yearly Summary Update Complete ===');
  } catch (error) {
    console.error('Error updating Bitcoin calculations:', error);
    throw error;
  }
}

async function main(): Promise<void> {
  try {
    console.log('Starting Bitcoin calculation updates...');
    
    // Update Bitcoin calculations
    await updateBitcoinCalculations();
    
    console.log('Bitcoin calculation update completed successfully');
    process.exit(0);
  } catch (error) {
    console.error('Error during Bitcoin calculation update:', error);
    process.exit(1);
  }
}

main().catch(error => {
  console.error('Unhandled error:', error);
  process.exit(1);
});