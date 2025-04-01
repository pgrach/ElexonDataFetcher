/**
 * Update Bitcoin Calculations for 2025-03-28
 * 
 * This script updates the Bitcoin calculations for 2025-03-28
 * after adding curtailment data for periods 40-48.
 */

import { processSingleDay } from './server/services/bitcoinService';
import { db } from './db';
import { and, eq, sql, gte } from 'drizzle-orm';
import { curtailmentRecords, historicalBitcoinCalculations } from './db/schema';

// Date to process
const date = '2025-03-28';
const MINER_MODEL_LIST = ['S19J_PRO', 'S9', 'M20S'];

async function updateBitcoinCalculations(): Promise<void> {
  try {
    console.log(`Updating Bitcoin calculations for ${date}...`);
    
    // First, let's check what data we have
    console.log(`\n=== Current Curtailment Records for ${date} ===`);
    const curtailmentStats = await db
      .select({
        recordCount: sql<number>`COUNT(*)`,
        periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
        farmCount: sql<number>`COUNT(DISTINCT farm_id)`,
        totalVolume: sql<string>`SUM(ABS(volume::numeric))::text`,
        totalPayment: sql<string>`SUM(payment::numeric)::text`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date));
      
    console.log(`Records: ${curtailmentStats[0]?.recordCount || 0}`);
    console.log(`Periods: ${curtailmentStats[0]?.periodCount || 0}`);
    console.log(`Farms: ${curtailmentStats[0]?.farmCount || 0}`);
    console.log(`Total Volume: ${Number(curtailmentStats[0]?.totalVolume || 0).toFixed(2)} MWh`);
    console.log(`Total Payment: £${Number(curtailmentStats[0]?.totalPayment || 0).toFixed(2)}`);
    
    // Check for periods >= 40
    const newPeriodStats = await db
      .select({
        recordCount: sql<number>`COUNT(*)`,
        periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
        totalVolume: sql<string>`SUM(ABS(volume::numeric))::text`,
      })
      .from(curtailmentRecords)
      .where(
        and(
          eq(curtailmentRecords.settlementDate, date),
          gte(curtailmentRecords.settlementPeriod, 40)
        )
      );
      
    console.log(`\n=== Recently Added Records (Periods >= 40) ===`);
    console.log(`Records: ${newPeriodStats[0]?.recordCount || 0}`);
    console.log(`Periods: ${newPeriodStats[0]?.periodCount || 0}`);
    console.log(`Total Volume: ${Number(newPeriodStats[0]?.totalVolume || 0).toFixed(2)} MWh`);
    
    // Process each miner model
    console.log(`\n=== Processing Bitcoin Calculations ===`);
    for (const minerModel of MINER_MODEL_LIST) {
      try {
        console.log(`Processing ${minerModel} for ${date}...`);
        await processSingleDay(date, minerModel);
        console.log(`Successfully processed ${minerModel} for ${date}`);
      } catch (error) {
        console.error(`Error processing ${minerModel} for ${date}:`, error);
      }
    }
    
    // Verify the results
    console.log(`\n=== Verification Results ===`);
    for (const minerModel of MINER_MODEL_LIST) {
      const bitcoinStats = await db
        .select({
          recordCount: sql<number>`COUNT(*)`,
          periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
          totalBitcoin: sql<string>`SUM(bitcoin_mined::numeric)::text`
        })
        .from(historicalBitcoinCalculations)
        .where(
          and(
            eq(historicalBitcoinCalculations.settlementDate, date),
            eq(historicalBitcoinCalculations.minerModel, minerModel)
          )
        );
        
      console.log(`${minerModel}:`);
      console.log(`- Records: ${bitcoinStats[0]?.recordCount || 0}`);
      console.log(`- Periods: ${bitcoinStats[0]?.periodCount || 0}`);
      console.log(`- Total Bitcoin: ${Number(bitcoinStats[0]?.totalBitcoin || 0).toFixed(8)} BTC`);
    }
    
    console.log(`\nBitcoin calculations updated successfully`);
  } catch (error) {
    console.error(`Error updating Bitcoin calculations:`, error);
  }
}

// Main function
async function main() {  
  await updateBitcoinCalculations();
  process.exit(0);
}

// Run the script
main().catch(error => {
  console.error('Unhandled error:', error);
  process.exit(1);
});