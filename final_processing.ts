/**
 * Final processing script to ensure all Bitcoin calculations are complete
 */

import { processSingleDay } from './server/services/bitcoinService';
import { db } from './db';
import { eq, sql } from 'drizzle-orm';
import { historicalBitcoinCalculations } from './db/schema';

const date = '2025-03-28';
const MINER_MODEL_LIST = ['S19J_PRO', 'S9', 'M20S'];

async function finalProcessing() {
  console.log(`Final processing for ${date}...`);
  
  // Process each miner model
  for (const minerModel of MINER_MODEL_LIST) {
    try {
      console.log(`Processing ${minerModel} for ${date}...`);
      await processSingleDay(date, minerModel);
      console.log(`Successfully processed ${minerModel} for ${date}`);
    } catch (error) {
      console.error(`Error processing ${minerModel} for ${date}:`, error);
    }
  }
  
  // Verify the final state
  for (const minerModel of MINER_MODEL_LIST) {
    const bitcoinStats = await db
      .select({
        recordCount: sql<number>`COUNT(*)`,
        periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
        totalBitcoin: sql<string>`SUM(bitcoin_mined::numeric)::text`
      })
      .from(historicalBitcoinCalculations)
      .where(
        eq(historicalBitcoinCalculations.minerModel, minerModel) &&
        eq(historicalBitcoinCalculations.settlementDate, date)
      );
      
    console.log(`${minerModel}:`);
    console.log(`- Records: ${bitcoinStats[0]?.recordCount || 0}`);
    console.log(`- Periods: ${bitcoinStats[0]?.periodCount || 0}`);
    console.log(`- Total Bitcoin: ${Number(bitcoinStats[0]?.totalBitcoin || 0).toFixed(8)} BTC`);
  }
}

// Run the function
finalProcessing().then(() => {
  console.log('Final processing complete');
  process.exit(0);
}).catch(error => {
  console.error('Error during final processing:', error);
  process.exit(1);
});