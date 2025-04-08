import { db } from './db';
import { bitcoinMonthlySummaries } from './db/schema';
import { sql, eq, and } from 'drizzle-orm';

// Also check other miner models
async function fixAllMinerModelsForMarch() {
  const minerModels = ['S19J_PRO', 'S9', 'M20S'];
  
  console.log(`Fixing March 2025 Bitcoin summaries for all miner models: ${minerModels.join(', ')}`);
  
  for (const minerModel of minerModels) {
    console.log(`\n=== Processing ${minerModel} ===`);
    
    // Calculate the correct total based on daily records - use raw SQL for better error handling
    const sqlQuery = `
      SELECT SUM(bitcoin_mined::NUMERIC) as total_bitcoin
      FROM historical_bitcoin_calculations
      WHERE settlement_date >= '2025-03-01'
        AND settlement_date <= '2025-03-31'
        AND miner_model = '${minerModel}'
    `;
    
    console.log(`Executing query for ${minerModel}:`);
    console.log(sqlQuery);
    
    const result = await db.execute(sql.raw(sqlQuery));
    
    console.log(`Result for ${minerModel}:`, result);
    
    // Access the data from rows property
    const totalBitcoin = result.rows && result.rows[0] ? result.rows[0].total_bitcoin : '0';
    console.log(`Correct total Bitcoin for March 2025 (${minerModel}):`, totalBitcoin);
    
    if (!totalBitcoin) {
      console.log(`No bitcoin data found for ${minerModel}, skipping update`);
      continue;
    }
    
    // Get current summary
    const currentSummary = await db
      .select()
      .from(bitcoinMonthlySummaries)
      .where(
        and(
          eq(bitcoinMonthlySummaries.yearMonth, '2025-03'),
          eq(bitcoinMonthlySummaries.minerModel, minerModel)
        )
      );
      
    console.log(`Current summary for ${minerModel}:`, currentSummary[0]?.bitcoinMined || 'Not found');
    
    // Update or insert as needed
    if (!currentSummary[0]) {
      await db.insert(bitcoinMonthlySummaries).values({
        yearMonth: '2025-03',
        minerModel: minerModel,
        bitcoinMined: totalBitcoin.toString(),
        updatedAt: new Date()
      });
      console.log(`Created new record for ${minerModel}`);
    } else if (currentSummary[0].bitcoinMined !== totalBitcoin.toString()) {
      await db
        .update(bitcoinMonthlySummaries)
        .set({
          bitcoinMined: totalBitcoin.toString(),
          updatedAt: new Date()
        })
        .where(
          and(
            eq(bitcoinMonthlySummaries.yearMonth, '2025-03'),
            eq(bitcoinMonthlySummaries.minerModel, minerModel)
          )
        );
      console.log(`Updated record for ${minerModel}`);
    } else {
      console.log(`Summary for ${minerModel} is already correct`);
    }
    
    // Verify the update
    const updatedSummary = await db
      .select()
      .from(bitcoinMonthlySummaries)
      .where(
        and(
          eq(bitcoinMonthlySummaries.yearMonth, '2025-03'),
          eq(bitcoinMonthlySummaries.minerModel, minerModel)
        )
      );
      
    console.log(`Updated summary for ${minerModel}:`, updatedSummary[0]?.bitcoinMined || 'Not found');
  }
  
  console.log('\nAll miner models processed successfully');
}

// Run the fix
fixAllMinerModelsForMarch();