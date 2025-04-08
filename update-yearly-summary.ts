import { db } from './db';
import { bitcoinMonthlySummaries, bitcoinYearlySummaries } from './db/schema';
import { sql, eq, and } from 'drizzle-orm';

async function updateYearlySummary() {
  try {
    console.log('Updating 2025 yearly Bitcoin summaries');
    
    const minerModels = ['S19J_PRO', 'S9', 'M20S', 'M30S++', 'S17', 'S19_PRO'];
    
    for (const minerModel of minerModels) {
      console.log(`\n=== Processing ${minerModel} for year 2025 ===`);
      
      // Get the sum of all monthly summaries for 2025
      const sqlQuery = `
        SELECT SUM(bitcoin_mined::NUMERIC) as total_bitcoin, COUNT(*) as months_count
        FROM bitcoin_monthly_summaries
        WHERE year_month LIKE '2025-%'
        AND miner_model = '${minerModel}'
      `;
      
      console.log(`Executing query for ${minerModel}:`);
      console.log(sqlQuery);
      
      const result = await db.execute(sql.raw(sqlQuery));
      
      const totalBitcoin = result.rows && result.rows[0] ? result.rows[0].total_bitcoin : '0';
      const monthsCount = result.rows && result.rows[0] ? result.rows[0].months_count : 0;
      
      console.log(`Total Bitcoin for 2025 (${minerModel}):`, totalBitcoin);
      console.log(`Months with data: ${monthsCount}`);
      
      if (!totalBitcoin || Number(totalBitcoin) === 0) {
        console.log(`No bitcoin data found for ${minerModel} in 2025, skipping update`);
        continue;
      }
      
      // Get current yearly summary
      const currentSummary = await db
        .select()
        .from(bitcoinYearlySummaries)
        .where(
          and(
            eq(bitcoinYearlySummaries.year, '2025'),
            eq(bitcoinYearlySummaries.minerModel, minerModel)
          )
        );
        
      console.log(`Current yearly summary for ${minerModel}:`, 
        currentSummary[0] ? 
          { bitcoinMined: currentSummary[0].bitcoinMined } : 
          'Not found'
      );
      
      // Update or insert as needed
      if (!currentSummary[0]) {
        await db.execute(sql.raw(`
          INSERT INTO bitcoin_yearly_summaries 
          (year, miner_model, bitcoin_mined, updated_at)
          VALUES (
            '2025',
            '${minerModel}',
            ${totalBitcoin.toString()},
            NOW()
          )
        `));
        console.log(`Created new yearly record for ${minerModel}`);
      } else if (currentSummary[0].bitcoinMined !== totalBitcoin.toString()) {
        await db.execute(sql.raw(`
          UPDATE bitcoin_yearly_summaries
          SET bitcoin_mined = ${totalBitcoin.toString()},
              updated_at = NOW()
          WHERE year = '2025'
          AND miner_model = '${minerModel}'
        `));
        console.log(`Updated yearly record for ${minerModel}`);
      } else {
        console.log(`Yearly summary for ${minerModel} is already correct`);
      }
      
      // Verify the update
      const updatedSummary = await db
        .select()
        .from(bitcoinYearlySummaries)
        .where(
          and(
            eq(bitcoinYearlySummaries.year, '2025'),
            eq(bitcoinYearlySummaries.minerModel, minerModel)
          )
        );
        
      console.log(`Updated yearly summary for ${minerModel}:`, 
        updatedSummary[0] ? 
          { bitcoinMined: updatedSummary[0].bitcoinMined } : 
          'Not found'
      );
    }
    
    console.log('\nAll yearly summaries processed successfully');
  } catch (error) {
    console.error('Error updating yearly summaries:', error);
  }
}

// Run the update
updateYearlySummary();