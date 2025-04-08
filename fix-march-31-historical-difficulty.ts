import { db } from './db';
import { sql } from 'drizzle-orm';

async function fixMarch31HistoricalDifficulty() {
  try {
    console.log('Checking difficulty for historical calculations on 2025-03-31...');
    
    // Get current difficulty value in the database
    const currentDifficultyQuery = `
      SELECT DISTINCT difficulty
      FROM historical_bitcoin_calculations
      WHERE settlement_date = '2025-03-31'
      LIMIT 1
    `;
    
    const currentDifficultyResult = await db.execute(sql.raw(currentDifficultyQuery));
    const currentDifficulty = currentDifficultyResult.rows[0]?.difficulty;
    
    console.log(`Current difficulty for 2025-03-31: ${currentDifficulty}`);
    
    // The expected difficulty for 2025-03-31
    const expectedDifficulty = '113757508810853';
    
    if (currentDifficulty !== expectedDifficulty) {
      console.log(`Difficulty mismatch - Expected: ${expectedDifficulty}, Found: ${currentDifficulty}`);
      console.log('Updating historical calculations with correct difficulty...');
      
      // Update historical_bitcoin_calculations with correct difficulty
      const updateQuery = `
        UPDATE historical_bitcoin_calculations
        SET difficulty = ${expectedDifficulty}
        WHERE settlement_date = '2025-03-31'
      `;
      
      await db.execute(sql.raw(updateQuery));
      
      console.log('Difficulty updated successfully');
    } else {
      console.log('Difficulty is already correct');
    }
    
    // Verify the update
    const verifyQuery = `
      SELECT DISTINCT difficulty
      FROM historical_bitcoin_calculations
      WHERE settlement_date = '2025-03-31'
      LIMIT 1
    `;
    
    const verifyResult = await db.execute(sql.raw(verifyQuery));
    const updatedDifficulty = verifyResult.rows[0]?.difficulty;
    
    console.log(`Updated difficulty for 2025-03-31: ${updatedDifficulty}`);
    
    if (updatedDifficulty === expectedDifficulty) {
      console.log('Verification successful - difficulty has been updated correctly');
    } else {
      console.log('Verification failed - difficulty is still incorrect');
    }
    
    // Now check the calculations
    const calculationsQuery = `
      SELECT DISTINCT miner_model, 
             COUNT(*) as record_count,
             SUM(bitcoin_mined::numeric) as total_bitcoin
      FROM historical_bitcoin_calculations
      WHERE settlement_date = '2025-03-31'
      GROUP BY miner_model
    `;
    
    const calculationsResult = await db.execute(sql.raw(calculationsQuery));
    
    console.log('\nBitcoin calculations for 2025-03-31:');
    calculationsResult.rows.forEach(row => {
      console.log(`${row.miner_model}: ${row.record_count} records, ${row.total_bitcoin} BTC`);
    });
    
    console.log('\nDone!');
  } catch (error) {
    console.error('Error fixing historical difficulty:', error);
  }
}

fixMarch31HistoricalDifficulty();