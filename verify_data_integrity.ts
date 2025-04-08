import { db } from './db';
import { sql } from 'drizzle-orm';

async function verifyDataIntegrity() {
  try {
    console.log('Verifying data integrity for March 2025 Bitcoin calculations');
    
    const minerModels = ['S19J_PRO', 'S9', 'M20S', 'M30S++', 'S17', 'S19_PRO'];
    
    // 1. Verify sum of daily records matches monthly summary
    for (const model of minerModels) {
      console.log(`\n=== Checking ${model} ===`);
      
      // Get daily totals
      const dailyQuery = `
        SELECT SUM(bitcoin_mined::numeric) as daily_total
        FROM historical_bitcoin_calculations
        WHERE settlement_date >= '2025-03-01' AND settlement_date <= '2025-03-31'
        AND miner_model = '${model}'
      `;
      
      const dailyResult = await db.execute(sql.raw(dailyQuery));
      const dailyTotal = dailyResult.rows[0]?.daily_total || '0';
      
      // Get monthly summary
      const monthlyQuery = `
        SELECT bitcoin_mined
        FROM bitcoin_monthly_summaries
        WHERE year_month = '2025-03'
        AND miner_model = '${model}'
      `;
      
      const monthlyResult = await db.execute(sql.raw(monthlyQuery));
      const monthlyTotal = monthlyResult.rows[0]?.bitcoin_mined || '0';
      
      console.log(`Daily records total: ${dailyTotal}`);
      console.log(`Monthly summary: ${monthlyTotal}`);
      
      // Check if they match (allowing for small precision differences)
      const dailyNum = Number(dailyTotal);
      const monthlyNum = Number(monthlyTotal);
      
      if (Math.abs(dailyNum - monthlyNum) < 0.000001) {
        console.log('✓ MATCH - Daily records total matches monthly summary');
      } else {
        console.log('✗ MISMATCH - Daily records total does not match monthly summary');
        console.log(`  Difference: ${dailyNum - monthlyNum}`);
      }
    }
    
    // 2. Verify sum of monthly records matches yearly summary
    console.log('\n\n=== Checking Yearly Summaries ===');
    
    for (const model of minerModels) {
      console.log(`\n${model}:`);
      
      // Get monthly totals for 2025
      const monthlyQuery = `
        SELECT SUM(bitcoin_mined::numeric) as monthly_total
        FROM bitcoin_monthly_summaries
        WHERE year_month LIKE '2025-%'
        AND miner_model = '${model}'
      `;
      
      const monthlyResult = await db.execute(sql.raw(monthlyQuery));
      const monthlyTotal = monthlyResult.rows[0]?.monthly_total || '0';
      
      // Get yearly summary
      const yearlyQuery = `
        SELECT bitcoin_mined
        FROM bitcoin_yearly_summaries
        WHERE year = '2025'
        AND miner_model = '${model}'
      `;
      
      const yearlyResult = await db.execute(sql.raw(yearlyQuery));
      const yearlyTotal = yearlyResult.rows[0]?.bitcoin_mined || '0';
      
      console.log(`Monthly summaries total: ${monthlyTotal}`);
      console.log(`Yearly summary: ${yearlyTotal}`);
      
      // Check if they match (allowing for small precision differences)
      const monthlyNum = Number(monthlyTotal);
      const yearlyNum = Number(yearlyTotal);
      
      if (Math.abs(monthlyNum - yearlyNum) < 0.000001) {
        console.log('✓ MATCH - Monthly summaries total matches yearly summary');
      } else {
        console.log('✗ MISMATCH - Monthly summaries total does not match yearly summary');
        console.log(`  Difference: ${monthlyNum - yearlyNum}`);
      }
    }
    
    console.log('\n=== Verification Complete ===');
  } catch (error) {
    console.error('Error during verification:', error);
  }
}

verifyDataIntegrity();