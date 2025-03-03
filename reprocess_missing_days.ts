/**
 * This script reprocesses missing curtailment data for specific days in 2025
 * It focuses on March 1 and March 2, 2025, which were identified as missing.
 */

import { format, subDays } from 'date-fns';
import { exec } from 'child_process';
import { db } from './db/index';
import { curtailmentRecords, historicalBitcoinCalculations } from './db/schema';
import { sql, between } from 'drizzle-orm';

async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function getDatabaseStats(date: string) {
  // Check curtailment records
  const curtailmentStats = await db
    .select({
      count: sql<number>`COUNT(*)`,
      periods: sql<number>`COUNT(DISTINCT settlement_period)`,
      volume: sql<string>`SUM(ABS(volume::numeric))::text`,
      payment: sql<string>`SUM(original_price * ABS(volume))::text`
    })
    .from(curtailmentRecords)
    .where(sql`settlement_date = ${date}`);

  // Check bitcoin calculations
  const bitcoinStats = await db
    .select({
      count: sql<number>`COUNT(*)`,
      models: sql<number>`COUNT(DISTINCT miner_model)`,
      bitcoin: sql<string>`SUM(bitcoin_mined)::text`,
    })
    .from(historicalBitcoinCalculations)
    .where(sql`settlement_date = ${date}`);

  console.log(`Stats for ${date}:`, {
    curtailment: {
      records: curtailmentStats[0].count,
      periods: curtailmentStats[0].periods,
      volume: curtailmentStats[0].volume ? parseFloat(curtailmentStats[0].volume).toFixed(2) : '0.00',
      payment: curtailmentStats[0].payment ? parseFloat(curtailmentStats[0].payment).toFixed(2) : '0.00'
    },
    bitcoin: {
      records: bitcoinStats[0].count,
      models: bitcoinStats[0].models,
      bitcoin: bitcoinStats[0].bitcoin ? parseFloat(bitcoinStats[0].bitcoin).toFixed(6) : '0.000000'
    }
  });

  return {
    curtailmentCount: curtailmentStats[0].count,
    calculationCount: bitcoinStats[0].count
  };
}

async function processMissingDays() {
  console.log("\n=== Reprocessing Missing Days ===\n");
  
  // Get today and yesterday
  const today = new Date();
  const yesterday = subDays(today, 1);
  const todayStr = format(today, 'yyyy-MM-dd');
  const yesterdayStr = format(yesterday, 'yyyy-MM-dd');
  
  try {
    // Check for missing data
    console.log("Checking current data for today and yesterday...");
    const todayStats = await getDatabaseStats(todayStr);
    const yesterdayStats = await getDatabaseStats(yesterdayStr);
    
    // Process missing days
    const missingDays = [];
    
    if (todayStats.curtailmentCount === 0) {
      console.log(`❌ Missing curtailment data for today (${todayStr})`);
      missingDays.push(todayStr);
    }
    
    if (yesterdayStats.curtailmentCount === 0) {
      console.log(`❌ Missing curtailment data for yesterday (${yesterdayStr})`);
      missingDays.push(yesterdayStr);
    }
    
    if (todayStats.curtailmentCount > 0 && todayStats.calculationCount === 0) {
      console.log(`❌ Missing Bitcoin calculations for today (${todayStr})`);
      missingDays.push(todayStr);
    }
    
    if (yesterdayStats.curtailmentCount > 0 && yesterdayStats.calculationCount === 0) {
      console.log(`❌ Missing Bitcoin calculations for yesterday (${yesterdayStr})`);
      missingDays.push(yesterdayStr);
    }
    
    // Process any missing days
    if (missingDays.length > 0) {
      console.log(`\nFound ${missingDays.length} day(s) with missing data. Reprocessing...`);
      
      for (const day of missingDays) {
        console.log(`\nReprocessing ${day}...`);
        await new Promise<void>((resolve, reject) => {
          exec(`npx tsx server/scripts/reprocessDay.ts ${day}`, (error, stdout, stderr) => {
            if (error) {
              console.error(`Error reprocessing ${day}: ${error.message}`);
              reject(error);
              return;
            }
            if (stderr) {
              console.error(`stderr: ${stderr}`);
            }
            console.log(stdout);
            resolve();
          });
        });
        
        console.log(`✅ Reprocessing complete for ${day}`);
        await delay(1000); // Brief delay between processing days
      }
      
      // Verify data was fixed
      console.log("\nVerifying reprocessed data...");
      for (const day of missingDays) {
        const stats = await getDatabaseStats(day);
        if (stats.curtailmentCount > 0 && stats.calculationCount > 0) {
          console.log(`✅ ${day} data is now complete`);
        } else {
          console.log(`❌ ${day} data is still incomplete!`);
        }
      }
    } else {
      console.log("✅ No missing data detected for today or yesterday");
    }
  } catch (error) {
    console.error("Error during processing:", error);
  }
  
  console.log("\n=== Reprocessing Complete ===\n");
}

processMissingDays();