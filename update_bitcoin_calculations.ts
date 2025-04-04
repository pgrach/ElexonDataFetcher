/**
 * Update Bitcoin Calculations for March 21, 2025
 * 
 * This script adds Bitcoin mining potential calculations for the 
 * curtailment data that exists for March 21, 2025.
 */

import { db } from './db';
import { sql } from 'drizzle-orm';
import fs from 'fs';

// Configuration
const TARGET_DATE = '2025-03-21';
const LOG_FILE = `bitcoin_calculations_${TARGET_DATE}.log`;

// Create a log stream
const logStream = fs.createWriteStream(LOG_FILE, { flags: 'a' });

function log(message: string): void {
  const timestamp = new Date().toISOString();
  const formattedMessage = `${timestamp} ${message}`;
  console.log(formattedMessage);
  logStream.write(formattedMessage + '\n');
}

async function updateBitcoinCalculations(): Promise<void> {
  const startTime = Date.now();
  
  try {
    log(`Starting Bitcoin calculations update for ${TARGET_DATE}`);
    
    // Clear any existing Bitcoin calculations
    await db.execute(sql`
      DELETE FROM historical_bitcoin_calculations
      WHERE settlement_date = ${TARGET_DATE}
    `);
    
    log('Cleared existing Bitcoin calculations');
    
    // Fetch farm energy data
    const farmDataResult = await db.execute(sql`
      SELECT 
        settlement_period::integer as period, 
        farm_id, 
        SUM(ABS(volume::numeric))::numeric as total_energy
      FROM curtailment_records
      WHERE settlement_date = ${TARGET_DATE}
      GROUP BY settlement_period, farm_id
      ORDER BY settlement_period, farm_id
    `);
    
    // Log raw results for debugging
    log(`Query result: ${JSON.stringify(farmDataResult).substring(0, 200)}...`);
    
    if (!farmDataResult || !Array.isArray(farmDataResult) || farmDataResult.length === 0) {
      // Try a different approach with raw access
      log('Using alternative query approach');
      const altResult = await db.execute(sql`
        SELECT COUNT(*) as count FROM curtailment_records WHERE settlement_date = ${TARGET_DATE}
      `);
      log(`Alternative count: ${JSON.stringify(altResult)}`);
      
      if (!altResult || !Array.isArray(altResult) || altResult.length === 0 || altResult[0].count === 0) {
        log(`No farm data found for ${TARGET_DATE}`);
        return;
      }
    }
    
    // Extract the rows
    const farmData = Array.isArray(farmDataResult) ? farmDataResult : [];
    log(`Found data for ${farmData.length} farm periods`);
    
    // Process each miner model
    const minerModels = ['S19J_PRO', 'S9', 'M20S'];
    const difficulty = 113757508810853;
    
    for (const minerModel of minerModels) {
      log(`Processing model: ${minerModel}`);
      let insertCount = 0;
      
      // Process in batches of 50 records
      const BATCH_SIZE = 50;
      for (let i = 0; i < farmData.length; i += BATCH_SIZE) {
        const batch = farmData.slice(i, Math.min(i + BATCH_SIZE, farmData.length));
        
        for (const row of batch) {
          const totalEnergy = parseFloat(row.total_energy);
          let bitcoinMined = 0;
          
          switch (minerModel) {
            case 'S19J_PRO':
              // 100 TH/s at 3250W
              bitcoinMined = totalEnergy * 0.007 * (100000000000000 / difficulty);
              break;
            case 'S9':
              // 13.5 TH/s at 1323W
              bitcoinMined = totalEnergy * 0.0025 * (13500000000000 / difficulty);
              break;
            case 'M20S':
              // 68 TH/s at 3360W
              bitcoinMined = totalEnergy * 0.005 * (68000000000000 / difficulty);
              break;
          }
          
          await db.execute(sql`
            INSERT INTO historical_bitcoin_calculations (
              settlement_date, settlement_period, farm_id, miner_model,
              bitcoin_mined, difficulty, calculated_at
            ) VALUES (
              ${TARGET_DATE}, 
              ${row.period}, 
              ${row.farm_id}, 
              ${minerModel}, 
              ${bitcoinMined}, 
              ${difficulty}, 
              NOW()
            )
          `);
          
          insertCount++;
        }
        
        log(`Processed batch ${Math.floor(i/BATCH_SIZE) + 1} of ${Math.ceil(farmData.length/BATCH_SIZE)} for ${minerModel}`);
      }
      
      // Get total for verification
      const totalResult = await db.execute(sql`
        SELECT 
          COUNT(*) as count,
          ROUND(SUM(bitcoin_mined::numeric)::numeric, 8) as total
        FROM historical_bitcoin_calculations
        WHERE settlement_date = ${TARGET_DATE} AND miner_model = ${minerModel}
      `);
      
      if (totalResult.length) {
        log(`${minerModel}: ${totalResult[0].count} records, ${totalResult[0].total} BTC`);
      }
    }
    
    const endTime = Date.now();
    const duration = ((endTime - startTime) / 1000).toFixed(1);
    
    log(`Bitcoin calculations completed in ${duration} seconds`);
  } catch (error) {
    log(`ERROR: ${error}`);
  } finally {
    logStream.end();
  }
}

// Run the update
updateBitcoinCalculations();