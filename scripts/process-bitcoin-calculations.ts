/**
 * Bitcoin Calculations Processing Script
 * 
 * This script processes Bitcoin mining calculations for a specific date
 * using historical difficulty data for future dates.
 * 
 * Run with: npx tsx scripts/process-bitcoin-calculations.ts <date> [miner_model]
 * Example: npx tsx scripts/process-bitcoin-calculations.ts 2025-04-10 S19J_PRO
 */

import { db } from '../db';
import { curtailmentRecords, historicalBitcoinCalculations, bitcoinDailySummaries } from '../db/schema';
import { eq, and, inArray, sql, desc } from 'drizzle-orm';
import { format, parse, subDays } from 'date-fns';

// Define miner models and their specs
const MINER_SPECS = {
  S19J_PRO: { hashrate: 104, power: 3068 },
  S9: { hashrate: 13.5, power: 1323 },
  M20S: { hashrate: 68, power: 3360 }
};

// Get miner model from command line args or use all models
const TARGET_DATE = process.argv[2];
const MINER_MODEL = process.argv[3];
const MINER_MODELS = MINER_MODEL ? [MINER_MODEL] : Object.keys(MINER_SPECS);

if (!TARGET_DATE) {
  console.error('Error: No date provided');
  console.error('Usage: npx tsx scripts/process-bitcoin-calculations.ts <date> [miner_model]');
  console.error('Example: npx tsx scripts/process-bitcoin-calculations.ts 2025-04-10 S19J_PRO');
  process.exit(1);
}

async function getHistoricalDifficulty() {
  // Get the most recent historical difficulty before the target date
  const result = await db.execute(sql`
    SELECT difficulty, date_time FROM historical_difficulty
    WHERE date_time <= ${TARGET_DATE}
    ORDER BY date_time DESC
    LIMIT 1
  `);
  
  if (result.rows.length === 0) {
    // Fallback to a default if no historical difficulty is found
    console.log('No historical difficulty found, using most recent available.');
    const latestResult = await db.execute(sql`
      SELECT difficulty, date_time FROM historical_difficulty
      ORDER BY date_time DESC
      LIMIT 1
    `);
    
    if (latestResult.rows.length === 0) {
      throw new Error('No historical difficulty data available');
    }
    
    return Number(latestResult.rows[0].difficulty);
  }
  
  return Number(result.rows[0].difficulty);
}

async function getHistoricalPrice() {
  // Get the most recent price before the target date
  const result = await db.execute(sql`
    SELECT price_gbp, date_time FROM historical_bitcoin_price
    WHERE date_time <= ${TARGET_DATE}
    ORDER BY date_time DESC
    LIMIT 1
  `);
  
  if (result.rows.length === 0) {
    // Fallback to a default if no historical price is found
    console.log('No historical price found, using most recent available.');
    const latestResult = await db.execute(sql`
      SELECT price_gbp, date_time FROM historical_bitcoin_price
      ORDER BY date_time DESC
      LIMIT 1
    `);
    
    if (latestResult.rows.length === 0) {
      throw new Error('No historical price data available');
    }
    
    return Number(latestResult.rows[0].price_gbp);
  }
  
  return Number(result.rows[0].price_gbp);
}

async function processSettlementPeriods(date: string, minerModel: string, difficulty: number, bitcoinPrice: number) {
  console.log(`Processing Bitcoin calculations for ${date} with ${minerModel}...`);
  
  // Get the miner specs
  const minerSpecs = MINER_SPECS[minerModel as keyof typeof MINER_SPECS];
  if (!minerSpecs) {
    throw new Error(`Invalid miner model: ${minerModel}`);
  }
  
  // Fetch curtailment records for the date
  const records = await db.select()
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date));
  
  if (records.length === 0) {
    console.log(`No curtailment records found for ${date}`);
    return;
  }
  
  // Group records by settlement period
  const periodGroups = records.reduce((acc, record) => {
    const period = record.settlementPeriod;
    if (!acc[period]) {
      acc[period] = [];
    }
    acc[period].push(record);
    return acc;
  }, {} as Record<string, typeof records>);
  
  // Process each settlement period
  const calculations = [];
  
  for (const [period, periodRecords] of Object.entries(periodGroups)) {
    // Calculate total energy for the period
    const totalEnergy = periodRecords.reduce((sum, record) => sum + Math.abs(Number(record.volume)), 0);
    
    // Skip periods with no energy
    if (totalEnergy === 0) {
      continue;
    }
    
    // Calculate Bitcoin mined using the formula:
    // Bitcoin = (Energy in MWh * Hashrate in TH/s) / (Difficulty * 2^32 / (600 * 10^12) * Power in W)
    const hashrate = minerSpecs.hashrate; // TH/s
    const power = minerSpecs.power; // W
    
    // Convert to hashes per joule
    const hashesPerJoule = hashrate * 1e12 / power; // Hashes per joule
    
    // Energy in joules
    const energyJoules = totalEnergy * 3.6e9; // MWh to joules
    
    // Total hashes that can be computed
    const totalHashes = energyJoules * hashesPerJoule;
    
    // Probability of finding a block per hash
    const probabilityPerHash = 1 / (difficulty * Math.pow(2, 32));
    
    // Expected Bitcoin
    const expectedBitcoin = totalHashes * probabilityPerHash * 6.25; // 6.25 BTC per block
    
    // Calculate value in GBP
    const valueGbp = expectedBitcoin * bitcoinPrice;
    
    calculations.push({
      settlementDate: date,
      settlementPeriod: period,
      minerModel: minerModel,
      totalCurtailedEnergy: totalEnergy,
      bitcoinMined: expectedBitcoin,
      valueGbp: valueGbp,
      calculationParams: JSON.stringify({
        difficulty,
        bitcoinPrice,
        hashrate,
        power,
        energyJoules,
        hashesPerJoule,
        totalHashes,
        probabilityPerHash
      })
    });
  }
  
  // Insert calculations
  if (calculations.length > 0) {
    console.log(`Inserting ${calculations.length} Bitcoin calculations for ${date} and ${minerModel}`);
    
    // Delete existing calculations for this date and miner model
    await db.delete(historicalBitcoinCalculations)
      .where(
        and(
          eq(historicalBitcoinCalculations.settlementDate, date),
          eq(historicalBitcoinCalculations.minerModel, minerModel)
        )
      );
    
    // Insert new calculations
    for (const calc of calculations) {
      await db.insert(historicalBitcoinCalculations).values(calc);
    }
    
    // Update daily summary
    const totalBitcoin = calculations.reduce((sum, calc) => sum + calc.bitcoinMined, 0);
    const totalValue = calculations.reduce((sum, calc) => sum + calc.valueGbp, 0);
    
    // Delete existing daily summary if any
    await db.delete(bitcoinDailySummaries)
      .where(
        and(
          eq(bitcoinDailySummaries.summaryDate, date),
          eq(bitcoinDailySummaries.minerModel, minerModel)
        )
      );
    
    // Insert new daily summary
    await db.insert(bitcoinDailySummaries).values({
      summaryDate: date,
      minerModel: minerModel,
      bitcoinMined: totalBitcoin,
      valueGbp: totalValue,
      createdAt: new Date(),
      bitcoinPrice: bitcoinPrice,
      difficulty: difficulty
    });
    
    console.log(`Successfully processed Bitcoin calculations for ${date} and ${minerModel}`);
    console.log(`Total Bitcoin: ${totalBitcoin.toFixed(8)} BTC (£${totalValue.toFixed(2)})`);
    
    return { success: true, bitcoinMined: totalBitcoin, valueGbp: totalValue };
  } else {
    console.log(`No calculations to insert for ${date} and ${minerModel}`);
    return { success: false };
  }
}

async function processAllModels() {
  try {
    console.log(`\n===== Processing Bitcoin Calculations for ${TARGET_DATE} =====`);
    
    // Get historical difficulty and price
    const difficulty = await getHistoricalDifficulty();
    const bitcoinPrice = await getHistoricalPrice();
    
    console.log(`Using historical difficulty: ${difficulty}`);
    console.log(`Using historical price: £${bitcoinPrice.toFixed(2)}`);
    
    const results: Record<string, { success: boolean; bitcoinMined?: number; valueGbp?: number }> = {};
    
    for (const minerModel of MINER_MODELS) {
      try {
        const result = await processSettlementPeriods(TARGET_DATE, minerModel, difficulty, bitcoinPrice);
        results[minerModel] = result;
      } catch (error) {
        console.error(`Error processing ${minerModel}:`, error instanceof Error ? error.message : 'Unknown error');
        results[minerModel] = { success: false };
      }
    }
    
    console.log('\n===== Processing Complete =====');
    console.log('Results:');
    
    for (const [model, result] of Object.entries(results)) {
      if (result.success) {
        console.log(`${model}: ${result.bitcoinMined?.toFixed(8)} BTC (£${result.valueGbp?.toFixed(2)})`);
      } else {
        console.log(`${model}: Failed`);
      }
    }
    
    console.log(`\nCompleted at: ${new Date().toISOString()}`);
    
  } catch (error) {
    console.error('\nError during processing:', error instanceof Error ? error.message : 'Unknown error');
    process.exit(1);
  }
}

// Execute processing
processAllModels().catch(error => {
  console.error('Fatal error:', error instanceof Error ? error.message : 'Unknown error');
  process.exit(1);
});