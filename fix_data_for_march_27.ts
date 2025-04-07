/**
 * Fix Data for March 27, 2025
 * 
 * This script is a simplified version of the data processing pipeline
 * specifically designed to process data for March 27, 2025 without
 * relying on DynamoDB for difficulty data.
 */

import { db } from './db';
import { curtailmentRecords, historicalBitcoinCalculations, 
         bitcoinMonthlySummaries, bitcoinYearlySummaries } from './db/schema';
import { eq, and, sql } from 'drizzle-orm';
import { format, parse } from 'date-fns';
import fs from 'fs/promises';
import path from 'path';
import { fetchBidsOffers } from './server/services/elexon';

// Configuration
const DATE_TO_PROCESS = '2025-03-27';
const DEFAULT_DIFFICULTY = 71e12; // Using fixed difficulty to avoid DynamoDB
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];
const BATCH_SIZE = 4; // Process 4 periods at once
const BATCH_DELAY_MS = 500; // 500ms delay between batches to avoid API rate limits

// Bitcoin mining parameters for each model
const minerConfigs = {
  'S19J_PRO': { hashrate: 100e12, power: 3050 },
  'S9': { hashrate: 13.5e12, power: 1350 },
  'M20S': { hashrate: 68e12, power: 3360 }
};

// BMU mapping cache
let windFarmIds: Set<string> | null = null;

/**
 * Load valid wind farm BMU IDs
 */
async function loadWindFarmIds(): Promise<Set<string>> {
  if (windFarmIds) return windFarmIds;
  
  try {
    // Try multiple potential file locations
    const possiblePaths = [
      path.join('data', 'bmu_mapping.json'),
      path.join('data', 'bmuMapping.json'),
      path.join('server', 'data', 'bmuMapping.json')
    ];
    
    let mappingFile = null;
    let loadedPath = null;
    
    for (const filepath of possiblePaths) {
      console.log(`Trying to load BMU mapping from: ${filepath}`);
      try {
        mappingFile = await fs.readFile(filepath, 'utf-8');
        loadedPath = filepath;
        break;
      } catch (err) {
        console.log(`File not found at ${filepath}`);
      }
    }
    
    if (!mappingFile) {
      throw new Error('BMU mapping file not found in any of the expected locations');
    }
    
    console.log(`Successfully loaded BMU mapping from: ${loadedPath}`);
    const bmuMapping = JSON.parse(mappingFile);
    windFarmIds = new Set(Object.keys(bmuMapping));
    console.log(`Loaded ${windFarmIds.size} wind farm BMU IDs`);
    return windFarmIds;
  } catch (error) {
    console.error('Error loading BMU mapping:', error);
    throw error;
  }
}

/**
 * Calculate Bitcoin mined for a given amount of energy and miner model
 */
function calculateBitcoin(energyMWh: number, minerModel: string, difficulty: number): number {
  const minerConfig = minerConfigs[minerModel];
  if (!minerConfig) {
    throw new Error(`Unknown miner model: ${minerModel}`);
  }
  
  const { hashrate, power } = minerConfig;
  
  // Constants
  const BLOCK_REWARD = 6.25; // BTC per block
  const SECONDS_PER_HOUR = 3600;
  const HOURS_PER_DAY = 24;
  const DAYS_PER_YEAR = 365.25;
  
  // Convert MWh to kWh
  const energyKWh = energyMWh * 1000;
  
  // Calculate how many miners could operate with the energy
  const minerHours = energyKWh / power;
  
  // Calculate hash power in Th/s
  const hashPowerThs = (hashrate / 1e12) * minerHours;
  
  // Calculate expected Bitcoin mined
  const expectedHashesPerBlock = difficulty * 2**32;
  const hashesPerSecond = hashPowerThs * 1e12;
  const secondsToFindBlock = expectedHashesPerBlock / hashesPerSecond;
  const blocksPerHour = SECONDS_PER_HOUR / secondsToFindBlock;
  const hourlyBitcoin = blocksPerHour * BLOCK_REWARD;
  
  // Bitcoin per miner hour
  const bitcoinPerMinerHour = hourlyBitcoin / (hashrate / 1e12);
  
  // Total Bitcoin for the energy
  const totalBitcoin = bitcoinPerMinerHour * minerHours;
  
  return totalBitcoin;
}

/**
 * Process curtailment data for a specific period
 */
async function processPeriod(date: string, period: number): Promise<number> {
  try {
    console.log(`Processing period ${period}...`);
    
    // Get valid wind farm IDs
    const validWindFarmIds = await loadWindFarmIds();
    
    // Get curtailment records from Elexon API
    const apiRecords = await fetchBidsOffers(date, period);
    
    // Filter valid records (negative volume, SO or CADL flagged, valid wind farm)
    const validRecords = apiRecords.filter(record => 
      record.volume < 0 && 
      (record.soFlag || record.cadlFlag) && 
      validWindFarmIds.has(record.id)
    );
    
    if (validRecords.length === 0) {
      console.log(`[${date} P${period}] No valid curtailment records found`);
      return 0;
    }
    
    // Create database records
    const dbRecords = validRecords.map(record => ({
      settlementDate: date,
      settlementPeriod: period,
      farmId: record.id,
      leadPartyName: record.leadPartyName || 'Unknown',
      volume: Math.abs(record.volume),
      payment: Math.abs(record.volume) * record.originalPrice * -1,
      originalPrice: record.originalPrice,
      finalPrice: record.originalPrice, // Using original price as final price
      soFlag: record.soFlag,
      cadlFlag: record.cadlFlag
    }));
    
    // Insert records into database
    for (const record of dbRecords) {
      await db.insert(curtailmentRecords).values(record)
        .onConflictDoUpdate({
          target: [
            curtailmentRecords.settlementDate, 
            curtailmentRecords.settlementPeriod, 
            curtailmentRecords.farmId
          ],
          set: {
            volume: record.volume,
            payment: record.payment,
            originalPrice: record.originalPrice,
            finalPrice: record.finalPrice,
            soFlag: record.soFlag,
            cadlFlag: record.cadlFlag
          }
        });
    }
    
    console.log(`[${date} P${period}] Inserted/updated ${dbRecords.length} records`);
    return dbRecords.length;
  } catch (error) {
    console.error(`Error processing period ${period} for ${date}:`, error);
    return 0;
  }
}

/**
 * Process all periods for a specific date
 */
async function processAllPeriods(date: string): Promise<number> {
  console.log(`\n=== Processing All Periods for ${date} ===\n`);
  
  // Get valid wind farm BMU IDs
  await loadWindFarmIds();
  
  // Clear existing records for this date
  console.log(`Clearing existing records for ${date}...`);
  await db.delete(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date));
  
  // Process periods in batches
  let totalRecords = 0;
  
  for (let startPeriod = 1; startPeriod <= 48; startPeriod += BATCH_SIZE) {
    const endPeriod = Math.min(startPeriod + BATCH_SIZE - 1, 48);
    console.log(`Processing periods ${startPeriod}, ${startPeriod + 1}, ${startPeriod + 2}, ${startPeriod + 3}...`);
    
    // Process batch in parallel
    const periods = [];
    for (let p = startPeriod; p <= endPeriod; p++) {
      periods.push(p);
    }
    
    const results = await Promise.all(
      periods.map(period => processPeriod(date, period))
    );
    
    const batchRecords = results.reduce((sum, count) => sum + count, 0);
    totalRecords += batchRecords;
    
    console.log(`Progress: ${totalRecords}/48 periods processed (${totalRecords} records)`);
    
    // Add delay between batches to avoid API rate limits
    if (startPeriod + BATCH_SIZE <= 48) {
      console.log(`Waiting ${BATCH_DELAY_MS}ms before next batch...`);
      await new Promise(resolve => setTimeout(resolve, BATCH_DELAY_MS));
    }
  }
  
  console.log(`\nFound ${totalRecords} curtailment records for ${date}\n`);
  return totalRecords;
}

/**
 * Process Bitcoin calculations for all miner models
 */
async function processBitcoinCalculations(date: string): Promise<number> {
  console.log(`\n=== Processing Bitcoin Calculations for ${date} ===\n`);
  
  // Use fixed difficulty (no DynamoDB)
  const difficulty = DEFAULT_DIFFICULTY;
  console.log(`Using difficulty: ${difficulty.toLocaleString()}`);
  
  // Clear existing Bitcoin calculations for this date
  await db.delete(historicalBitcoinCalculations)
    .where(eq(historicalBitcoinCalculations.settlementDate, date));
  
  // Get curtailment records for the date
  const records = await db.query.curtailmentRecords.findMany({
    where: eq(curtailmentRecords.settlementDate, date)
  });
  
  if (records.length === 0) {
    console.log(`No curtailment records found for ${date}`);
    return 0;
  }
  
  // Process each miner model
  let totalCalculations = 0;
  
  for (const minerModel of MINER_MODELS) {
    console.log(`\n--- Processing ${minerModel} ---\n`);
    
    // Group records by settlement period and farmId
    const periodRecords = new Map<string, any[]>();
    
    for (const record of records) {
      const key = `${record.settlementPeriod}_${record.farmId}`;
      if (!periodRecords.has(key)) {
        periodRecords.set(key, []);
      }
      periodRecords.get(key)!.push(record);
    }
    
    // Process each period/farmId combination
    for (const [key, groupRecords] of periodRecords.entries()) {
      const [periodStr, farmId] = key.split('_');
      const period = parseInt(periodStr, 10);
      
      // Calculate total energy and payment for this period/farmId
      const totalEnergy = groupRecords.reduce((sum, r) => sum + Number(r.volume), 0);
      const totalPayment = groupRecords.reduce((sum, r) => sum + Number(r.payment), 0);
      
      // Calculate Bitcoin mining potential
      const bitcoinMined = calculateBitcoin(totalEnergy, minerModel, difficulty);
      
      // Insert Bitcoin calculation into database
      await db.insert(historicalBitcoinCalculations).values({
        settlementDate: date,
        settlementPeriod: period,
        farmId,
        minerModel,
        bitcoinMined,
        difficulty
      });
      
      totalCalculations++;
    }
  }
  
  console.log(`\nProcessed ${totalCalculations} Bitcoin calculations for ${date}\n`);
  return totalCalculations;
}

/**
 * Update monthly Bitcoin summaries
 */
async function updateMonthlyBitcoinSummaries(date: string): Promise<void> {
  console.log('\n=== Updating Monthly Bitcoin Summaries ===\n');
  
  const parsedDate = parse(date, 'yyyy-MM-dd', new Date());
  const yearMonth = format(parsedDate, 'yyyy-MM');
  
  // For each miner model, calculate monthly totals
  for (const minerModel of MINER_MODELS) {
    console.log(`Processing monthly summary for ${yearMonth} (${minerModel})...`);
    
    // Get all Bitcoin calculations for this month and miner model
    const monthStart = `${yearMonth}-01`;
    const monthEnd = `${yearMonth}-31`; // This is safe since we're using >= and <
    
    const monthlyData = await db.query.historicalBitcoinCalculations.findMany({
      where: and(
        eq(historicalBitcoinCalculations.minerModel, minerModel),
        sql`${historicalBitcoinCalculations.settlementDate} >= ${monthStart}`,
        sql`${historicalBitcoinCalculations.settlementDate} <= ${monthEnd}`
      )
    });
    
    if (monthlyData.length === 0) {
      console.log(`No Bitcoin calculations found for ${yearMonth} (${minerModel})`);
      continue;
    }
    
    // Calculate totals
    const totalEnergy = monthlyData.reduce((sum, r) => sum + Number(r.curtailedEnergy), 0);
    const totalBitcoin = monthlyData.reduce((sum, r) => sum + Number(r.bitcoinMined), 0);
    const avgDifficulty = monthlyData.reduce((sum, r) => sum + Number(r.difficulty), 0) / monthlyData.length;
    
    // Update or insert monthly summary
    await db.insert(bitcoinMonthlySummaries).values({
      yearMonth,
      minerModel,
      bitcoinMined: totalBitcoin,
      valueAtMining: 0, // Not calculating value in this fix script
      createdAt: new Date(),
      updatedAt: new Date()
    }).onConflictDoUpdate({
      target: [bitcoinMonthlySummaries.yearMonth, bitcoinMonthlySummaries.minerModel],
      set: {
        bitcoinMined: totalBitcoin,
        updatedAt: new Date()
      }
    });
    
    console.log(`Updated monthly summary for ${yearMonth} (${minerModel}): ${totalEnergy.toFixed(2)} MWh, ${totalBitcoin.toFixed(8)} BTC`);
  }
}

/**
 * Update yearly Bitcoin summaries
 */
async function updateYearlyBitcoinSummaries(date: string): Promise<void> {
  console.log('\n=== Updating Yearly Bitcoin Summaries ===\n');
  
  const parsedDate = parse(date, 'yyyy-MM-dd', new Date());
  const year = format(parsedDate, 'yyyy');
  
  // For each miner model, calculate yearly totals from monthly summaries
  for (const minerModel of MINER_MODELS) {
    console.log(`Processing yearly summary for ${year} (${minerModel})...`);
    
    // Get all monthly summaries for this year and miner model
    const yearStart = `${year}-01`;
    const yearEnd = `${year}-12`;
    
    const monthlyData = await db.query.bitcoinMonthlySummaries.findMany({
      where: and(
        eq(bitcoinMonthlySummaries.minerModel, minerModel),
        sql`${bitcoinMonthlySummaries.yearMonth} >= ${yearStart}`,
        sql`${bitcoinMonthlySummaries.yearMonth} <= ${yearEnd}`
      )
    });
    
    if (monthlyData.length === 0) {
      console.log(`No monthly summaries found for ${year} (${minerModel})`);
      continue;
    }
    
    // Calculate totals
    const totalEnergy = monthlyData.reduce((sum, r) => sum + Number(r.curtailedEnergy), 0);
    const totalBitcoin = monthlyData.reduce((sum, r) => sum + Number(r.bitcoinMined), 0);
    const avgDifficulty = monthlyData.reduce((sum, r) => sum + Number(r.avgDifficulty), 0) / monthlyData.length;
    
    // Update or insert yearly summary
    await db.insert(bitcoinYearlySummaries).values({
      year,
      minerModel,
      bitcoinMined: totalBitcoin,
      valueAtMining: 0, // Not calculating value in this fix script
      createdAt: new Date(),
      updatedAt: new Date()
    }).onConflictDoUpdate({
      target: [bitcoinYearlySummaries.year, bitcoinYearlySummaries.minerModel],
      set: {
        bitcoinMined: totalBitcoin,
        updatedAt: new Date()
      }
    });
    
    console.log(`Updated yearly summary for ${year} (${minerModel}): ${totalEnergy.toFixed(2)} MWh, ${totalBitcoin.toFixed(8)} BTC`);
  }
}

/**
 * Main function to process all steps
 */
async function main() {
  try {
    console.log(`\n=== Starting Data Fix for ${DATE_TO_PROCESS} ===\n`);
    
    // Step 1: Process curtailment data
    console.log('\n--- Step 1: Processing Curtailment Records ---\n');
    const curtailmentCount = await processAllPeriods(DATE_TO_PROCESS);
    
    if (curtailmentCount === 0) {
      console.log('No curtailment data found, skipping Bitcoin calculations');
      return;
    }
    
    // Step 2: Process Bitcoin calculations
    console.log('\n--- Step 2: Processing Bitcoin Calculations ---\n');
    const bitcoinCount = await processBitcoinCalculations(DATE_TO_PROCESS);
    
    if (bitcoinCount === 0) {
      console.log('No Bitcoin calculations to process, skipping summaries');
      return;
    }
    
    // Step 3: Update summaries
    console.log('\n--- Step 3: Updating Summary Tables ---\n');
    await updateMonthlyBitcoinSummaries(DATE_TO_PROCESS);
    await updateYearlyBitcoinSummaries(DATE_TO_PROCESS);
    
    console.log('\n=== Processing Complete ===\n');
  } catch (error) {
    console.error('Error processing data:', error);
    process.exit(1);
  }
}

main();