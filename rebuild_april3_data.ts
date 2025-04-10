/**
 * Rebuild April 3, 2025 Curtailment Records
 * 
 * This script rebuilds curtailment records for April 3, 2025 based on:
 * 1. The daily summary data we have for that date
 * 2. Distribution patterns from nearby dates
 * 
 * The process:
 * 1. Query the daily summary to get the total curtailed energy for April 3
 * 2. Analyze the distribution pattern of curtailment records from April 2 and April 4
 * 3. Create proportional curtailment records for each wind farm
 * 4. Calculate Bitcoin mining potential based on the rebuilt records
 */

import { db } from './db';
import { curtailmentRecords, dailySummaries, historicalBitcoinCalculations, bitcoinDailySummaries } from './db/schema';
import { eq, and, between, sql } from 'drizzle-orm';
import * as fs from 'fs';

// Configuration
const TARGET_DATE = '2025-04-03';
const LOG_FILE_PATH = `./logs/rebuild_april3_${new Date().toISOString().replace(/:/g, '-')}.log`;
const DEFAULT_DIFFICULTY = 113757508810853; // Default difficulty value
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

// Distribution pattern weights for each period (1-48)
// This represents the pattern of when curtailment typically happens
const PERIOD_WEIGHTS = new Array(48).fill(1);
// Higher weights in morning/evening hours when wind generation and curtailment is often higher
[4, 5, 6, 7, 8, 36, 37, 38, 39, 40, 41, 42].forEach(period => PERIOD_WEIGHTS[period - 1] = 2.5);
[9, 10, 11, 35, 43, 44].forEach(period => PERIOD_WEIGHTS[period - 1] = 1.8);

// Mining efficiency by miner model (TH/s per MW)
const MINING_EFFICIENCIES = {
  'S19J_PRO': 100.0,
  'S9': 13.5,
  'M20S': 68.0
};

// Terahashes per Bitcoin based on difficulty
function terahashesPerBitcoin(difficulty: number): number {
  return difficulty / 100000000;
}

/**
 * Simple logging utility with timestamps
 */
function log(message: string): void {
  const timestamp = new Date().toISOString().substring(11, 19);
  const logMessage = `[${timestamp}] ${message}`;
  console.log(logMessage);
  
  // Append to log file
  fs.appendFileSync(LOG_FILE_PATH, logMessage + '\n');
}

/**
 * Get summary data for the target date
 */
async function getDailySummary(): Promise<any> {
  log(`Getting daily summary for ${TARGET_DATE}...`);
  
  const summary = await db
    .select()
    .from(dailySummaries)
    .where(eq(dailySummaries.summaryDate, TARGET_DATE));
    
  if (summary.length === 0) {
    throw new Error(`No daily summary found for ${TARGET_DATE}`);
  }
  
  log(`Found daily summary: ${parseFloat(summary[0].totalCurtailedEnergy?.toString() || '0').toFixed(2)} MWh, £${parseFloat(summary[0].totalPayment?.toString() || '0').toFixed(2)}`);
  return summary[0];
}

/**
 * Get wind farm distribution from nearby dates
 */
async function getWindFarmDistribution(): Promise<any[]> {
  log(`Getting wind farm distribution from nearby dates...`);
  
  // Get April 2 and April 4 records
  const nearbyRecords = await db
    .select({
      farmId: curtailmentRecords.farmId,
      totalEnergy: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
      totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`,
      recordCount: sql<number>`COUNT(*)`
    })
    .from(curtailmentRecords)
    .where(sql`${curtailmentRecords.settlementDate} = '2025-04-02' OR ${curtailmentRecords.settlementDate} = '2025-04-04'`)
    .groupBy(curtailmentRecords.farmId);
  
  if (nearbyRecords.length === 0) {
    log(`No records found for nearby dates. Using default distribution...`);
    
    // Default farm distribution if no data is available
    return [
      { farmId: 'T_DOREW-2', share: 0.15, avgPaymentRate: -18.5 },
      { farmId: 'T_MOWEO-3', share: 0.20, avgPaymentRate: -24.5 },
      { farmId: 'T_MOWEO-2', share: 0.18, avgPaymentRate: -24.5 },
      { farmId: 'T_GLNKW-1', share: 0.12, avgPaymentRate: -25.0 },
      { farmId: 'E_BABAW-1', share: 0.10, avgPaymentRate: -71.5 },
      { farmId: 'T_CRMLW-1', share: 0.08, avgPaymentRate: -77.0 },
      { farmId: 'E_BETHW-1', share: 0.09, avgPaymentRate: -82.0 },
      { farmId: 'T_BHLAW-1', share: 0.08, avgPaymentRate: -84.5 }
    ];
  }
  
  // Calculate total energy to determine shares
  const totalEnergy = nearbyRecords.reduce((sum, r) => sum + parseFloat(r.totalEnergy || '0'), 0);
  
  // Calculate shares and average payment rates
  const farmDistribution = nearbyRecords.map(record => {
    const energy = parseFloat(record.totalEnergy || '0');
    const payment = parseFloat(record.totalPayment || '0');
    const share = energy / totalEnergy;
    const avgPaymentRate = payment / energy;
    
    return {
      farmId: record.farmId,
      share,
      avgPaymentRate,
      recordCount: record.recordCount
    };
  });
  
  log(`Retrieved distribution for ${farmDistribution.length} wind farms`);
  return farmDistribution;
}

/**
 * Clear existing curtailment records for the target date
 */
async function clearExistingCurtailmentRecords(): Promise<void> {
  log(`Clearing existing curtailment records for ${TARGET_DATE}...`);
  
  const result = await db.delete(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE))
    .returning({
      id: curtailmentRecords.id
    });
  
  log(`Cleared ${result.length} existing curtailment records`);
}

/**
 * Clear existing Bitcoin calculations for the target date
 */
async function clearExistingBitcoinCalculations(): Promise<void> {
  log(`Clearing existing Bitcoin calculations for ${TARGET_DATE}...`);
  
  for (const minerModel of MINER_MODELS) {
    // Clear historical calculations
    const histResult = await db.delete(historicalBitcoinCalculations)
      .where(and(
        eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
        eq(historicalBitcoinCalculations.minerModel, minerModel)
      ))
      .returning({
        id: historicalBitcoinCalculations.id
      });
    
    log(`Cleared ${histResult.length} historical Bitcoin calculations for ${minerModel}`);
    
    // Clear daily summaries
    const summaryResult = await db.delete(bitcoinDailySummaries)
      .where(and(
        eq(bitcoinDailySummaries.summaryDate, TARGET_DATE),
        eq(bitcoinDailySummaries.minerModel, minerModel)
      ))
      .returning({
        id: bitcoinDailySummaries.id
      });
    
    log(`Cleared ${summaryResult.length} Bitcoin daily summaries for ${minerModel}`);
  }
}

/**
 * Create curtailment records based on distribution
 */
async function createCurtailmentRecords(summary: any, distribution: any[]): Promise<void> {
  log(`Creating curtailment records for ${TARGET_DATE}...`);
  
  const totalEnergy = parseFloat(summary.totalCurtailedEnergy?.toString() || '0');
  const totalPayment = parseFloat(summary.totalPayment?.toString() || '0');
  
  const totalPeriodWeight = PERIOD_WEIGHTS.reduce((sum, w) => sum + w, 0);
  
  let recordsCreated = 0;
  let totalEnergyCreated = 0;
  let totalPaymentCreated = 0;
  
  // For each period, distribute energy among farms
  for (let period = 1; period <= 48; period++) {
    const periodWeight = PERIOD_WEIGHTS[period - 1];
    const periodShare = periodWeight / totalPeriodWeight;
    const periodEnergy = totalEnergy * periodShare;
    
    // For each farm, create records
    for (const farm of distribution) {
      if (farm.share < 0.01) continue; // Skip very small contributions
      
      const farmEnergy = periodEnergy * farm.share;
      // Use average payment rate from distribution or calculate from total payment
      const farmPayment = farmEnergy * farm.avgPaymentRate || (totalPayment / totalEnergy) * farmEnergy;
      
      if (farmEnergy < 0.001) continue; // Skip negligible amounts
      
      try {
        await db.insert(curtailmentRecords).values({
          settlementDate: TARGET_DATE,
          settlementPeriod: period,
          volume: farmEnergy.toString(),
          payment: farmPayment.toString(),
          farmId: farm.farmId,
          curtailmentType: 'bid_offer',
          createdBy: 'data_reconstruction_script',
          createdAt: new Date()
        });
        
        totalEnergyCreated += farmEnergy;
        totalPaymentCreated += farmPayment;
        recordsCreated++;
        
        if (recordsCreated % 50 === 0) {
          log(`Created ${recordsCreated} records so far...`);
        }
      } catch (error) {
        log(`Error creating record for ${farm.farmId} in period ${period}: ${(error as Error).message}`);
      }
    }
  }
  
  log(`Created ${recordsCreated} curtailment records`);
  log(`Total energy: ${totalEnergyCreated.toFixed(2)} MWh (target: ${totalEnergy.toFixed(2)} MWh)`);
  log(`Total payment: £${totalPaymentCreated.toFixed(2)} (target: £${totalPayment.toFixed(2)})`);
  
  // Adjust if there's significant difference between created and target
  if (Math.abs(totalEnergyCreated - totalEnergy) > 1.0) {
    log(`⚠️ Warning: Created energy (${totalEnergyCreated.toFixed(2)} MWh) differs from target (${totalEnergy.toFixed(2)} MWh)`);
    
    // Create an adjustment record if needed
    const adjustmentEnergy = totalEnergy - totalEnergyCreated;
    const adjustmentPayment = totalPayment - totalPaymentCreated;
    
    if (Math.abs(adjustmentEnergy) > 0.1) {
      const mainFarm = distribution[0]?.farmId || 'T_DOREW-2';
      
      await db.insert(curtailmentRecords).values({
        settlementDate: TARGET_DATE,
        settlementPeriod: 24, // Middle of the day
        volume: adjustmentEnergy.toString(),
        payment: adjustmentPayment.toString(),
        farmId: mainFarm,
        curtailmentType: 'adjustment',
        createdBy: 'data_reconstruction_script',
        createdAt: new Date()
      });
      
      log(`Created adjustment record: ${adjustmentEnergy.toFixed(2)} MWh, £${adjustmentPayment.toFixed(2)}`);
      recordsCreated++;
    }
  }
  
  log(`Total records created: ${recordsCreated}`);
}

/**
 * Calculate Bitcoin mining potential for a miner model
 */
async function calculateBitcoinPotential(minerModel: string): Promise<void> {
  log(`Calculating Bitcoin potential for ${TARGET_DATE} with model ${minerModel}...`);
  
  // Get all curtailment records for the target date
  const records = await db
    .select()
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
  
  log(`Found ${records.length} curtailment records for Bitcoin calculations`);
  
  if (records.length === 0) {
    throw new Error(`No curtailment records found for ${TARGET_DATE}`);
  }
  
  const miningEfficiency = MINING_EFFICIENCIES[minerModel as keyof typeof MINING_EFFICIENCIES] || 100.0;
  const thPerBitcoin = terahashesPerBitcoin(DEFAULT_DIFFICULTY);
  
  let totalBitcoin = 0;
  let recordsProcessed = 0;
  
  // Process in batches of 50
  const batchSize = 50;
  const batches = Math.ceil(records.length / batchSize);
  
  for (let batchIndex = 0; batchIndex < batches; batchIndex++) {
    const startIdx = batchIndex * batchSize;
    const endIdx = Math.min(startIdx + batchSize, records.length);
    const batchRecords = records.slice(startIdx, endIdx);
    
    for (const record of batchRecords) {
      const energy = parseFloat(record.volume?.toString() || '0');
      
      if (energy <= 0) continue;
      
      // Calculate how many terahashes this energy could produce
      // Energy is in MWh, so we multiply by 0.5 to get the effective hours (assuming 30min periods)
      const terahashes = energy * miningEfficiency * 0.5;
      
      // Calculate Bitcoin that could be mined
      const bitcoin = terahashes / thPerBitcoin;
      
      // Insert historical calculation
      await db.insert(historicalBitcoinCalculations).values({
        calculationDate: TARGET_DATE,
        settlementPeriod: record.settlementPeriod,
        curtailmentRecordId: record.id,
        minerModel: minerModel,
        bitcoinMined: bitcoin.toString(),
        difficulty: DEFAULT_DIFFICULTY.toString(),
        createdAt: new Date()
      });
      
      totalBitcoin += bitcoin;
      recordsProcessed++;
    }
    
    log(`Batch ${batchIndex + 1}/${batches}: Processed ${batchRecords.length} records`);
  }
  
  log(`Processed ${recordsProcessed} Bitcoin calculations for ${minerModel}`);
  log(`Total Bitcoin calculated: ${totalBitcoin.toFixed(8)} BTC`);
  
  // Create daily summary
  await db.insert(bitcoinDailySummaries).values({
    summaryDate: TARGET_DATE,
    minerModel: minerModel,
    bitcoinMined: totalBitcoin.toString(),
    difficulty: DEFAULT_DIFFICULTY.toString(),
    createdAt: new Date()
  });
  
  log(`Updated daily Bitcoin summary for ${minerModel}: ${totalBitcoin.toFixed(8)} BTC`);
}

/**
 * Run the rebuild process
 */
async function runRebuild(): Promise<void> {
  try {
    log(`Starting rebuild process for ${TARGET_DATE}...`);
    
    // Step 1: Get daily summary
    const summary = await getDailySummary();
    
    // Step 2: Get wind farm distribution
    const distribution = await getWindFarmDistribution();
    
    // Step 3: Clear existing records
    await clearExistingCurtailmentRecords();
    await clearExistingBitcoinCalculations();
    
    // Step 4: Create curtailment records
    await createCurtailmentRecords(summary, distribution);
    
    // Step 5: Calculate Bitcoin potential for each miner model
    for (const minerModel of MINER_MODELS) {
      await calculateBitcoinPotential(minerModel);
    }
    
    log(`Rebuild process for ${TARGET_DATE} completed successfully`);
  } catch (error) {
    log(`Error during rebuild process: ${(error as Error).message}`);
    throw error;
  }
}

// Create logs directory if it doesn't exist
if (!fs.existsSync('./logs')) {
  fs.mkdirSync('./logs');
}

// Helper function to handle 'or' condition in drizzle
function or(...conditions: any[]) {
  return sql`(${sql.join(conditions, ') OR (')})`;
}

// Execute the rebuild process
runRebuild()
  .then(() => {
    console.log('\nRebuild completed successfully');
    process.exit(0);
  })
  .catch((error) => {
    console.error('\nRebuild failed:', error);
    process.exit(1);
  });