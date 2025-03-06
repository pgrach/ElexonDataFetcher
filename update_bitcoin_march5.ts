import { db } from "./db";
import { curtailmentRecords, historicalBitcoinCalculations } from "./db/schema";
import { eq, and, sql, gte, lte, inArray } from "drizzle-orm";
import { getDifficultyData } from "./server/services/dynamodbService";
import { minerModels, MinerStats } from "./server/types/bitcoin";

// Define the miner models to use for calculations
const MINER_MODELS = ["S19J_PRO", "S9", "M20S"];

// Constants for Bitcoin calculation
const SECONDS_PER_DAY = 86400;
const BITCOIN_BLOCK_REWARD = 6.25; // Current block reward
const TERAHASH_TO_HASH = 1_000_000_000_000;
const WATTS_TO_MWH = 0.000001;
const SETTLEMENT_PERIOD_HOURS = 0.5; // 30 minutes

async function updateBitcoinCalculations() {
  const TARGET_DATE = '2025-03-05';
  // Only process periods 32-48 which were newly added
  const TARGET_PERIODS = Array.from({ length: 17 }, (_, i) => i + 32);
  
  try {
    console.log(`\n=== Updating Bitcoin Calculations for ${TARGET_DATE} (Periods ${TARGET_PERIODS[0]}-${TARGET_PERIODS[TARGET_PERIODS.length-1]}) ===\n`);
    
    // Get difficulty from DynamoDB
    const difficulty = await getDifficultyData(TARGET_DATE);
    console.log(`Using difficulty: ${difficulty.toLocaleString()}`);
    
    let totalCalculationsAdded = 0;
    
    // Process each period
    for (const period of TARGET_PERIODS) {
      console.log(`\nProcessing period ${period}...`);
      
      // Get all curtailment records for this period
      const records = await db
        .select({
          farmId: curtailmentRecords.farmId,
          volume: curtailmentRecords.volume
        })
        .from(curtailmentRecords)
        .where(
          and(
            eq(curtailmentRecords.settlementDate, TARGET_DATE),
            eq(curtailmentRecords.settlementPeriod, period)
          )
        );
        
      console.log(`Found ${records.length} curtailment records for period ${period}`);
      
      if (records.length === 0) {
        console.log(`No records found for period ${period}, skipping`);
        continue;
      }
      
      // Process each miner model
      for (const minerModel of MINER_MODELS) {
        console.log(`\n  Processing ${minerModel}...`);
        
        const minerStats = minerModels[minerModel];
        let periodCalculationsAdded = 0;
        
        // Process each farm
        for (const record of records) {
          const volumeMWh = Math.abs(Number(record.volume));
          
          // Calculate Bitcoin mined
          const bitcoinMined = calculateBitcoin(
            volumeMWh,
            minerStats,
            difficulty,
            SETTLEMENT_PERIOD_HOURS
          );
          
          // Insert calculation record
          await db.insert(historicalBitcoinCalculations).values({
            settlementDate: TARGET_DATE,
            settlementPeriod: period,
            farmId: record.farmId,
            bitcoinMined: bitcoinMined.toString(),
            minerModel: minerModel,
            difficulty: difficulty.toString(),
            calculatedAt: new Date()
          });
          
          periodCalculationsAdded++;
        }
        
        console.log(`  Added ${periodCalculationsAdded} calculations for ${minerModel}`);
        totalCalculationsAdded += periodCalculationsAdded;
      }
    }
    
    console.log(`\n=== Summary ===`);
    console.log(`Total Bitcoin calculations added: ${totalCalculationsAdded}`);
    
    // Verify the calculations were added correctly
    const calculationCounts = await db
      .select({
        periodCount: sql<string>`COUNT(DISTINCT settlement_period)::text`,
        recordCount: sql<string>`COUNT(*)::text`
      })
      .from(historicalBitcoinCalculations)
      .where(eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE));
      
    const periodCount = Number(calculationCounts[0].periodCount);
    const recordCount = Number(calculationCounts[0].recordCount);
    
    console.log(`\n=== Verification ===`);
    console.log(`Total periods with calculations: ${periodCount} / 48`);
    console.log(`Total calculation records: ${recordCount}`);
    console.log(`Completion status: ${periodCount === 48 ? '✅ Complete' : '❌ Incomplete'}`);
    
  } catch (error) {
    console.error('Error updating Bitcoin calculations:', error);
  }
}

/**
 * Calculate bitcoin mined based on curtailed energy
 */
function calculateBitcoin(
  curtailedMWh: number,
  minerStats: MinerStats,
  difficulty: number,
  durationHours: number = 24
): number {
  // Energy calculations
  const energyWh = curtailedMWh * 1000 * 1000; // Convert MWh to Wh
  const minerPowerWh = minerStats.power; // Power consumption in Watts (Wh)
  const minersSupported = energyWh / (minerPowerWh * durationHours);
  
  // Hashrate calculations
  const totalHashrate = minersSupported * minerStats.hashrate * TERAHASH_TO_HASH;
  const networkHashrate = difficulty / 600; // approximation based on 10 min block time
  const networkShare = totalHashrate / networkHashrate;
  
  // Bitcoin calculation - how much could be mined in the time period
  const periodBlocks = (durationHours * 3600) / 600; // blocks in the period (assuming 10 min blocks)
  const bitcoinMined = networkShare * periodBlocks * BITCOIN_BLOCK_REWARD;
  
  return bitcoinMined;
}

updateBitcoinCalculations();