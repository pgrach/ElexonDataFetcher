/**
 * Script to fix period 41 specifically for S19J_PRO historical bitcoin calculations
 */
import pg from 'pg';
import { minerModels, DEFAULT_DIFFICULTY } from '../types/bitcoin';
import { getDifficultyData } from '../services/dynamodbService';

// Initialize database connection pool
const pool = new pg.Pool({
  connectionString: process.env.DATABASE_URL,
  max: 5,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000
});

// Constants
const BLOCK_REWARD = 3.125;
const SETTLEMENT_PERIOD_MINUTES = 30;
const BLOCKS_PER_SETTLEMENT_PERIOD = 3;

// Date and model to fix
const DATE = '2025-03-05';
const MINER_MODEL = 'S19J_PRO';
const PERIOD_TO_FIX = 41;

// Calculate Bitcoin amount for given curtailed energy
function calculateBitcoinForPeriod(
  curtailedMwh: number,
  minerModel: string,
  difficulty: number
): number {
  const miner = minerModels[minerModel];
  if (!miner) throw new Error(`Invalid miner model: ${minerModel}`);

  const curtailedKwh = curtailedMwh * 1000;
  const minerConsumptionKwh = (miner.power / 1000) * (SETTLEMENT_PERIOD_MINUTES / 60);
  const potentialMiners = Math.floor(curtailedKwh / minerConsumptionKwh);
  const difficultyNum = typeof difficulty === 'string' ? parseFloat(difficulty) : difficulty;
  const hashesPerBlock = difficultyNum * Math.pow(2, 32);
  const networkHashRate = hashesPerBlock / 600;
  const networkHashRateTH = networkHashRate / 1e12;
  const totalHashPower = potentialMiners * miner.hashrate;
  const ourNetworkShare = totalHashPower / networkHashRateTH;
  return Number((ourNetworkShare * BLOCK_REWARD * BLOCKS_PER_SETTLEMENT_PERIOD).toFixed(8));
}

async function fixPeriod41() {
  console.log(`\n=== Starting targeted fix for ${DATE}, model ${MINER_MODEL}, period ${PERIOD_TO_FIX} ===`);
  
  try {
    // Get difficulty for the date
    let difficulty: number;
    try {
      difficulty = await getDifficultyData(DATE);
      console.log(`Using difficulty from database: ${difficulty}`);
    } catch (error) {
      console.warn(`Could not fetch difficulty, using default: ${DEFAULT_DIFFICULTY}`);
      difficulty = DEFAULT_DIFFICULTY;
    }
    
    // First, clear any existing S19J_PRO records for this period/date
    await pool.query(
      `DELETE FROM historical_bitcoin_calculations 
       WHERE settlement_date = $1 
       AND settlement_period = $2 
       AND miner_model = $3`,
      [DATE, PERIOD_TO_FIX, MINER_MODEL]
    );
    
    console.log(`Cleared any existing calculations for period ${PERIOD_TO_FIX}`);
    
    // Get curtailment records for this period
    const { rows: records } = await pool.query(
      'SELECT * FROM curtailment_records WHERE settlement_date = $1 AND settlement_period = $2',
      [DATE, PERIOD_TO_FIX]
    );
    
    if (records.length === 0) {
      console.log(`No curtailment records for period ${PERIOD_TO_FIX}`);
      return;
    }
    
    console.log(`Found ${records.length} curtailment records for period ${PERIOD_TO_FIX}`);
    
    // Group records by farm to calculate per-farm volume
    const farmVolumes = new Map<string, number>();
    let totalVolume = 0;
    
    for (const record of records) {
      const absVolume = Math.abs(Number(record.volume));
      totalVolume += absVolume;
      farmVolumes.set(
        record.farm_id,
        (farmVolumes.get(record.farm_id) || 0) + absVolume
      );
    }
    
    console.log(`Total curtailed energy for period ${PERIOD_TO_FIX}: ${totalVolume.toFixed(2)} MWh`);
    
    // Calculate total Bitcoin for this period
    const periodBitcoin = calculateBitcoinForPeriod(
      totalVolume,
      MINER_MODEL,
      difficulty
    );
    
    // Create values for insertion
    const insertValues = [];
    for (const [farmId, farmVolume] of farmVolumes.entries()) {
      const bitcoinShare = (periodBitcoin * farmVolume) / totalVolume;
      insertValues.push({
        settlement_date: DATE,
        settlement_period: PERIOD_TO_FIX,
        farm_id: farmId,
        miner_model: MINER_MODEL,
        bitcoin_mined: bitcoinShare.toFixed(8),
        difficulty: difficulty.toString(),
        calculated_at: new Date()
      });
    }
    
    // Insert the calculations
    let insertCount = 0;
    for (const value of insertValues) {
      await pool.query(
        `INSERT INTO historical_bitcoin_calculations 
        (settlement_date, settlement_period, farm_id, miner_model, bitcoin_mined, difficulty, calculated_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7)`,
        [
          value.settlement_date,
          value.settlement_period,
          value.farm_id,
          value.miner_model,
          value.bitcoin_mined,
          value.difficulty,
          value.calculated_at
        ]
      );
      insertCount++;
    }
    
    console.log(`Inserted ${insertCount} calculations for period ${PERIOD_TO_FIX}`);
  } catch (error) {
    console.error('Error in fixPeriod41:', error);
  } finally {
    // Close the database connection
    await pool.end();
  }
}

// Run the script
fixPeriod41()
  .then(() => console.log('\nScript completed successfully'))
  .catch(err => console.error('\nScript failed:', err));