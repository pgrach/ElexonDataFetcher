/**
 * Script to fix missing S19J_PRO historical bitcoin calculations for specific date and periods
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
const MISSING_PERIODS = [16, 18, 22, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 41];

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

async function fixMissingPeriods() {
  console.log(`\n=== Starting targeted fix for ${DATE}, model ${MINER_MODEL} ===`);
  console.log(`Fixing periods: ${MISSING_PERIODS.join(', ')}`);
  
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
    
    let totalInsertCount = 0;
    
    // Process each missing period
    for (const period of MISSING_PERIODS) {
      try {
        console.log(`\nProcessing period ${period}...`);
        
        // Get curtailment records for this period
        const { rows: records } = await pool.query(
          'SELECT * FROM curtailment_records WHERE settlement_date = $1 AND settlement_period = $2',
          [DATE, period]
        );
        
        if (records.length === 0) {
          console.log(`No curtailment records for period ${period}`);
          continue;
        }
        
        console.log(`Found ${records.length} curtailment records for period ${period}`);
        
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
        
        console.log(`Total curtailed energy for period ${period}: ${totalVolume.toFixed(2)} MWh`);
        
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
            settlement_period: period,
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
        
        console.log(`Inserted ${insertCount} calculations for period ${period}`);
        totalInsertCount += insertCount;
      } catch (error) {
        console.error(`Error processing period ${period}:`, error);
      }
    }
    
    console.log(`\n=== Completed targeted fix ===`);
    console.log(`Total calculations inserted: ${totalInsertCount}`);
  } catch (error) {
    console.error('Error in fixMissingPeriods:', error);
  } finally {
    // Close the database connection
    await pool.end();
  }
}

// Run the script
fixMissingPeriods()
  .then(() => console.log('\nScript completed successfully'))
  .catch(err => console.error('\nScript failed:', err));