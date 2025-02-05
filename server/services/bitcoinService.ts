import { BitcoinCalculation, MinerStats, minerModels, BMUCalculation, DEFAULT_DIFFICULTY } from '../types/bitcoin';
import axios from 'axios';
import { db } from "@db";
import { curtailmentRecords, historicalBitcoinCalculations } from "@db/schema";
import { and, eq, between } from "drizzle-orm";
import { format, parseISO, eachDayOfInterval } from 'date-fns';
import { getDifficultyData } from './dynamodbService';
import pLimit from 'p-limit';

// Bitcoin network constants
const BLOCK_REWARD = 3.125; // Current block reward
const SETTLEMENT_PERIOD_MINUTES = 30; // Each settlement period is 30 minutes
const BLOCKS_PER_SETTLEMENT_PERIOD = 3; // 3 blocks per 30 minutes (1 block every 10 minutes)
const MAX_CONCURRENT_DAYS = 5; // Maximum number of days to process concurrently

function calculateBitcoinForBMU(
  curtailedMwh: number,
  minerModel: string,
  difficulty: number
): number {
  console.log('[Bitcoin Calculation] Starting calculation with parameters:', {
    curtailedMwh,
    minerModel,
    difficulty,
    difficultySource: difficulty === DEFAULT_DIFFICULTY ? 'DEFAULT_DIFFICULTY' : 'Historical'
  });

  const miner = minerModels[minerModel];
  if (!miner) {
    throw new Error(`Invalid miner model: ${minerModel}`);
  }

  // Convert MWh to kWh
  const curtailedKwh = curtailedMwh * 1000;

  // Each miner consumes power in kWh per settlement period
  const minerConsumptionKwh = (miner.power / 1000) * (SETTLEMENT_PERIOD_MINUTES / 60);

  // How many miners can be powered for the settlement period
  const potentialMiners = Math.floor(curtailedKwh / minerConsumptionKwh);

  // Calculate expected hashes to find a block from difficulty
  const hashesPerBlock = difficulty * Math.pow(2, 32);

  // Calculate network hashrate (hashes per second)
  const networkHashRate = hashesPerBlock / 600; // 600 seconds = 10 minutes

  // Convert to TH/s for consistency with miner hashrates
  const networkHashRateTH = networkHashRate / 1e12;

  // Total hash power from our miners in TH/s
  const totalHashPower = potentialMiners * miner.hashrate;

  // Calculate probability of finding blocks
  const ourNetworkShare = totalHashPower / networkHashRateTH;

  // Estimate BTC mined per settlement period
  const bitcoinMined = Number((ourNetworkShare * BLOCK_REWARD * BLOCKS_PER_SETTLEMENT_PERIOD).toFixed(8));

  console.log('[Bitcoin Calculation] Calculation details:', {
    curtailedMwh,
    curtailedKwh,
    minerConsumptionKwh,
    potentialMiners,
    networkHashRateTH,
    totalHashPower,
    ourNetworkShare,
    bitcoinMined,
    usedDifficulty: difficulty,
    minerModel,
    minerHashrate: miner.hashrate,
    minerPower: miner.power
  });

  return bitcoinMined;
}

async function processSingleDay(
  date: string,
  minerModel: string
): Promise<void> {
  console.log(`[Bitcoin Service] Processing date: ${date}`);

  try {
    console.log(`[Bitcoin Service] Fetching difficulty data for date: ${date}`);
    const difficulty = await getDifficultyData(date);
    console.log(`[Bitcoin Service] Retrieved difficulty: ${difficulty}`);

    // Fetch all records for the date
    const records = await db
      .select({
        settlementPeriod: curtailmentRecords.settlementPeriod,
        farmId: curtailmentRecords.farmId,
        volume: curtailmentRecords.volume
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date));

    console.log(`[Bitcoin Service] Retrieved ${records.length} curtailment records for date: ${date}`);

    // Group records by period and farm, ensuring we capture all records
    const periodGroups = records.reduce((groups, record) => {
      const periodFarmKey = `${record.settlementPeriod}-${record.farmId}`;
      if (!groups[periodFarmKey]) {
        groups[periodFarmKey] = {
          settlementPeriod: record.settlementPeriod,
          farmId: record.farmId,
          totalVolume: 0
        };
      }
      groups[periodFarmKey].totalVolume += Math.abs(Number(record.volume));
      return groups;
    }, {} as Record<string, { settlementPeriod: number; farmId: string; totalVolume: number; }>);

    console.log(`[Bitcoin Service] Grouped into ${Object.keys(periodGroups).length} period-farm combinations`);

    // Process each group and store results
    for (const group of Object.values(periodGroups)) {
      const bitcoinMined = calculateBitcoinForBMU(
        group.totalVolume,
        minerModel,
        difficulty
      );

      console.log(`[Bitcoin Service] Calculated for period ${group.settlementPeriod}, farm ${group.farmId}:`, {
        totalVolume: group.totalVolume,
        bitcoinMined,
        difficulty
      });

      // Store calculation with improved error handling
      try {
        await db.insert(historicalBitcoinCalculations).values({
          settlementDate: date,
          settlementPeriod: group.settlementPeriod,
          farmId: group.farmId,
          minerModel,
          bitcoinMined: bitcoinMined.toString(),
          difficulty: difficulty.toString()
        }).onConflictDoUpdate({
          target: [
            historicalBitcoinCalculations.settlementDate,
            historicalBitcoinCalculations.settlementPeriod,
            historicalBitcoinCalculations.farmId,
            historicalBitcoinCalculations.minerModel
          ],
          set: {
            bitcoinMined: bitcoinMined.toString(),
            difficulty: difficulty.toString(),
            calculatedAt: new Date()
          }
        });

        console.log(`[Bitcoin Service] Successfully stored calculation for period ${group.settlementPeriod}, farm ${group.farmId}`);
      } catch (error) {
        console.error(`[Bitcoin Service] Error storing calculation for period ${group.settlementPeriod}, farm ${group.farmId}:`, error);
        throw error;
      }
    }

    console.log(`[Bitcoin Service] Completed processing for date: ${date}`);
  } catch (error) {
    console.error(`[Bitcoin Service] Error processing date ${date}:`, error);
    throw error;
  }
}

async function processHistoricalCalculations(
  startDate: string,
  endDate: string,
  minerModel: string = 'S19J_PRO'
): Promise<void> {
  console.log('Processing historical calculations:', { startDate, endDate, minerModel });

  const dateRange = eachDayOfInterval({
    start: parseISO(startDate),
    end: parseISO(endDate)
  });

  // Create a limit function to control concurrency
  const limit = pLimit(MAX_CONCURRENT_DAYS);

  // Process days in parallel with controlled concurrency
  const processPromises = dateRange.map(date => {
    const formattedDate = format(date, 'yyyy-MM-dd');
    return limit(() => processSingleDay(formattedDate, minerModel));
  });

  try {
    await Promise.all(processPromises);
    console.log('Completed processing all dates');
  } catch (error) {
    console.error('Error during parallel processing:', error);
    throw error;
  }
}

async function fetchFromMinerstat(): Promise<{ difficulty: number; price: number }> {
  try {
    const response = await axios.get('https://api.minerstat.com/v2/coins?list=BTC');
    const { difficulty, price } = response.data[0];

    if (!difficulty || !price) {
      throw new Error('Data not found in minerstat response');
    }

    return { difficulty, price };
  } catch (error) {
    console.error('Error fetching from minerstat:', error);
    throw new Error('Failed to fetch data from minerstat');
  }
}

async function calculateBitcoinMining(
  date: string,
  minerModel: string,
  difficulty: number,
  currentPrice: number,
  leadParty?: string,
  farmId?: string
): Promise<{
  totalBitcoin: number;
  totalValue: number;
  periodCalculations: any[];
}> {
  console.log('[Bitcoin Mining] Starting calculation with parameters:', {
    date,
    minerModel,
    difficulty,
    currentPrice,
    leadParty,
    farmId
  });

  // Build the where clause based on filters
  const whereClause = [eq(curtailmentRecords.settlementDate, date)];

  if (farmId) {
    whereClause.push(eq(curtailmentRecords.farmId, farmId));
  } else if (leadParty) {
    whereClause.push(eq(curtailmentRecords.leadPartyName, leadParty));
  }

  // Fetch records with filters
  const periodRecords = await db
    .select({
      settlementPeriod: curtailmentRecords.settlementPeriod,
      volume: curtailmentRecords.volume,
      farmId: curtailmentRecords.farmId,
      leadPartyName: curtailmentRecords.leadPartyName
    })
    .from(curtailmentRecords)
    .where(and(...whereClause))
    .orderBy(curtailmentRecords.settlementPeriod);

  console.log(`[Bitcoin Mining] Retrieved ${periodRecords.length} records for processing`);

  // Group records by period
  const periodGroups = periodRecords.reduce((groups, record) => {
    const period = Number(record.settlementPeriod);
    if (!groups[period]) {
      groups[period] = [];
    }
    groups[period].push(record);
    return groups;
  }, {} as Record<number, typeof periodRecords>);

  // Calculate for each period
  const periodCalculations: any[] = [];
  let totalBitcoin = 0;
  let totalValue = 0;

  for (const [period, records] of Object.entries(periodGroups)) {
    const bmuCalculations: BMUCalculation[] = [];

    // Calculate for each BMU in the period
    for (const record of records) {
      const curtailedMwh = Math.abs(Number(record.volume));
      const calculation = calculateBitcoinForBMU(
        curtailedMwh,
        minerModel,
        difficulty
      );

      const bmuResult = {
        farmId: record.farmId,
        bitcoinMined: calculation,
        valueAtCurrentPrice: calculation * currentPrice,
        curtailedMwh
      };

      bmuCalculations.push(bmuResult);

      // Only add to totals if it matches our filter criteria
      if ((!farmId || record.farmId === farmId) &&
        (!leadParty || record.leadPartyName === leadParty)) {
        totalBitcoin += calculation;
        totalValue += calculation * currentPrice;
      }
    }

    // Calculate period totals only for matching records
    const matchingBMUs = bmuCalculations.filter(calc =>
      (!farmId || calc.farmId === farmId)
    );

    periodCalculations.push({
      period: Number(period),
      bmuCalculations: matchingBMUs,
      periodTotal: {
        bitcoinMined: matchingBMUs.reduce((sum, calc) => sum + calc.bitcoinMined, 0),
        valueAtCurrentPrice: matchingBMUs.reduce((sum, calc) => sum + calc.valueAtCurrentPrice, 0),
        curtailedMwh: matchingBMUs.reduce((sum, calc) => sum + calc.curtailedMwh, 0)
      }
    });
  }

  console.log('[Bitcoin Mining] Calculation completed:', {
    totalBitcoin,
    totalValue,
    usedDifficulty: difficulty,
    periodCount: periodCalculations.length
  });

  return {
    totalBitcoin,
    totalValue,
    periodCalculations
  };
}

// Single consolidated export statement
export {
  calculateBitcoinForBMU,
  calculateBitcoinMining,
  processHistoricalCalculations,
  fetchFromMinerstat,
  processSingleDay
};