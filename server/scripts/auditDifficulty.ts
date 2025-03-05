import { format } from 'date-fns';
import axios from 'axios';
import { getDifficultyData } from '../services/dynamodbService';
import { calculateBitcoinForBMU } from '../services/bitcoinService';
import { db } from "@db";
import { curtailmentRecords } from "@db/schema";
import { and, eq, sql } from "drizzle-orm";

const TODAY = format(new Date(), 'yyyy-MM-dd');

async function fetchMinerstatDifficulty(): Promise<{ difficulty: number | null; price: number | null }> {
  try {
    console.log('Fetching from Minerstat API...');
    // Use the correct public endpoint
    const response = await axios.get('https://api.minerstat.com/v2/coins?list=BTC');

    console.log('Raw API response:', JSON.stringify(response.data, null, 2));

    if (Array.isArray(response.data) && response.data.length > 0) {
      const btcData = response.data[0];
      if (btcData && typeof btcData.difficulty === 'number' && typeof btcData.price === 'number') {
        return {
          difficulty: btcData.difficulty,
          price: btcData.price
        };
      } else {
        return { difficulty: null, price: null };
      }
    }

    throw new Error('Invalid response format from Minerstat API');
  } catch (error: any) {
    console.error('Error fetching from Minerstat:', error.message);
    if (error.response) {
      console.error('API Response:', {
        status: error.response.status,
        data: error.response.data
      });
    }
    throw new Error('Failed to fetch current network data');
  }
}

async function formatNumber(num: number | string | null): Promise<string> {
  return new Intl.NumberFormat('en-US', {
    maximumFractionDigits: 8,
    minimumFractionDigits: 0
  }).format(Number(num) || 0); // Handle null or undefined
}

function getDifficultyValue(data: { difficulty: number } | number | null): number | null {
  if (data === null || data === undefined) return null;
  if (typeof data === 'number') return data;
  return data.difficulty;
}


async function auditDifficulty() {
  try {
    console.log('\n=== Difficulty Audit for', TODAY, '===\n');

    // 1. Get current Minerstat difficulty
    console.log('Fetching current network difficulty from Minerstat...');
    console.log('Note: Data refreshes every 5-10 minutes, limited to 12 requests/minute\n');

    let minerstatData;
    try {
      minerstatData = await fetchMinerstatDifficulty();
      console.log('Current network difficulty:', await formatNumber(getDifficultyValue(minerstatData?.difficulty)));
      console.log('Current BTC price: $', await formatNumber(getDifficultyValue(minerstatData?.price)));
    } catch (error) {
      console.error('Failed to fetch Minerstat data. Please try again in 5-10 minutes.');
      console.error('Note: Data refreshes every 5-10 minutes. API is rate limited to 12 requests per minute.');
      process.exit(1);
    }

    // 2. Get our stored/calculated difficulty
    let storedDifficulty;
    try {
      storedDifficulty = await getDifficultyData(TODAY);
      console.log('\nStored difficulty for today:', await formatNumber(getDifficultyValue(storedDifficulty)));
    } catch (error) {
      console.log('No stored difficulty found for today');
      storedDifficulty = null;
    }

    // 3. Get today's curtailment data
    const curtailmentData = await db
      .select({
        totalEnergy: sql<string>`SUM(ABS(volume::numeric))`,
        periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
        farmCount: sql<number>`COUNT(DISTINCT farm_id)`
      })
      .from(curtailmentRecords)
      .where(
        and(
          eq(curtailmentRecords.settlementDate, TODAY),
          sql`ABS(volume::numeric) > 0`
        )
      );

    const totalEnergy = Number(curtailmentData[0]?.totalEnergy) || 0;

    console.log('\nToday\'s Curtailment Stats:');
    console.log('Total Energy:', await formatNumber(totalEnergy), 'MWh');
    console.log('Unique Periods:', curtailmentData[0]?.periodCount);
    console.log('Unique Farms:', curtailmentData[0]?.farmCount);

    // 4. Compare mining potential calculations
    if (totalEnergy > 0) {
      const minerModels = ['S19J_PRO', 'S9', 'M20S'];
      console.log('\nMining Potential Comparison:');

      for (const model of minerModels) {
        console.log(`\n${model}:`);

        if (storedDifficulty) {
          const storedBitcoin = calculateBitcoinForBMU(totalEnergy, model, getDifficultyValue(storedDifficulty));
          console.log(`Using stored difficulty (${await formatNumber(getDifficultyValue(storedDifficulty))}):`);
          console.log('- Bitcoin:', storedBitcoin.toFixed(8));
          console.log('- Value: $', (storedBitcoin * (getDifficultyValue(minerstatData?.price) || 0)).toFixed(2)); //Handle null price
        }

        const currentBitcoin = calculateBitcoinForBMU(totalEnergy, model, getDifficultyValue(minerstatData?.difficulty));
        console.log(`Using current difficulty (${await formatNumber(getDifficultyValue(minerstatData?.difficulty))}):`);
        console.log('- Bitcoin:', currentBitcoin.toFixed(8));
        console.log('- Value: $', (currentBitcoin * (getDifficultyValue(minerstatData?.price) || 0)).toFixed(2)); //Handle null price
      }
    }

    // 5. Report findings
    console.log('\n=== Audit Summary ===');
    if (storedDifficulty) {
      const diffPct = (getDifficultyValue(minerstatData?.difficulty) - getDifficultyValue(storedDifficulty)) / getDifficultyValue(storedDifficulty) * 100;
      console.log('Difficulty delta:', (diffPct || 0).toFixed(2) + '%'); //Handle NaN

      if (isNaN(diffPct) || Math.abs(diffPct) > 1) {
        console.log('WARNING: Current network difficulty differs from stored difficulty by more than 1% or is NaN');
        console.log('Consider updating stored difficulty to current network value');
      } else {
        console.log('√ Stored difficulty is within acceptable range of current network difficulty');
      }
    } else {
      console.log('WARNING: No stored difficulty found for today');
      console.log('Recommendation: Update difficulty data with current network value');
    }

  } catch (error) {
    console.error('Error during difficulty audit:', error);
    process.exit(1);
  }
}

auditDifficulty();