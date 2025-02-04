import { db } from "@db";
import { 
  bitcoinDifficultyHistory,
  settlementPeriodMining,
  dailyMiningPotential,
  monthlyMiningPotential,
  yearlyMiningPotential,
  curtailmentRecords
} from "@db/schema";
import { eq, and, sql, desc } from "drizzle-orm";
import { DynamoDB } from "@aws-sdk/client-dynamodb";
import { format, parse, startOfMonth, endOfMonth, getDaysInMonth } from "date-fns";

interface MinerConfig {
  hashrate: number;  // TH/s
  powerDraw: number; // Watts
}

const MINER_CONFIGS: Record<string, MinerConfig> = {
  'S19J_PRO': {
    hashrate: 104,
    powerDraw: 3068
  },
  'S19_XP': {
    hashrate: 140,
    powerDraw: 3010
  }
};

// Utility function to calculate Bitcoin mined
function calculateBitcoinMined(
  energy: number,
  difficulty: number,
  minerModel: string
): number {
  const miner = MINER_CONFIGS[minerModel];
  if (!miner) throw new Error(`Unknown miner model: ${minerModel}`);

  const duration = (energy * 1000000) / miner.powerDraw; // Duration in seconds
  const hashrate = miner.hashrate * Math.pow(10, 12); // Convert TH/s to H/s
  const bitcoinPerBlock = 6.25;
  const secondsPerBlock = 600;

  const probabilityPerHash = 1 / (difficulty * Math.pow(2, 32));
  const expectedHashes = hashrate * duration;
  const expectedBlocks = expectedHashes * probabilityPerHash;

  return expectedBlocks * bitcoinPerBlock;
}

// Validate environment variables
const requiredEnvVars = {
  AWS_ACCESS_KEY_ID: process.env.AWS_ACCESS_KEY_ID,
  AWS_SECRET_ACCESS_KEY: process.env.AWS_SECRET_ACCESS_KEY,
  AWS_REGION: process.env.AWS_REGION,
  DYNAMODB_TABLE: process.env.DYNAMODB_TABLE
};

for (const [key, value] of Object.entries(requiredEnvVars)) {
  if (!value) {
    throw new Error(`Missing required environment variable: ${key}`);
  }
}

// Initialize DynamoDB client
const dynamoDB = new DynamoDB({
  region: requiredEnvVars.AWS_REGION,
  credentials: {
    accessKeyId: requiredEnvVars.AWS_ACCESS_KEY_ID!,
    secretAccessKey: requiredEnvVars.AWS_SECRET_ACCESS_KEY!
  }
});

async function fetchHistoricalDifficulty(date: string): Promise<{
  difficulty: number;
  price: number;
}> {
  try {
    console.log(`Fetching historical data for ${date} from DynamoDB table: ${requiredEnvVars.DYNAMODB_TABLE}`);

    // First, scan for the ID associated with this date
    const scanParams = {
      TableName: requiredEnvVars.DYNAMODB_TABLE,
      FilterExpression: "#dateAttr = :dateVal",
      ExpressionAttributeNames: {
        "#dateAttr": "date"
      },
      ExpressionAttributeValues: {
        ":dateVal": { S: date }
      }
    };

    const scanResult = await dynamoDB.scan(scanParams);
    console.log(`Scan results for ${date}:`, JSON.stringify(scanResult.Items || [], null, 2));

    if (!scanResult.Items?.length) {
      throw new Error(`No difficulty data found for date: ${date}. This could be because the date is in the future or data hasn't been ingested yet.`);
    }

    const item = scanResult.Items[0];
    console.log(`Found raw data for ${date}:`, JSON.stringify(item, null, 2));

    const difficulty = Number(item.difficulty?.N);
    const price = Number(item.price?.N);

    if (isNaN(difficulty) || isNaN(price)) {
      console.error('Raw DynamoDB item:', JSON.stringify(item, null, 2));
      throw new Error(`Invalid data format for ${date}: difficulty=${difficulty}, price=${price}`);
    }

    console.log(`Found data for ${date}: difficulty=${difficulty}, price=${price}`);
    return { difficulty, price };
  } catch (error) {
    console.error(`Error fetching difficulty data for ${date}:`, error);
    throw error;
  }
}

async function processDayMining(date: string, minerModel: string) {
  console.log(`Processing mining calculations for ${date} with ${minerModel}`);

  try {
    // Get difficulty and price for the day
    const { difficulty, price } = await fetchHistoricalDifficulty(date);

    // Store difficulty history
    await db.insert(bitcoinDifficultyHistory).values({
      timestamp: new Date(date),
      difficulty: difficulty.toString(),
      price: price.toString()
    }).onConflictDoNothing();

    // Process each settlement period
    const records = await db
      .select()
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date));

    console.log(`Processing ${records.length} curtailment records for ${date}`);

    for (const record of records) {
      const curtailedEnergy = Math.abs(Number(record.volume));
      const bitcoinMined = calculateBitcoinMined(curtailedEnergy, difficulty, minerModel);
      const valueAtPrice = bitcoinMined * price;

      await db.insert(settlementPeriodMining).values({
        settlementDate: date,
        settlementPeriod: record.settlementPeriod,
        farmId: record.farmId,
        curtailedEnergy: curtailedEnergy.toString(),
        bitcoinMined: bitcoinMined.toString(),
        valueAtPrice: valueAtPrice.toString(),
        difficulty: difficulty.toString(),
        price: price.toString(),
        minerModel
      }).onConflictDoUpdate({
        target: [
          settlementPeriodMining.settlementDate,
          settlementPeriodMining.settlementPeriod,
          settlementPeriodMining.farmId,
          settlementPeriodMining.minerModel
        ],
        set: {
          curtailedEnergy: curtailedEnergy.toString(),
          bitcoinMined: bitcoinMined.toString(),
          valueAtPrice: valueAtPrice.toString(),
          difficulty: difficulty.toString(),
          price: price.toString()
        }
      });
    }

    // Calculate daily totals per farm
    const dailyTotals = await db
      .select({
        farmId: settlementPeriodMining.farmId,
        totalCurtailedEnergy: sql<string>`SUM(${settlementPeriodMining.curtailedEnergy}::numeric)`,
        totalBitcoinMined: sql<string>`SUM(${settlementPeriodMining.bitcoinMined}::numeric)`,
        averageValue: sql<string>`AVG(${settlementPeriodMining.valueAtPrice}::numeric)`
      })
      .from(settlementPeriodMining)
      .where(and(
        eq(settlementPeriodMining.settlementDate, date),
        eq(settlementPeriodMining.minerModel, minerModel)
      ))
      .groupBy(settlementPeriodMining.farmId);

    // Store daily mining potential
    for (const total of dailyTotals) {
      await db.insert(dailyMiningPotential).values({
        summaryDate: date,
        farmId: total.farmId,
        minerModel,
        totalCurtailedEnergy: total.totalCurtailedEnergy,
        totalBitcoinMined: total.totalBitcoinMined,
        averageValue: total.averageValue,
        updatedAt: new Date()
      }).onConflictDoUpdate({
        target: [
          dailyMiningPotential.summaryDate,
          dailyMiningPotential.farmId,
          dailyMiningPotential.minerModel
        ],
        set: {
          totalCurtailedEnergy: total.totalCurtailedEnergy,
          totalBitcoinMined: total.totalBitcoinMined,
          averageValue: total.averageValue,
          updatedAt: new Date()
        }
      });
    }

    console.log(`Completed processing for ${date}`);
  } catch (error) {
    console.error(`Error processing ${date}:`, error);
    throw error;
  }
}

async function processMonthMining(yearMonth: string, minerModel: string) {
  console.log(`Processing monthly mining calculations for ${yearMonth} with ${minerModel}`);

  const [year, month] = yearMonth.split('-').map(Number);
  const startDate = startOfMonth(new Date(year, month - 1));
  const endDate = endOfMonth(startDate);

  // Calculate monthly totals per farm
  const monthlyTotals = await db
    .select({
      farmId: dailyMiningPotential.farmId,
      totalCurtailedEnergy: sql<string>`SUM(${dailyMiningPotential.totalCurtailedEnergy}::numeric)`,
      totalBitcoinMined: sql<string>`SUM(${dailyMiningPotential.totalBitcoinMined}::numeric)`,
      averageValue: sql<string>`AVG(${dailyMiningPotential.averageValue}::numeric)`
    })
    .from(dailyMiningPotential)
    .where(and(
      sql`${dailyMiningPotential.summaryDate} >= ${format(startDate, 'yyyy-MM-dd')}`,
      sql`${dailyMiningPotential.summaryDate} <= ${format(endDate, 'yyyy-MM-dd')}`,
      eq(dailyMiningPotential.minerModel, minerModel)
    ))
    .groupBy(dailyMiningPotential.farmId);

  // Store monthly mining potential
  for (const total of monthlyTotals) {
    await db.insert(monthlyMiningPotential).values({
      yearMonth,
      farmId: total.farmId,
      minerModel,
      totalCurtailedEnergy: total.totalCurtailedEnergy,
      totalBitcoinMined: total.totalBitcoinMined,
      averageValue: total.averageValue,
      updatedAt: new Date()
    }).onConflictDoUpdate({
      target: [
        monthlyMiningPotential.yearMonth,
        monthlyMiningPotential.farmId,
        monthlyMiningPotential.minerModel
      ],
      set: {
        totalCurtailedEnergy: total.totalCurtailedEnergy,
        totalBitcoinMined: total.totalBitcoinMined,
        averageValue: total.averageValue,
        updatedAt: new Date()
      }
    });
  }

  console.log(`Completed monthly calculations for ${yearMonth}`);
}

async function processYearMining(year: string, minerModel: string) {
  console.log(`Processing yearly mining calculations for ${year} with ${minerModel}`);

  // Calculate yearly totals per farm
  const yearlyTotals = await db
    .select({
      farmId: monthlyMiningPotential.farmId,
      totalCurtailedEnergy: sql<string>`SUM(${monthlyMiningPotential.totalCurtailedEnergy}::numeric)`,
      totalBitcoinMined: sql<string>`SUM(${monthlyMiningPotential.totalBitcoinMined}::numeric)`,
      averageValue: sql<string>`AVG(${monthlyMiningPotential.averageValue}::numeric)`
    })
    .from(monthlyMiningPotential)
    .where(and(
      sql`substring(${monthlyMiningPotential.yearMonth} from 1 for 4) = ${year}`,
      eq(monthlyMiningPotential.minerModel, minerModel)
    ))
    .groupBy(monthlyMiningPotential.farmId);

  // Store yearly mining potential
  for (const total of yearlyTotals) {
    await db.insert(yearlyMiningPotential).values({
      year,
      farmId: total.farmId,
      minerModel,
      totalCurtailedEnergy: total.totalCurtailedEnergy,
      totalBitcoinMined: total.totalBitcoinMined,
      averageValue: total.averageValue,
      updatedAt: new Date()
    }).onConflictDoUpdate({
      target: [
        yearlyMiningPotential.year,
        yearlyMiningPotential.farmId,
        yearlyMiningPotential.minerModel
      ],
      set: {
        totalCurtailedEnergy: total.totalCurtailedEnergy,
        totalBitcoinMined: total.totalBitcoinMined,
        averageValue: total.averageValue,
        updatedAt: new Date()
      }
    });
  }

  console.log(`Completed yearly calculations for ${year}`);
}

async function listTableItems() {
  try {
    console.log(`Scanning table ${requiredEnvVars.DYNAMODB_TABLE} to understand data structure...`);

    const scanParams = {
      TableName: requiredEnvVars.DYNAMODB_TABLE,
      Limit: 5 // Get a few items to understand the structure
    };

    const result = await dynamoDB.scan(scanParams);
    console.log('Table sample data:', JSON.stringify(result.Items || [], null, 2));

    return result.Items;
  } catch (error) {
    console.error('Error scanning table:', error);
    throw error;
  }
}

async function processHistoricalMonth(yearMonth: string, minerModel = 'S19J_PRO') {
  try {
    // First, check table structure
    console.log('\n=== Checking DynamoDB Table Structure ===\n');
    await listTableItems();

    console.log(`\n=== Processing Historical Mining Data for ${yearMonth} ===\n`);

    const [year, month] = yearMonth.split('-').map(Number);
    const daysInMonth = getDaysInMonth(new Date(year, month - 1));

    // Process each day
    for (let day = 1; day <= daysInMonth; day++) {
      const date = format(new Date(year, month - 1, day), 'yyyy-MM-dd');
      await processDayMining(date, minerModel);
    }

    // Process monthly aggregates
    await processMonthMining(yearMonth, minerModel);

    // Process yearly aggregates
    await processYearMining(year.toString(), minerModel);

    console.log(`\n=== Completed Processing for ${yearMonth} ===\n`);
  } catch (error) {
    console.error('Error processing historical mining data:', error);
    throw error;
  }
}

// Command line interface
const args = process.argv.slice(2);
const yearMonth = args[0];
const minerModel = args[1] || 'S19J_PRO';

if (!yearMonth || !yearMonth.match(/^\d{4}-\d{2}$/)) {
  console.error('Please provide arguments in the format: YYYY-MM [minerModel]');
  console.error('Example: npm run calculate-historical 2024-02');
  console.error('Example with miner model: npm run calculate-historical 2024-02 S19_XP');
  process.exit(1);
}

processHistoricalMonth(yearMonth, minerModel);