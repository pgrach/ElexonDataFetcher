/**
 * Optimized Bitcoin Calculation Processor
 * 
 * This script processes Bitcoin mining calculations for a specific date
 * for all miner models in a single run, fetching the difficulty data only once.
 */

import { db } from './db';
import { curtailmentRecords, historicalBitcoinCalculations, bitcoinMonthlySummaries, bitcoinYearlySummaries } from './db/schema';
import { eq, and, sql } from 'drizzle-orm';
import { format } from 'date-fns';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, ScanCommand } from '@aws-sdk/lib-dynamodb';

// Configuration
const DIFFICULTY_TABLE = 'asics-dynamodb-DifficultyTable-DQ308ID3POT6';
const DEFAULT_DIFFICULTY = 71e12; // Default difficulty if none found
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

// Bitcoin mining parameters for each model
const minerConfigs = {
  'S19J_PRO': { hashrate: 100e12, power: 3050 },
  'S9': { hashrate: 13.5e12, power: 1350 },
  'M20S': { hashrate: 68e12, power: 3360 }
};

// Set up DynamoDB client
const client = new DynamoDBClient({
  region: 'eu-north-1'
});
const docClient = DynamoDBDocumentClient.from(client);

/**
 * Format a date string for DynamoDB difficulty lookup
 */
function formatDateForDifficulty(date: string): string {
  return date; // Assume date is already in YYYY-MM-DD format
}

/**
 * Retry an operation with exponential backoff
 */
async function retryOperation<T>(operation: () => Promise<T>, maxRetries = 3): Promise<T> {
  let lastError;
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await operation();
    } catch (error) {
      console.error(`Operation failed (attempt ${attempt}/${maxRetries}):`, error);
      lastError = error;
      
      if (attempt < maxRetries) {
        const delay = Math.min(100 * Math.pow(2, attempt), 3000);
        await new Promise(resolve => setTimeout(resolve, delay));
      }
    }
  }
  throw lastError;
}

/**
 * Check if DynamoDB table exists
 */
async function verifyTableExists(tableName: string): Promise<boolean> {
  try {
    const response = await docClient.send({
      TableName: tableName,
    } as any);
    
    console.info('[DynamoDB] Table status:', {
      status: response.Table.TableStatus,
      itemCount: response.Table.ItemCount,
      keySchema: response.Table.KeySchema
    });
    
    return true;
  } catch (error: any) {
    if (error.name === 'ResourceNotFoundException') {
      return false;
    }
    throw error;
  }
}

/**
 * Get difficulty data for a date from DynamoDB (with caching)
 */
async function getDifficultyData(date: string): Promise<number> {
  try {
    const formattedDate = formatDateForDifficulty(date);
    console.info(`[DynamoDB] Fetching difficulty for date: ${formattedDate}`);

    const tableExists = await verifyTableExists(DIFFICULTY_TABLE);
    if (!tableExists) {
      console.warn(`[DynamoDB] Table ${DIFFICULTY_TABLE} does not exist, using default difficulty (${DEFAULT_DIFFICULTY})`);
      return DEFAULT_DIFFICULTY;
    }

    // First, scan the table to find records with our date
    const scanCommand = new ScanCommand({
      TableName: DIFFICULTY_TABLE,
      FilterExpression: "#date = :date",
      ExpressionAttributeNames: {
        "#date": "Date"
      },
      ExpressionAttributeValues: {
        ":date": formattedDate
      }
    });

    console.debug('[DynamoDB] Executing difficulty scan:', {
      table: DIFFICULTY_TABLE,
      date: formattedDate,
      command: 'ScanCommand'
    });

    const scanResponse = await retryOperation(() => docClient.send(scanCommand));

    if (!scanResponse.Items?.length) {
      console.warn(`[DynamoDB] No difficulty data found for ${formattedDate}, using default: ${DEFAULT_DIFFICULTY}`);
      return DEFAULT_DIFFICULTY;
    }

    // Sort items by date (descending) to get the most recent record if multiple exist
    const sortedItems = scanResponse.Items.sort((a, b) => 
      b.Date.localeCompare(a.Date)
    );

    const difficulty = Number(sortedItems[0].Difficulty);
    console.info(`[DynamoDB] Found historical difficulty for ${formattedDate}:`, {
      difficulty: difficulty.toLocaleString(),
      id: sortedItems[0].ID,
      totalRecords: sortedItems.length
    });

    if (isNaN(difficulty)) {
      console.error(`[DynamoDB] Invalid difficulty value:`, sortedItems[0].Difficulty);
      return DEFAULT_DIFFICULTY;
    }

    return difficulty;

  } catch (error) {
    console.error('[DynamoDB] Error fetching difficulty:', error);
    return DEFAULT_DIFFICULTY;
  }
}

/**
 * Calculate Bitcoin mined for a given amount of energy and miner model
 */
function calculateBitcoin(energyMWh: number, minerModel: string, difficulty: number): number {
  if (energyMWh <= 0) return 0;
  
  const config = minerConfigs[minerModel];
  if (!config) {
    console.error(`Unknown miner model: ${minerModel}`);
    return 0;
  }
  
  const { hashrate, power } = config;
  
  // Convert MWh to Wh
  const energyWh = energyMWh * 1000000;
  
  // Calculate mining time in hours
  const miningHours = energyWh / power;
  
  // Calculate Bitcoin mined
  // BTC = (hashrate * time in seconds) / (difficulty * 2^32) * 6.25
  const miningSeconds = miningHours * 3600;
  const bitcoin = (hashrate * miningSeconds) / (difficulty * Math.pow(2, 32)) * 6.25;
  
  return bitcoin;
}

/**
 * Process Bitcoin calculations for a specific date
 * This function fetches difficulty data once and processes all miners
 */
async function processBitcoinCalculations(date: string): Promise<{
  success: boolean;
  difficulty: number;
  results: Record<string, {
    recordsProcessed: number;
    totalBitcoin: number;
  }>
}> {
  try {
    console.log(`\n=== Processing Bitcoin Calculations for ${date} ===\n`);
    
    // Step 1: Fetch difficulty data ONCE for the date
    console.log('Fetching difficulty data...');
    const difficulty = await getDifficultyData(date);
    console.log(`Using difficulty: ${difficulty.toLocaleString()}`);
    
    // Step 2: Get all curtailment records for the date
    const curtailmentData = await db.query.curtailmentRecords.findMany({
      where: eq(curtailmentRecords.settlementDate, date)
    });
    
    if (curtailmentData.length === 0) {
      console.log(`No curtailment data found for ${date}`);
      return {
        success: false,
        difficulty,
        results: {}
      };
    }
    
    console.log(`Found ${curtailmentData.length} curtailment records for ${date}`);
    
    // Step 3: Process each miner model
    const results: Record<string, {
      recordsProcessed: number;
      totalBitcoin: number;
    }> = {};
    
    for (const minerModel of MINER_MODELS) {
      console.log(`\n--- Processing ${minerModel} ---`);
      
      // Clear existing records for this miner and date
      await db.delete(historicalBitcoinCalculations)
        .where(
          and(
            eq(historicalBitcoinCalculations.settlementDate, date),
            eq(historicalBitcoinCalculations.minerModel, minerModel)
          )
        );
      
      // Process by settlement period
      const settlementPeriods = new Set(curtailmentData.map(record => record.settlementPeriod));
      let totalBitcoin = 0;
      let recordsProcessed = 0;
      
      for (const period of Array.from(settlementPeriods).sort((a, b) => a - b)) {
        const periodRecords = curtailmentData.filter(record => record.settlementPeriod === period);
        
        // Group by farm
        const farmGroups = new Map<string, {
          farmId: string;
          farmName: string;
          records: typeof curtailmentData;
          totalEnergy: number;
        }>();
        
        for (const record of periodRecords) {
          const farmId = record.farmId; // Use farmId directly from the record
          
          if (!farmGroups.has(farmId)) {
            farmGroups.set(farmId, {
              farmId,
              farmName: record.leadPartyName || farmId,
              records: [],
              totalEnergy: 0
            });
          }
          
          const group = farmGroups.get(farmId)!;
          group.records.push(record);
          group.totalEnergy += Math.abs(Number(record.volume));
        }
        
        // Calculate Bitcoin for each farm
        for (const [farmId, group] of farmGroups.entries()) {
          const bitcoinMined = calculateBitcoin(group.totalEnergy, minerModel, difficulty);
          totalBitcoin += bitcoinMined;
          
          // Insert the calculation record
          await db.insert(historicalBitcoinCalculations).values({
            settlementDate: date,
            settlementPeriod: period,
            farmId,
            minerModel,
            bitcoinMined: bitcoinMined.toString(),
            difficulty: difficulty.toString(),
            // curtailedEnergy field doesn't exist in the schema, removed it
            calculatedAt: new Date()
          });
          
          recordsProcessed++;
        }
      }
      
      console.log(`${minerModel} Summary:`);
      console.log(`- Records Processed: ${recordsProcessed}`);
      console.log(`- Total Bitcoin: ${totalBitcoin.toFixed(8)} BTC`);
      
      results[minerModel] = {
        recordsProcessed,
        totalBitcoin
      };
    }
    
    console.log(`\n=== All Bitcoin Calculations Complete for ${date} ===\n`);
    
    return {
      success: true,
      difficulty,
      results
    };
  } catch (error) {
    console.error('Error processing Bitcoin calculations:', error);
    throw error;
  }
}

/**
 * Update monthly Bitcoin summaries
 */
async function updateMonthlyBitcoinSummaries(date: string): Promise<void> {
  try {
    const yearMonth = date.substring(0, 7);
    console.log(`\n=== Updating Monthly Bitcoin Summaries for ${yearMonth} ===\n`);
    
    for (const minerModel of MINER_MODELS) {
      // Calculate monthly totals - no curtailedEnergy field in historicalBitcoinCalculations table
      const monthlyData = await db
        .select({
          totalBitcoin: sql<string>`SUM(${historicalBitcoinCalculations.bitcoinMined}::numeric)`
        })
        .from(historicalBitcoinCalculations)
        .where(
          and(
            sql`DATE_TRUNC('month', ${historicalBitcoinCalculations.settlementDate}::date) = DATE_TRUNC('month', ${date}::date)`,
            eq(historicalBitcoinCalculations.minerModel, minerModel)
          )
        );
      
      if (!monthlyData[0]?.totalBitcoin) {
        console.log(`No data found for ${minerModel} in ${yearMonth}`);
        continue;
      }
      
      // Get total curtailed energy from curtailment_records for the month
      const monthlyEnergyData = await db
        .select({
          totalEnergy: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`
        })
        .from(curtailmentRecords)
        .where(
          sql`DATE_TRUNC('month', ${curtailmentRecords.settlementDate}::date) = DATE_TRUNC('month', ${date}::date)`
        );
      
      const totalEnergy = monthlyEnergyData[0]?.totalEnergy || '0';
      
      // Update or insert monthly summary
      await db.insert(bitcoinMonthlySummaries).values({
        yearMonth,
        minerModel,
        bitcoinMined: monthlyData[0].totalBitcoin,
        valueAtMining: '0', // Set default value
        updatedAt: new Date()
      }).onConflictDoUpdate({
        target: [bitcoinMonthlySummaries.yearMonth, bitcoinMonthlySummaries.minerModel],
        set: {
          bitcoinMined: monthlyData[0].totalBitcoin,
          updatedAt: new Date()
        }
      });
      
      console.log(`Updated monthly summary for ${minerModel} in ${yearMonth}:`);
      console.log(`- Energy: ${Number(totalEnergy).toFixed(2)} MWh`);
      console.log(`- Bitcoin: ${Number(monthlyData[0].totalBitcoin).toFixed(8)} BTC`);
    }
  } catch (error) {
    console.error('Error updating monthly Bitcoin summaries:', error);
    throw error;
  }
}

/**
 * Update yearly Bitcoin summaries
 */
async function updateYearlyBitcoinSummaries(date: string): Promise<void> {
  try {
    const year = date.substring(0, 4);
    console.log(`\n=== Updating Yearly Bitcoin Summaries for ${year} ===\n`);
    
    for (const minerModel of MINER_MODELS) {
      // Calculate yearly totals from monthly summaries - just bitcoin totals
      const yearlyData = await db
        .select({
          totalBitcoin: sql<string>`SUM(${bitcoinMonthlySummaries.bitcoinMined}::numeric)`
        })
        .from(bitcoinMonthlySummaries)
        .where(
          and(
            sql`${bitcoinMonthlySummaries.yearMonth} LIKE ${year + '-%'}`,
            eq(bitcoinMonthlySummaries.minerModel, minerModel)
          )
        );
      
      if (!yearlyData[0]?.totalBitcoin) {
        console.log(`No data found for ${minerModel} in ${year}`);
        continue;
      }
      
      // Get yearly energy from curtailment_records
      const yearlyEnergyData = await db
        .select({
          totalEnergy: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`
        })
        .from(curtailmentRecords)
        .where(
          sql`EXTRACT(YEAR FROM ${curtailmentRecords.settlementDate}) = ${parseInt(year, 10)}`
        );
        
      const totalEnergy = yearlyEnergyData[0]?.totalEnergy || '0';
      
      // Update or insert yearly summary
      await db.insert(bitcoinYearlySummaries).values({
        year,
        minerModel,
        bitcoinMined: yearlyData[0].totalBitcoin,
        valueAtMining: '0', // Default value
        updatedAt: new Date()
      }).onConflictDoUpdate({
        target: [bitcoinYearlySummaries.year, bitcoinYearlySummaries.minerModel],
        set: {
          bitcoinMined: yearlyData[0].totalBitcoin,
          updatedAt: new Date()
        }
      });
      
      console.log(`Updated yearly summary for ${minerModel} in ${year}:`);
      console.log(`- Energy: ${Number(totalEnergy).toFixed(2)} MWh`);
      console.log(`- Bitcoin: ${Number(yearlyData[0].totalBitcoin).toFixed(8)} BTC`);
    }
  } catch (error) {
    console.error('Error updating yearly Bitcoin summaries:', error);
    throw error;
  }
}

/**
 * Process the full cascade of Bitcoin calculations and summaries
 */
export async function processFullCascade(date: string): Promise<void> {
  try {
    // Step 1: Process Bitcoin calculations
    const bitcoinResult = await processBitcoinCalculations(date);
    
    if (!bitcoinResult.success) {
      console.log('No Bitcoin calculations to process, skipping summaries');
      return;
    }
    
    // Step 2: Update monthly summaries
    await updateMonthlyBitcoinSummaries(date);
    
    // Step 3: Update yearly summaries
    await updateYearlyBitcoinSummaries(date);
    
    console.log(`\n=== Full Cascade Processing Complete for ${date} ===`);
    
    console.log('\nSummary:');
    console.log(`Date: ${date}`);
    console.log(`Difficulty: ${bitcoinResult.difficulty.toLocaleString()}`);
    
    for (const minerModel of MINER_MODELS) {
      const result = bitcoinResult.results[minerModel];
      if (result) {
        console.log(`\n${minerModel}:`);
        console.log(`- Records: ${result.recordsProcessed}`);
        console.log(`- Bitcoin: ${result.totalBitcoin.toFixed(8)} BTC`);
      }
    }
  } catch (error) {
    console.error('Error processing full cascade:', error);
    throw error;
  }
}

/**
 * Main function
 */
async function main() {
  try {
    // Get the date from command-line arguments or use default
    const dateToProcess = process.argv[2] || format(new Date(), 'yyyy-MM-dd');
    
    // Process everything
    await processFullCascade(dateToProcess);
    
    console.log(`\n=== Processing Complete ===\n`);
  } catch (error) {
    console.error('Error in main process:', error);
    process.exit(1);
  }
}

main();