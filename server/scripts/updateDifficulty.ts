import { processHistoricalCalculations, processSingleDay, fetch2024Difficulties } from '../services/bitcoinService';
import { getDifficultyData } from '../services/dynamodbService';
import { db } from "@db";
import { historicalBitcoinCalculations } from "@db/schema";
import { and, eq, sql } from "drizzle-orm";
import { minerModels } from '../types/bitcoin';

const DATE_TO_UPDATE = '2025-02-10';

async function updateDifficultyData() {
  try {
    console.log(`\n=== Updating Bitcoin Calculations for ${DATE_TO_UPDATE} ===`);

    // Step 1: Fetch difficulty from DynamoDB
    console.log('\nFetching difficulty from DynamoDB...');
    const difficulty = await getDifficultyData(DATE_TO_UPDATE);
    console.log(`Fetched difficulty: ${difficulty}`);

    // Step 2: Pre-fetch difficulties to ensure cache is populated
    await fetch2024Difficulties();

    // Step 3: Process each miner model
    const MINER_MODEL_LIST = Object.keys(minerModels);
    console.log(`\nProcessing ${MINER_MODEL_LIST.length} miner models...`);

    for (const minerModel of MINER_MODEL_LIST) {
      console.log(`\n- Processing ${minerModel}`);

      // Delete existing records for this date/model
      await db.delete(historicalBitcoinCalculations)
        .where(
          and(
            eq(historicalBitcoinCalculations.settlementDate, DATE_TO_UPDATE),
            eq(historicalBitcoinCalculations.minerModel, minerModel)
          )
        );

      // Process the day with new difficulty
      await processSingleDay(DATE_TO_UPDATE, minerModel);

      // Verify records were created
      const records = await db
        .select({
          count: sql<number>`COUNT(*)::int`,
          totalBitcoin: sql<string>`SUM(bitcoin_mined::numeric)::text`,
          avgDifficulty: sql<string>`AVG(difficulty::numeric)::text`
        })
        .from(historicalBitcoinCalculations)
        .where(
          and(
            eq(historicalBitcoinCalculations.settlementDate, DATE_TO_UPDATE),
            eq(historicalBitcoinCalculations.minerModel, minerModel)
          )
        );

      console.log(`✓ ${minerModel} processed:`, {
        recordCount: records[0].count,
        totalBitcoin: records[0].totalBitcoin,
        difficulty: records[0].avgDifficulty
      });
    }

    console.log('\n=== Update Complete ===');

  } catch (error) {
    console.error('Error updating difficulty data:', error);
    process.exit(1);
  }
}

// Run the update
updateDifficultyData();