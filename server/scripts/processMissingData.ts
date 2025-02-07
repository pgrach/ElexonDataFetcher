import { format } from 'date-fns';
import { minerModels } from '../types/bitcoin';
import { processSingleDay, fetch2024Difficulties } from '../services/bitcoinService';
import { db } from "@db";
import { historicalBitcoinCalculations, curtailmentRecords } from "@db/schema";
import { sql, and, eq, inArray } from "drizzle-orm";

// Constants
const RETRY_ATTEMPTS = 3;
const RETRY_DELAY = 2000;
const MINER_MODEL_LIST = ['S19J_PRO', 'S9', 'M20S'];

async function findMissingDates() {
  const query = `
    WITH curtailment_summary AS (
      SELECT 
        settlement_date,
        array_agg(DISTINCT settlement_period) as curtailment_periods,
        COUNT(DISTINCT settlement_period) as period_count,
        COUNT(DISTINCT farm_id) as farm_count,
        SUM(ABS(volume::numeric)) as total_volume
      FROM curtailment_records
      WHERE ABS(volume::numeric) > 0
        AND settlement_date BETWEEN '2024-01-01' AND '2025-02-06'
      GROUP BY settlement_date
    ),
    bitcoin_summary AS (
      SELECT 
        settlement_date,
        array_agg(DISTINCT miner_model) as processed_models,
        miner_model,
        COUNT(DISTINCT settlement_period) as period_count,
        COUNT(DISTINCT farm_id) as farm_count,
        SUM(bitcoin_mined::numeric) as total_bitcoin
      FROM historical_bitcoin_calculations
      WHERE settlement_date BETWEEN '2024-01-01' AND '2025-02-06'
      GROUP BY settlement_date, miner_model
    )
    SELECT 
      cs.settlement_date::text as date,
      cs.curtailment_periods,
      cs.period_count as required_period_count,
      cs.farm_count,
      cs.total_volume,
      ARRAY(
        SELECT unnest(array['S19J_PRO', 'S9', 'M20S'])
        EXCEPT
        SELECT unnest(COALESCE(array_agg(DISTINCT bs.miner_model), ARRAY[]::text[]))
      ) as missing_models,
      MIN(bs.period_count) as min_calculated_periods,
      MAX(bs.period_count) as max_calculated_periods
    FROM curtailment_summary cs
    LEFT JOIN bitcoin_summary bs ON cs.settlement_date = bs.settlement_date
    WHERE cs.total_volume > 0
    GROUP BY 
      cs.settlement_date,
      cs.curtailment_periods,
      cs.period_count,
      cs.farm_count,
      cs.total_volume
    HAVING 
      ARRAY_LENGTH(ARRAY(
        SELECT unnest(array['S19J_PRO', 'S9', 'M20S'])
        EXCEPT
        SELECT unnest(COALESCE(array_agg(DISTINCT bs.miner_model), ARRAY[]::text[]))
      ), 1) > 0
      OR MIN(bs.period_count) < cs.period_count
    ORDER BY cs.settlement_date;
  `;

  const result = await db.execute(sql.raw(query));
  return result.rows.map(row => ({
    date: row.date,
    curtailmentPeriods: row.curtailment_periods,
    requiredPeriodCount: row.required_period_count,
    farmCount: row.farm_count,
    totalVolume: row.total_volume,
    missingModels: Array.isArray(row.missing_models) ? row.missing_models : MINER_MODEL_LIST,
    minCalculatedPeriods: row.min_calculated_periods || 0,
    maxCalculatedPeriods: row.max_calculated_periods || 0
  }));
}

async function verifyDateCompletion(date: string, minerModel: string) {
  try {
    // First get curtailment periods with actual volume
    const curtailmentData = await db
      .select({
        periods: sql<number[]>`array_agg(DISTINCT settlement_period)`,
        periodCount: sql<number>`COUNT(DISTINCT settlement_period)::int`,
        farmCount: sql<number>`COUNT(DISTINCT farm_id)::int`,
        totalVolume: sql<string>`SUM(ABS(volume::numeric))::text`
      })
      .from(curtailmentRecords)
      .where(
        and(
          eq(curtailmentRecords.settlementDate, date),
          sql`ABS(volume::numeric) > 0`
        )
      );

    if (!curtailmentData[0] || curtailmentData[0].periodCount === 0) {
      return { isComplete: true, noVolume: true };
    }

    // Get bitcoin calculations
    const bitcoinData = await db
      .select({
        periods: sql<number[]>`array_agg(DISTINCT settlement_period)`,
        periodCount: sql<number>`COUNT(DISTINCT settlement_period)::int`,
        farmCount: sql<number>`COUNT(DISTINCT farm_id)::int`,
        totalBitcoin: sql<string>`SUM(bitcoin_mined::numeric)::text`,
        avgDifficulty: sql<string>`AVG(difficulty::numeric)::text`
      })
      .from(historicalBitcoinCalculations)
      .where(
        and(
          eq(historicalBitcoinCalculations.settlementDate, date),
          eq(historicalBitcoinCalculations.minerModel, minerModel)
        )
      );

    const curtailmentPeriods = curtailmentData[0].periods || [];
    const calculatedPeriods = bitcoinData[0]?.periods || [];
    const missingPeriods = curtailmentPeriods.filter(p => !calculatedPeriods.includes(p));

    const isComplete = missingPeriods.length === 0 && 
                      bitcoinData[0]?.periodCount === curtailmentData[0].periodCount &&
                      bitcoinData[0]?.farmCount === curtailmentData[0].farmCount;

    return {
      curtailmentPeriods,
      periodCount: bitcoinData[0]?.periodCount || 0,
      expectedPeriods: curtailmentData[0].periodCount,
      farmCount: bitcoinData[0]?.farmCount || 0,
      expectedFarms: curtailmentData[0].farmCount,
      totalVolume: curtailmentData[0].totalVolume,
      totalBitcoin: bitcoinData[0]?.totalBitcoin || '0',
      avgDifficulty: bitcoinData[0]?.avgDifficulty || '0',
      missingPeriods,
      isComplete
    };
  } catch (error) {
    console.error(`Error verifying ${date} for ${minerModel}:`, error);
    throw error;
  }
}

async function processDate(date: string, minerModel: string) {
  let attempt = 0;
  while (attempt < RETRY_ATTEMPTS) {
    try {
      console.log(`\n- Processing ${minerModel} (Attempt ${attempt + 1}/${RETRY_ATTEMPTS})`);

      // First verify if we need to process this date
      const verification = await verifyDateCompletion(date, minerModel);

      if (verification.noVolume) {
        console.log(`✓ No curtailment volume for ${date}, skipping`);
        return true;
      }

      // Delete existing data for this model/date
      await db.delete(historicalBitcoinCalculations)
        .where(
          and(
            eq(historicalBitcoinCalculations.settlementDate, date),
            eq(historicalBitcoinCalculations.minerModel, minerModel)
          )
        );

      // Process the day
      await processSingleDay(date, minerModel);

      // Verify the results
      const results = await verifyDateCompletion(date, minerModel);

      console.log(`✓ Completed ${minerModel} for ${date}:`, {
        periodsProcessed: results.periodCount,
        expectedPeriods: results.expectedPeriods,
        farms: results.farmCount,
        expectedFarms: results.expectedFarms,
        totalVolume: results.totalVolume,
        bitcoin: results.totalBitcoin,
        difficulty: results.avgDifficulty,
        complete: results.isComplete,
        missingPeriods: results.missingPeriods
      });

      if (results.isComplete) {
        return true;
      }

      console.log(`! Incomplete processing detected, missing periods:`, results.missingPeriods);
      attempt++;

      if (attempt < RETRY_ATTEMPTS) {
        await new Promise(resolve => setTimeout(resolve, RETRY_DELAY * Math.pow(2, attempt)));
      }
    } catch (error) {
      console.error(`× Error processing ${minerModel} for ${date} (Attempt ${attempt + 1}):`, error);
      attempt++;
      if (attempt < RETRY_ATTEMPTS) {
        await new Promise(resolve => setTimeout(resolve, RETRY_DELAY * Math.pow(2, attempt)));
      }
    }
  }

  return false;
}

async function processMissingDates() {
  try {
    console.log('\n=== Processing Missing Historical Data ===');
    console.log('Date Range: 2024-01-01 to 2025-02-06\n');

    // Pre-fetch difficulties
    console.log('Pre-fetching difficulties...');
    await fetch2024Difficulties();
    console.log('Difficulties pre-fetch complete\n');

    // Get missing dates
    const missingData = await findMissingDates();
    console.log(`Found ${missingData.length} dates with missing or incomplete data\n`);

    let processed = 0;
    const totalDates = missingData.length;
    const results = {
      success: [] as string[],
      failed: [] as { date: string; minerModel: string; reason: string }[]
    };

    for (const {
      date,
      requiredPeriodCount,
      farmCount,
      totalVolume,
      missingModels,
      minCalculatedPeriods
    } of missingData) {
      console.log(`\n=== Processing Date: ${date} (${++processed}/${totalDates}) ===`);
      console.log(`Status: ${3 - missingModels.length}/3 miner models, ` +
                 `${minCalculatedPeriods || 0}/${requiredPeriodCount} periods processed`);
      console.log(`Farm count: ${farmCount}, Total volume: ${totalVolume} MWh`);
      console.log(`Missing models: ${missingModels.join(', ')}`);

      let dateSuccess = true;

      for (const minerModel of missingModels) {
        const success = await processDate(date, minerModel);

        if (!success) {
          dateSuccess = false;
          results.failed.push({
            date,
            minerModel,
            reason: 'Failed to process all curtailment periods'
          });
        }
      }

      if (dateSuccess) {
        results.success.push(date);
      }

      // Progress update
      const progress = ((processed / totalDates) * 100).toFixed(1);
      console.log(`\nOverall Progress: ${progress}% (${processed}/${totalDates})`);
      console.log(`Success: ${results.success.length}, Failed: ${results.failed.length}`);
    }

    console.log('\n=== Missing Data Processing Complete ===');
    console.log(`Total dates processed: ${totalDates}`);
    console.log(`Successfully processed: ${results.success.length}`);
    console.log(`Failed processing: ${results.failed.length}`);

    if (results.failed.length > 0) {
      console.log('\nFailed Dates:', results.failed);
    }

  } catch (error) {
    console.error('Error processing missing dates:', error);
    process.exit(1);
  }
}

// Start processing
processMissingDates();