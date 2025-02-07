import { format } from 'date-fns';
import { minerModels } from '../types/bitcoin';
import { processSingleDay, fetch2024Difficulties } from '../services/bitcoinService';
import { db } from "@db";
import { historicalBitcoinCalculations, curtailmentRecords } from "@db/schema";
import { sql, and, eq } from "drizzle-orm";

async function findMissingDates() {
  const query = `
    WITH RECURSIVE
    settlement_periods AS (
      SELECT generate_series(1, 48) as period
    ),
    date_series AS (
      SELECT generate_series(
        '2024-01-01'::date,
        '2025-02-06'::date,
        '1 day'::interval
      )::date as date
    ),
    curtailment_summary AS (
      SELECT 
        settlement_date,
        COUNT(DISTINCT settlement_period) as curtailment_periods,
        COUNT(DISTINCT farm_id) as farm_count
      FROM curtailment_records
      GROUP BY settlement_date
    ),
    bitcoin_summary AS (
      SELECT 
        settlement_date,
        miner_model,
        COUNT(DISTINCT settlement_period) as calculated_periods,
        COUNT(*) as record_count,
        MIN(difficulty::numeric) as min_difficulty,
        MAX(difficulty::numeric) as max_difficulty,
        SUM(bitcoin_mined::numeric) as total_bitcoin
      FROM historical_bitcoin_calculations
      GROUP BY settlement_date, miner_model
    )
    SELECT 
      ds.date::text as check_date,
      cs.curtailment_periods,
      cs.farm_count,
      COALESCE(array_agg(DISTINCT bs.miner_model) FILTER (WHERE bs.miner_model IS NOT NULL), ARRAY[]::text[]) as processed_models,
      MIN(bs.calculated_periods) as min_calculated_periods,
      MAX(bs.calculated_periods) as max_calculated_periods,
      MIN(bs.min_difficulty) as min_difficulty,
      SUM(bs.total_bitcoin) as total_bitcoin
    FROM date_series ds
    JOIN curtailment_summary cs ON ds.date = cs.settlement_date
    LEFT JOIN bitcoin_summary bs ON ds.date = bs.settlement_date
    WHERE cs.curtailment_periods > 0
    GROUP BY ds.date, cs.curtailment_periods, cs.farm_count
    HAVING 
      array_length(COALESCE(array_agg(DISTINCT bs.miner_model) FILTER (WHERE bs.miner_model IS NOT NULL), ARRAY[]::text[]), 1) < 3
      OR MIN(bs.calculated_periods) < cs.curtailment_periods
      OR MIN(bs.min_difficulty) IS NULL
    ORDER BY ds.date;
  `;

  const result = await db.execute(sql.raw(query));
  return result.rows.map(row => ({
    date: row.check_date,
    curtailmentPeriods: row.curtailment_periods,
    farmCount: row.farm_count,
    processedModels: Array.isArray(row.processed_models) ? row.processed_models : [],
    minCalculatedPeriods: row.min_calculated_periods || 0,
    maxCalculatedPeriods: row.max_calculated_periods || 0,
    minDifficulty: row.min_difficulty,
    totalBitcoin: row.total_bitcoin || '0'
  }));
}

async function verifyDateCompletion(date: string, minerModel: string, expectedPeriods: number) {
  const records = await db
    .select({
      periodCount: sql<number>`COUNT(DISTINCT settlement_period)::int`,
      recordCount: sql<number>`COUNT(*)::int`,
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

  const expectedRecordCount = expectedPeriods * (records[0]?.farmCount || 0);
  const isComplete = records[0]?.periodCount === expectedPeriods && 
                    records[0]?.recordCount === expectedRecordCount;

  return {
    periodCount: records[0]?.periodCount || 0,
    recordCount: records[0]?.recordCount || 0,
    farmCount: records[0]?.farmCount || 0,
    expectedRecords: expectedRecordCount,
    totalBitcoin: records[0]?.totalBitcoin || '0',
    avgDifficulty: records[0]?.avgDifficulty || '0',
    isComplete
  };
}

async function processMissingDates() {
  try {
    console.log('\n=== Processing Missing Historical Data ===');
    console.log('Date Range: 2024-01-01 to 2025-02-06');

    // Pre-fetch difficulties
    console.log('\nPre-fetching difficulties...');
    await fetch2024Difficulties();
    console.log('Difficulties pre-fetch complete\n');

    // Get missing dates
    const missingData = await findMissingDates();
    console.log(`Found ${missingData.length} dates with missing or incomplete data`);

    let processed = 0;
    const totalDates = missingData.length;

    for (const { date, curtailmentPeriods, farmCount, processedModels, minCalculatedPeriods } of missingData) {
      console.log(`\n=== Processing Date: ${date} (${++processed}/${totalDates}) ===`);
      console.log(`Status: ${processedModels.length}/3 miner models, ` +
                 `${minCalculatedPeriods || 0}/${curtailmentPeriods} periods processed, ` +
                 `${farmCount} farms`);

      if (processedModels.length > 0) {
        console.log(`Already processed models: ${processedModels.join(', ')}`);
      }

      // Get list of models to process
      const remainingModels = Object.keys(minerModels).filter(model => 
        !processedModels.includes(model)
      );

      // Process each missing model
      for (const minerModel of remainingModels) {
        try {
          console.log(`\n- Processing ${minerModel}`);

          // First delete any incomplete data for this model/date
          await db.delete(historicalBitcoinCalculations)
            .where(
              and(
                eq(historicalBitcoinCalculations.settlementDate, date),
                eq(historicalBitcoinCalculations.minerModel, minerModel)
              )
            );

          // Process the day
          await processSingleDay(date, minerModel);

          // Verify the data was properly inserted
          const verification = await verifyDateCompletion(date, minerModel, curtailmentPeriods);
          console.log(`✓ Completed ${minerModel} for ${date}:`, {
            periodsProcessed: verification.periodCount,
            recordsCreated: verification.recordCount,
            expectedRecords: verification.expectedRecords,
            farms: verification.farmCount,
            bitcoin: verification.totalBitcoin,
            difficulty: verification.avgDifficulty,
            complete: verification.isComplete
          });

          if (!verification.isComplete) {
            throw new Error(
              `Incomplete processing for ${minerModel} on ${date}: ` +
              `got ${verification.periodCount}/${curtailmentPeriods} periods, ` +
              `${verification.recordCount}/${verification.expectedRecords} records`
            );
          }

        } catch (error) {
          console.error(`× Error processing ${minerModel} for ${date}:`, error);
          // Continue with next model instead of throwing
          console.log(`  Continuing with next model...`);
        }
      }

      // Progress update
      const progress = ((processed / totalDates) * 100).toFixed(1);
      console.log(`\nOverall Progress: ${progress}% (${processed}/${totalDates})`);
    }

    console.log('\n=== Missing Data Processing Complete ===');
    console.log(`Total dates processed: ${totalDates}`);

  } catch (error) {
    console.error('Error processing missing dates:', error);
    process.exit(1);
  }
}

// Start processing
processMissingDates();