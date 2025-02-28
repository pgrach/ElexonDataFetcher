import { db } from "@db";
import { curtailmentRecords, dailySummaries, monthlySummaries, yearlySummaries, historicalBitcoinCalculations } from "@db/schema";
import { format, startOfMonth, endOfMonth, parseISO, isBefore, subDays, subMonths, eachDayOfInterval } from "date-fns";
import { processDailyCurtailment } from "./curtailment";
import { fetchBidsOffers } from "./elexon";
import { eq, and, sql } from "drizzle-orm";
import { processSingleDay } from "./bitcoinService";

// Configuration constants
const MAX_CONCURRENT_DAYS = 5;
const RECONCILIATION_HOUR = 3; // Run at 3 AM to ensure all updates are captured
const SAMPLE_PERIODS = [1, 12, 24, 36, 48]; // Check more periods throughout the day
const LOOK_BACK_DAYS = 7; // Look back up to a week for potential updates
const MONTHLY_RECONCILIATION_HOUR = 2; // Run monthly reconciliation at 2 AM, before daily reconciliation
const MINER_MODEL_LIST = ['S19J_PRO', 'S9', 'M20S']; // Standard miner models used throughout the application

/**
 * Check if a specific day's data needs to be reprocessing by comparing
 * sample periods with the Elexon API
 */
async function needsReprocessing(date: string): Promise<boolean> {
  try {
    console.log(`Checking if ${date} needs reprocessing...`);

    // Get daily summary for comparison
    const summary = await db.query.dailySummaries.findFirst({
      where: eq(dailySummaries.summaryDate, date)
    });

    console.log(`Current daily summary for ${date}:`, {
      energy: summary?.totalCurtailedEnergy ? `${Number(summary.totalCurtailedEnergy).toFixed(2)} MWh` : 'No data',
      payment: summary?.totalPayment ? `£${Number(summary.totalPayment).toFixed(2)}` : 'No data'
    });

    let totalApiVolume = 0;
    let totalApiPayment = 0;
    let totalDbVolume = 0;
    let totalDbPayment = 0;

    for (const period of SAMPLE_PERIODS) {
      const apiRecords = await fetchBidsOffers(date, period);
      console.log(`[${date} P${period}] API records: ${apiRecords.length}`);

      // Calculate API totals for this period
      const apiTotal = apiRecords.reduce((acc, record) => ({
        volume: acc.volume + Math.abs(record.volume),
        payment: acc.payment + (Math.abs(record.volume) * record.originalPrice)
      }), { volume: 0, payment: 0 });

      totalApiVolume += apiTotal.volume;
      totalApiPayment += apiTotal.payment;

      // Get existing records for this period
      const dbRecords = await db
        .select({
          totalVolume: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
          totalPayment: sql<string>`SUM(ABS(${curtailmentRecords.payment}::numeric))`
        })
        .from(curtailmentRecords)
        .where(
          and(
            eq(curtailmentRecords.settlementDate, date),
            eq(curtailmentRecords.settlementPeriod, period)
          )
        );

      const dbTotal = {
        volume: Number(dbRecords[0]?.totalVolume || 0),
        payment: Number(dbRecords[0]?.totalPayment || 0)
      };

      totalDbVolume += dbTotal.volume;
      totalDbPayment += dbTotal.payment;

      // Compare totals with a small tolerance for floating point differences
      const volumeDiff = Math.abs(apiTotal.volume - dbTotal.volume);
      const paymentDiff = Math.abs(apiTotal.payment - dbTotal.payment);

      if (volumeDiff > 0.01 || paymentDiff > 0.01) {
        console.log(`[${date} P${period}] Differences detected:`, {
          volumeDiff: volumeDiff.toFixed(3),
          paymentDiff: paymentDiff.toFixed(3)
        });
        return true;
      }
    }

    // Compare total daily values
    const avgVolumeDiff = Math.abs((totalApiVolume / SAMPLE_PERIODS.length) - Number(summary?.totalCurtailedEnergy || 0));
    const avgPaymentDiff = Math.abs((totalApiPayment / SAMPLE_PERIODS.length) - Number(summary?.totalPayment || 0));

    if (avgVolumeDiff > 1 || avgPaymentDiff > 10) {
      console.log('Significant daily total differences detected:', {
        volumeDiff: avgVolumeDiff.toFixed(2),
        paymentDiff: avgPaymentDiff.toFixed(2)
      });
      return true;
    }

    return false;
  } catch (error) {
    console.error(`Error checking data for ${date}:`, error);
    return true; // Reprocess on error to be safe
  }
}

export async function reconcileDay(date: string): Promise<void> {
  try {
    if (await needsReprocessing(date)) {
      console.log(`[${date}] Data differences detected, reprocessing...`);
      await processDailyCurtailment(date);

      // Verify the update
      const summary = await db.query.dailySummaries.findFirst({
        where: eq(dailySummaries.summaryDate, date)
      });

      console.log(`[${date}] Reprocessing complete:`, {
        energy: `${Number(summary?.totalCurtailedEnergy || 0).toFixed(2)} MWh`,
        payment: `£${Number(summary?.totalPayment || 0).toFixed(2)}`
      });

      // Update Bitcoin calculations after curtailment data is updated
      console.log(`[${date}] Updating Bitcoin calculations...`);

      // Process for all three miner models
      const minerModels = ['S19J_PRO', 'S9', 'M20S'];
      for (const minerModel of minerModels) {
        await processSingleDay(date, minerModel)
          .catch(error => {
            console.error(`Error processing Bitcoin calculations for ${date} with ${minerModel}:`, error);
            // Continue with other models even if one fails
          });
      }

      console.log(`[${date}] Bitcoin calculations updated for models: ${minerModels.join(', ')}`);
    } else {
      console.log(`[${date}] Data is up to date`);
    }
  } catch (error) {
    console.error(`Error reconciling data for ${date}:`, error);
    throw error;
  }
}

export async function reconcileRecentData(): Promise<void> {
  try {
    const now = new Date();
    const startDate = subDays(now, LOOK_BACK_DAYS);
    const dates: string[] = [];

    // Add recent days
    let currentDate = startDate;
    while (isBefore(currentDate, now)) {
      dates.push(format(currentDate, 'yyyy-MM-dd'));
      currentDate = new Date(currentDate.setDate(currentDate.getDate() + 1));
    }

    console.log(`Starting reconciliation for recent days (${format(startDate, 'yyyy-MM-dd')} to ${format(now, 'yyyy-MM-dd')})`);

    // Process dates in batches
    for (let i = 0; i < dates.length; i += MAX_CONCURRENT_DAYS) {
      const batch = dates.slice(i, i + MAX_CONCURRENT_DAYS);
      await Promise.all(batch.map(date => reconcileDay(date)));
    }

    console.log('Completed reconciliation of recent data');
  } catch (error) {
    console.error('Error during recent data reconciliation:', error);
    throw error;
  }
}

export async function reconcilePreviousMonth(): Promise<void> {
  try {
    const now = new Date();
    const previousMonth = subMonths(now, 1);
    const startDate = startOfMonth(previousMonth);
    const endDate = endOfMonth(previousMonth);

    console.log(`Starting reconciliation for previous month: ${format(previousMonth, 'yyyy-MM')}`);

    // Get all dates in the previous month
    const dates: string[] = [];
    let currentDate = startDate;

    while (isBefore(currentDate, endDate)) {
      dates.push(format(currentDate, 'yyyy-MM-dd'));
      currentDate = new Date(currentDate.setDate(currentDate.getDate() + 1));
    }

    // Process dates in batches
    for (let i = 0; i < dates.length; i += MAX_CONCURRENT_DAYS) {
      const batch = dates.slice(i, i + MAX_CONCURRENT_DAYS);
      await Promise.all(batch.map(date => reconcileDay(date)));
    }

    console.log(`Completed reconciliation for ${format(previousMonth, 'yyyy-MM')}`);
  } catch (error) {
    console.error('Error during previous month reconciliation:', error);
    throw error;
  }
}

export function shouldRunReconciliation(): boolean {
  const currentHour = new Date().getHours();
  return currentHour === RECONCILIATION_HOUR;
}

export function shouldRunMonthlyReconciliation(): boolean {
  const currentHour = new Date().getHours();
  return currentHour === MONTHLY_RECONCILIATION_HOUR;
}

export async function reconcileYearlyData(): Promise<void> {
  try {
    const currentYear = new Date().getFullYear();
    console.log(`Starting yearly data reconciliation for ${currentYear}`);

    // Calculate totals from monthly summaries
    const monthlyTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(${monthlySummaries.totalCurtailedEnergy}::numeric)`,
        totalPayment: sql<string>`SUM(ABS(${monthlySummaries.totalPayment}::numeric))`
      })
      .from(monthlySummaries)
      .where(sql`TO_DATE(${monthlySummaries.yearMonth} || '-01', 'YYYY-MM-DD')::date >= DATE_TRUNC('year', NOW())::date
            AND TO_DATE(${monthlySummaries.yearMonth} || '-01', 'YYYY-MM-DD')::date < DATE_TRUNC('year', NOW())::date + INTERVAL '1 year'`);

    if (monthlyTotals[0]?.totalCurtailedEnergy) {
      // Update yearly summary
      await db.insert(yearlySummaries).values({
        year: currentYear.toString(),
        totalCurtailedEnergy: monthlyTotals[0].totalCurtailedEnergy,
        totalPayment: monthlyTotals[0].totalPayment,
        updatedAt: new Date()
      }).onConflictDoUpdate({
        target: [yearlySummaries.year],
        set: {
          totalCurtailedEnergy: monthlyTotals[0].totalCurtailedEnergy,
          totalPayment: monthlyTotals[0].totalPayment,
          updatedAt: new Date()
        }
      });

      console.log(`Updated yearly summary for ${currentYear}:`, {
        energy: Number(monthlyTotals[0].totalCurtailedEnergy).toFixed(2),
        payment: Number(monthlyTotals[0].totalPayment).toFixed(2)
      });
    }
  } catch (error) {
    console.error('Error during yearly reconciliation:', error);
  }
}

export async function reprocessDay(dateStr: string) {
  try {
    console.log(`\n=== Starting Data Re-processing for ${dateStr} ===`);

    // Use the reconciliation process to check and update if needed
    await reconcileDay(dateStr);

    console.log(`\n=== Data Re-processing Complete for ${dateStr} ===\n`);
  } catch (error) {
    console.error('Error during re-processing:', error);
    throw error;
  }
}

/**
 * Finds dates with missing or incomplete Bitcoin calculations.
 * This is a utility function used by audit tools and scripts.
 */
export async function findMissingDates(startDate: string, endDate: string) {
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
        AND settlement_date BETWEEN $1 AND $2
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
      WHERE settlement_date BETWEEN $1 AND $2
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

  // Modify the query to use direct parameters instead
  const modifiedQuery = query.replace(/\$1/g, `'${startDate}'`).replace(/\$2/g, `'${endDate}'`);
  const result = await db.execute(sql.raw(modifiedQuery));
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

/**
 * Process a date range for reconciliation.
 * This is a utility function that can be used by various scripts.
 */
export async function reconcileDateRange(startDate: string, endDate: string): Promise<{
  processedDates: number;
  updatedDates: number;
  errors: Array<{ date: string; error: string }>;
}> {
  try {
    console.log(`\n=== Processing Date Range (${startDate} to ${endDate}) ===\n`);
    
    const dates = eachDayOfInterval({
      start: parseISO(startDate),
      end: parseISO(endDate)
    }).map(date => format(date, 'yyyy-MM-dd'));
    
    let updatedDates = 0;
    const errors: Array<{ date: string; error: string }> = [];
    
    // Process dates in batches
    for (let i = 0; i < dates.length; i += MAX_CONCURRENT_DAYS) {
      const batch = dates.slice(i, i + MAX_CONCURRENT_DAYS);
      
      const results = await Promise.allSettled(
        batch.map(async (date) => {
          const needsUpdate = await needsReprocessing(date);
          if (needsUpdate) {
            await reconcileDay(date);
            return true;
          }
          return false;
        })
      );
      
      // Analyze results
      results.forEach((result, index) => {
        const date = batch[index];
        if (result.status === 'fulfilled') {
          if (result.value === true) {
            updatedDates++;
          }
        } else {
          errors.push({ date, error: result.reason.message });
        }
      });
      
      // Log progress
      console.log(`Processed ${i + batch.length}/${dates.length} dates (${updatedDates} updated)`);
    }
    
    console.log(`\n=== Date Range Processing Complete ===`);
    console.log(`Total dates processed: ${dates.length}`);
    console.log(`Dates updated: ${updatedDates}`);
    
    return {
      processedDates: dates.length,
      updatedDates,
      errors
    };
  } catch (error) {
    console.error('Error during date range reconciliation:', error);
    throw error;
  }
}

/**
 * Check for and fix Bitcoin calculation issues for a specific date.
 * This combines verification of completeness with automatic fixing.
 */
export async function auditAndFixBitcoinCalculations(date: string): Promise<{
  success: boolean;
  fixed: boolean;
  message: string;
}> {
  try {
    console.log(`\n=== Auditing Bitcoin Calculations for ${date} ===`);
    
    // Check for missing calculations
    const missingCalculations = [];
    
    for (const minerModel of MINER_MODEL_LIST) {
      // Check if all records exist for this date and model
      const existingRecords = await db
        .select({
          count: sql<number>`COUNT(*)`,
          distinct_periods: sql<number>`COUNT(DISTINCT settlement_period)`
        })
        .from(historicalBitcoinCalculations)
        .where(
          and(
            eq(historicalBitcoinCalculations.settlementDate, date),
            eq(historicalBitcoinCalculations.minerModel, minerModel)
          )
        );
      
      // Check curtailment records to see how many periods should exist
      const curtailmentPeriods = await db
        .select({
          distinct_periods: sql<number>`COUNT(DISTINCT settlement_period)`
        })
        .from(curtailmentRecords)
        .where(eq(curtailmentRecords.settlementDate, date));
      
      const expectedPeriods = curtailmentPeriods[0]?.distinct_periods || 0;
      const actualPeriods = existingRecords[0]?.distinct_periods || 0;
      
      if (expectedPeriods > 0 && (actualPeriods === 0 || actualPeriods < expectedPeriods)) {
        missingCalculations.push({
          minerModel,
          expectedPeriods,
          actualPeriods
        });
      }
    }
    
    if (missingCalculations.length === 0) {
      console.log(`✓ All Bitcoin calculations for ${date} are complete`);
      return {
        success: true,
        fixed: false,
        message: 'All calculations are up to date'
      };
    }
    
    // Fix missing calculations
    console.log(`! Found missing calculations for ${date}:`, missingCalculations);
    console.log('Fixing missing calculations...');
    
    for (const { minerModel } of missingCalculations) {
      await processSingleDay(date, minerModel);
      console.log(`✓ Processed ${minerModel} for ${date}`);
    }
    
    console.log(`\n=== Bitcoin Calculation Audit Complete for ${date} ===`);
    return {
      success: true,
      fixed: true,
      message: `Fixed calculations for ${missingCalculations.map(m => m.minerModel).join(', ')}`
    };
  } catch (error) {
    console.error(`Error auditing Bitcoin calculations for ${date}:`, error);
    return {
      success: false,
      fixed: false,
      message: `Error: ${error instanceof Error ? error.message : String(error)}`
    };
  }
}

/**
 * Run the CLI command processor with the given arguments
 */
export async function runCliCommand(args: string[]): Promise<void> {
  if (args.length === 0) {
    console.error(`
Usage:
  Process a single date:
    ts-node historicalReconciliation.ts process-day YYYY-MM-DD
  
  Process a date range:
    ts-node historicalReconciliation.ts process-range YYYY-MM-DD YYYY-MM-DD
  
  Audit recent data:
    ts-node historicalReconciliation.ts recent-data [days=7]
  
  Audit previous month:
    ts-node historicalReconciliation.ts prev-month
  
  Verify Bitcoin calculations:
    ts-node historicalReconciliation.ts verify-bitcoin YYYY-MM-DD
`);
    process.exit(1);
  }

  const command = args[0];
  
  try {
    if (command === 'process-day') {
      if (!args[1] || !args[1].match(/^\d{4}-\d{2}-\d{2}$/)) {
        console.error('Please provide a date in YYYY-MM-DD format');
        process.exit(1);
      }
      await reprocessDay(args[1]);
      console.log('Reprocessing complete');
    }
    else if (command === 'process-range') {
      if (!args[1] || !args[1].match(/^\d{4}-\d{2}-\d{2}$/) || 
          !args[2] || !args[2].match(/^\d{4}-\d{2}-\d{2}$/)) {
        console.error('Please provide start and end dates in YYYY-MM-DD format');
        process.exit(1);
      }
      const result = await reconcileDateRange(args[1], args[2]);
      console.log('Processing complete:', result);
    }
    else if (command === 'recent-data') {
      const days = args[1] ? parseInt(args[1], 10) : LOOK_BACK_DAYS;
      console.log(`Processing recent data (last ${days} days)...`);
      await reconcileRecentData();
      console.log('Recent data reconciliation complete');
    }
    else if (command === 'prev-month') {
      console.log('Processing previous month...');
      await reconcilePreviousMonth();
      console.log('Previous month reconciliation complete');
    }
    else if (command === 'verify-bitcoin') {
      if (!args[1] || !args[1].match(/^\d{4}-\d{2}-\d{2}$/)) {
        console.error('Please provide a date in YYYY-MM-DD format');
        process.exit(1);
      }
      const result = await auditAndFixBitcoinCalculations(args[1]);
      console.log('Bitcoin verification complete:', result);
    }
    else {
      console.error(`Unknown command: ${command}`);
      process.exit(1);
    }
  } catch (error) {
    console.error('Fatal error:', error);
    process.exit(1);
  }
}

// Run CLI commands if this module is executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
  runCliCommand(process.argv.slice(2))
    .then(() => process.exit(0))
    .catch((error) => {
      console.error('Fatal error in CLI execution:', error);
      process.exit(1);
    });
}