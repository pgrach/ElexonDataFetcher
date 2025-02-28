/**
 * Comprehensive Reconciliation Tool
 * 
 * This script ensures 100% reconciliation between curtailment_records and historical_bitcoin_calculations tables.
 * It identifies any missing or incomplete Bitcoin calculations, processes them, and verifies the results.
 * 
 * Usage:
 *   npx tsx comprehensive_reconcile.ts status                 - Check overall reconciliation status
 *   npx tsx comprehensive_reconcile.ts check-date YYYY-MM-DD  - Check a specific date
 *   npx tsx comprehensive_reconcile.ts fix-date YYYY-MM-DD    - Fix a specific date
 *   npx tsx comprehensive_reconcile.ts fix-all [limit]        - Fix all dates with missing calculations (optional limit)
 *   npx tsx comprehensive_reconcile.ts fix-range START END    - Fix a date range (YYYY-MM-DD format)
 */

import { db } from "./db";
import { curtailmentRecords, historicalBitcoinCalculations } from "./db/schema";
import { sql } from "drizzle-orm";
import { format, parseISO, eachDayOfInterval } from "date-fns";
import { auditAndFixBitcoinCalculations, reconcileDay } from "./server/services/historicalReconciliation";

// Core miner models used throughout the application
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];
const MAX_BATCH_SIZE = 5; // Maximum number of dates to process in parallel

/**
 * Get detailed reconciliation status for the entire database or a specific date range
 */
async function getDetailedReconciliationStatus(startDate?: string, endDate?: string) {
  let dateFilter = "";
  const params: any[] = [];
  
  if (startDate && endDate) {
    dateFilter = "WHERE c.settlement_date BETWEEN $1 AND $2";
    params.push(startDate, endDate);
  } else if (startDate) {
    dateFilter = "WHERE c.settlement_date >= $1";
    params.push(startDate);
  } else if (endDate) {
    dateFilter = "WHERE c.settlement_date <= $1";
    params.push(endDate);
  }

  // Modify the query to include the date filter if provided
  const query = `
    WITH curtailment_summary AS (
      SELECT 
        COUNT(*) as total_records,
        COUNT(DISTINCT (settlement_date || '-' || settlement_period || '-' || farm_id)) as unique_combinations,
        COUNT(DISTINCT settlement_date) as unique_dates
      FROM curtailment_records c
      ${dateFilter}
    ),
    bitcoin_summary AS (
      SELECT
        miner_model,
        COUNT(*) as calculation_count,
        COUNT(DISTINCT settlement_date) as date_count
      FROM historical_bitcoin_calculations b
      ${dateFilter}
      GROUP BY miner_model
    ),
    aggregated_bitcoin AS (
      SELECT
        SUM(calculation_count) as total_calculations,
        COUNT(DISTINCT miner_model) as model_count
      FROM bitcoin_summary
    )
    SELECT 
      cs.total_records,
      cs.unique_combinations,
      cs.unique_dates,
      COALESCE(ab.total_calculations, 0) as total_bitcoin_calculations,
      cs.unique_combinations * ${MINER_MODELS.length} as expected_calculations,
      ROUND(COALESCE(ab.total_calculations, 0) * 100.0 / NULLIF(cs.unique_combinations * ${MINER_MODELS.length}, 0), 2) as reconciliation_percentage
    FROM curtailment_summary cs
    LEFT JOIN aggregated_bitcoin ab ON true
  `;

  // Replace parameters in the query string - a simplified approach
  let finalQuery = query;
  if (params.length > 0) {
    params.forEach((param, index) => {
      finalQuery = finalQuery.replace(`$${index + 1}`, `'${param}'`);
    });
  }

  const overallResult = await db.execute(sql.raw(finalQuery));
  
  // Get data by miner model
  const modelQuery = `
    WITH curtailment_summary AS (
      SELECT 
        COUNT(DISTINCT (settlement_date || '-' || settlement_period || '-' || farm_id)) as unique_combinations
      FROM curtailment_records c
      ${dateFilter}
    ),
    bitcoin_by_model AS (
      SELECT
        miner_model,
        COUNT(*) as calculation_count
      FROM historical_bitcoin_calculations b
      ${dateFilter}
      GROUP BY miner_model
    )
    SELECT 
      bm.miner_model,
      bm.calculation_count,
      cs.unique_combinations as expected_per_model,
      ROUND(bm.calculation_count * 100.0 / NULLIF(cs.unique_combinations, 0), 2) as model_percentage
    FROM bitcoin_by_model bm
    CROSS JOIN curtailment_summary cs
    ORDER BY bm.miner_model
  `;

  let finalModelQuery = modelQuery;
  if (params.length > 0) {
    params.forEach((param, index) => {
      finalModelQuery = finalModelQuery.replace(`$${index + 1}`, `'${param}'`);
    });
  }

  const modelResult = await db.execute(sql.raw(finalModelQuery));

  // Format the results
  const overall = overallResult.rows[0] ? {
    totalCurtailmentRecords: Number(overallResult.rows[0].total_records || 0),
    uniqueCombinations: Number(overallResult.rows[0].unique_combinations || 0),
    uniqueDates: Number(overallResult.rows[0].unique_dates || 0),
    totalBitcoinCalculations: Number(overallResult.rows[0].total_bitcoin_calculations || 0),
    expectedCalculations: Number(overallResult.rows[0].expected_calculations || 0),
    reconciliationPercentage: Number(overallResult.rows[0].reconciliation_percentage || 0),
  } : {
    totalCurtailmentRecords: 0,
    uniqueCombinations: 0,
    uniqueDates: 0,
    totalBitcoinCalculations: 0,
    expectedCalculations: 0,
    reconciliationPercentage: 0,
  };

  const byModel: Record<string, { count: number, expected: number, percentage: number }> = {};
  modelResult.rows.forEach((row) => {
    byModel[String(row.miner_model)] = {
      count: Number(row.calculation_count || 0),
      expected: Number(row.expected_per_model || 0),
      percentage: Number(row.model_percentage || 0),
    };
  });

  // Add entries for any missing models
  MINER_MODELS.forEach(model => {
    if (!byModel[model]) {
      byModel[model] = {
        count: 0,
        expected: overall.uniqueCombinations,
        percentage: 0,
      };
    }
  });

  return { overall, byModel };
}

/**
 * Find all dates with missing or incomplete Bitcoin calculations
 */
async function findDatesWithMissingCalculations(startDate?: string, endDate?: string, limit: number = 100) {
  let dateFilter = "";
  const params: any[] = [];
  
  if (startDate && endDate) {
    dateFilter = "WHERE settlement_date BETWEEN $1 AND $2";
    params.push(startDate, endDate);
  } else if (startDate) {
    dateFilter = "WHERE settlement_date >= $1";
    params.push(startDate);
  } else if (endDate) {
    dateFilter = "WHERE settlement_date <= $1";
    params.push(endDate);
  }

  const query = `
    WITH dates_with_curtailment AS (
      SELECT DISTINCT settlement_date
      FROM curtailment_records
      ${dateFilter}
      ORDER BY settlement_date DESC
    ),
    unique_date_combos AS (
      SELECT 
        settlement_date,
        COUNT(DISTINCT (settlement_period || '-' || farm_id)) as unique_combinations
      FROM curtailment_records
      ${dateFilter ? dateFilter : ""}
      GROUP BY settlement_date
    ),
    date_calculations AS (
      SELECT 
        c.settlement_date,
        COUNT(DISTINCT b.id) as calculation_count,
        u.unique_combinations * ${MINER_MODELS.length} as expected_count
      FROM dates_with_curtailment c
      JOIN unique_date_combos u ON c.settlement_date = u.settlement_date
      LEFT JOIN historical_bitcoin_calculations b 
        ON c.settlement_date = b.settlement_date
      GROUP BY c.settlement_date, u.unique_combinations
    )
    SELECT 
      settlement_date::text as date,
      calculation_count,
      expected_count,
      ROUND((calculation_count * 100.0) / NULLIF(expected_count, 0), 2) as completion_percentage
    FROM date_calculations
    WHERE calculation_count < expected_count
    ORDER BY completion_percentage ASC, settlement_date DESC
    LIMIT ${limit}
  `;

  // Replace parameters in the query string
  let finalQuery = query;
  if (params.length > 0) {
    params.forEach((param, index) => {
      finalQuery = finalQuery.replace(`$${index + 1}`, `'${param}'`);
    });
  }

  const result = await db.execute(sql.raw(finalQuery));
  
  return result.rows.map(row => ({
    date: String(row.date),
    actual: Number(row.calculation_count || 0),
    expected: Number(row.expected_count || 0),
    completionPercentage: Number(row.completion_percentage || 0)
  }));
}

/**
 * Get detailed information about Bitcoin calculations for a specific date
 */
async function getDateReconciliationDetails(date: string) {
  // First get the curtailment records summary
  const curtailmentSummary = await db.execute(sql`
    WITH period_summary AS (
      SELECT 
        settlement_period,
        COUNT(DISTINCT farm_id) as farm_count,
        SUM(ABS(volume::numeric)) as total_volume
      FROM curtailment_records
      WHERE settlement_date = ${date}
      GROUP BY settlement_period
      ORDER BY settlement_period
    )
    SELECT 
      json_agg(json_build_object(
        'period', settlement_period,
        'farmCount', farm_count,
        'volume', total_volume
      )) as period_data,
      COUNT(*) as period_count,
      SUM(farm_count) as total_farm_periods
    FROM period_summary
  `);
  
  // Then get the Bitcoin calculation summary by model
  const calculationSummary = await db.execute(sql`
    WITH calc_summary AS (
      SELECT 
        miner_model,
        COUNT(DISTINCT (settlement_period || '-' || farm_id)) as combination_count,
        COUNT(DISTINCT settlement_period) as period_count,
        SUM(bitcoin_mined::numeric) as total_bitcoin
      FROM historical_bitcoin_calculations
      WHERE settlement_date = ${date}
      GROUP BY miner_model
      ORDER BY miner_model
    )
    SELECT 
      miner_model,
      combination_count,
      period_count,
      total_bitcoin
    FROM calc_summary
  `);
  
  // Get detailed list of missing combinations
  const missingCalculations = await db.execute(sql`
    WITH curtailment_combos AS (
      SELECT DISTINCT
        settlement_period,
        farm_id
      FROM curtailment_records
      WHERE settlement_date = ${date}
    ),
    bitcoin_combos AS (
      SELECT DISTINCT
        miner_model,
        settlement_period,
        farm_id
      FROM historical_bitcoin_calculations
      WHERE settlement_date = ${date}
    ),
    missing_by_model AS (
      SELECT
        m.miner_model,
        c.settlement_period,
        c.farm_id
      FROM curtailment_combos c
      CROSS JOIN (SELECT unnest(ARRAY['S19J_PRO', 'S9', 'M20S']) as miner_model) m
      EXCEPT
      SELECT
        miner_model,
        settlement_period,
        farm_id
      FROM bitcoin_combos
    )
    SELECT
      miner_model,
      settlement_period,
      farm_id,
      COUNT(*) OVER (PARTITION BY miner_model) as model_missing_count
    FROM missing_by_model
    ORDER BY miner_model, settlement_period, farm_id
  `);
  
  // Format the results
  const periodData = curtailmentSummary.rows[0]?.period_data || [];
  const periodCount = Number(curtailmentSummary.rows[0]?.period_count || 0);
  const totalFarmPeriods = Number(curtailmentSummary.rows[0]?.total_farm_periods || 0);
  
  const calculationsByModel: Record<string, { combinationCount: number, periodCount: number, totalBitcoin: number }> = {};
  
  calculationSummary.rows.forEach(row => {
    calculationsByModel[String(row.miner_model)] = {
      combinationCount: Number(row.combination_count || 0),
      periodCount: Number(row.period_count || 0),
      totalBitcoin: Number(row.total_bitcoin || 0)
    };
  });
  
  // Add entries for any missing models
  MINER_MODELS.forEach(model => {
    if (!calculationsByModel[model]) {
      calculationsByModel[model] = {
        combinationCount: 0,
        periodCount: 0,
        totalBitcoin: 0
      };
    }
  });
  
  // Group missing calculations by model
  const missingByModel: Record<string, Array<{ period: number, farmId: string }>> = {};
  
  missingCalculations.rows.forEach(row => {
    const model = String(row.miner_model);
    if (!missingByModel[model]) {
      missingByModel[model] = [];
    }
    
    missingByModel[model].push({
      period: Number(row.settlement_period),
      farmId: String(row.farm_id)
    });
  });
  
  // Calculate overall reconciliation percentage
  const expectedTotal = totalFarmPeriods * MINER_MODELS.length;
  const actualTotal = Object.values(calculationsByModel).reduce((sum, model) => sum + model.combinationCount, 0);
  
  let reconciliationPercentage = expectedTotal > 0 ? Math.min((actualTotal * 100) / expectedTotal, 100) : 100;
  reconciliationPercentage = Math.round(reconciliationPercentage * 100) / 100;
  
  return {
    date,
    curtailment: {
      periodCount,
      periodData,
      totalFarmPeriods
    },
    calculations: {
      byModel: calculationsByModel,
      missing: missingByModel,
      totalCalculations: actualTotal,
      expectedCalculations: expectedTotal,
      reconciliationPercentage
    }
  };
}

/**
 * Process a batch of dates to ensure complete reconciliation
 */
async function processDateBatch(dates: string[]) {
  console.log(`Processing ${dates.length} dates for reconciliation...`);
  
  let successful = 0;
  let failed = 0;
  const errors: Array<{date: string, error: string}> = [];
  
  for (const date of dates) {
    try {
      console.log(`\nProcessing ${date}...`);
      
      // First check for discrepancies in the curtailment data itself and fix if needed
      await reconcileDay(date);
      
      // Then audit and fix the Bitcoin calculations
      const result = await auditAndFixBitcoinCalculations(date);
      
      if (result.success) {
        if (result.fixed) {
          console.log(`✅ ${date}: Fixed - ${result.message}`);
        } else {
          console.log(`✓ ${date}: Already complete - ${result.message}`);
        }
        successful++;
      } else {
        console.log(`❌ ${date}: Failed - ${result.message}`);
        errors.push({ date, error: result.message });
        failed++;
      }
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      console.error(`Error processing ${date}:`, errorMessage);
      errors.push({ date, error: errorMessage });
      failed++;
    }
  }
  
  return { successful, failed, errors };
}

/**
 * Fix all dates with missing or incomplete Bitcoin calculations
 */
async function fixAllMissingCalculations(limit?: number) {
  try {
    console.log("=== Starting Comprehensive Reconciliation ===\n");
    
    // Get initial reconciliation status
    console.log("Checking current reconciliation status...");
    const initialStatus = await getDetailedReconciliationStatus();
    
    console.log("\n=== Initial Status ===");
    console.log(`Curtailment Records: ${initialStatus.overall.totalCurtailmentRecords}`);
    console.log(`Unique Date-Period-Farm Combinations: ${initialStatus.overall.uniqueCombinations}`);
    console.log(`Unique Dates: ${initialStatus.overall.uniqueDates}`);
    console.log(`Bitcoin Calculations: ${initialStatus.overall.totalBitcoinCalculations}`);
    console.log(`Expected Calculations: ${initialStatus.overall.expectedCalculations}`);
    console.log(`Reconciliation: ${initialStatus.overall.reconciliationPercentage}%\n`);
    
    console.log("Calculations by Miner Model:");
    Object.entries(initialStatus.byModel).forEach(([model, data]) => {
      console.log(`- ${model}: ${data.count}/${data.expected} (${data.percentage}%)`);
    });
    
    // If we're already at 100%, we're done
    if (initialStatus.overall.reconciliationPercentage === 100) {
      console.log("\n✅ Already at 100% reconciliation! No action needed.");
      return {
        initialStatus,
        finalStatus: initialStatus,
        datesProcessed: 0,
        successful: 0,
        failed: 0
      };
    }
    
    // Find dates with missing calculations
    console.log("\nFinding dates with missing calculations...");
    const missingDates = await findDatesWithMissingCalculations();
    
    if (missingDates.length === 0) {
      console.log("No dates with missing calculations found!");
      return {
        initialStatus,
        finalStatus: initialStatus,
        datesProcessed: 0,
        successful: 0,
        failed: 0
      };
    }
    
    console.log(`\nFound ${missingDates.length} dates with missing calculations:`);
    missingDates.slice(0, 10).forEach(d => {
      console.log(`- ${d.date}: ${d.actual}/${d.expected} (${d.completionPercentage}%)`);
    });
    
    if (missingDates.length > 10) {
      console.log(`  ... and ${missingDates.length - 10} more dates`);
    }
    
    // Process dates with missing calculations (limited by parameter if provided)
    const datesToProcess = limit ? missingDates.slice(0, limit) : missingDates;
    console.log(`\nWill process ${datesToProcess.length} dates with missing calculations.`);
    
    // Process in batches
    const BATCH_SIZE = Math.min(MAX_BATCH_SIZE, datesToProcess.length);
    let totalSuccessful = 0;
    let totalFailed = 0;
    const allErrors: Array<{date: string, error: string}> = [];
    
    const dates = datesToProcess.map(d => d.date);
    
    for (let i = 0; i < dates.length; i += BATCH_SIZE) {
      const batch = dates.slice(i, i + BATCH_SIZE);
      const batchProgress = Math.round(((i + batch.length) / dates.length) * 100);
      
      console.log(`\n=== Processing Batch ${Math.floor(i/BATCH_SIZE) + 1} (${batchProgress}% complete) ===`);
      const batchResult = await processDateBatch(batch);
      
      totalSuccessful += batchResult.successful;
      totalFailed += batchResult.failed;
      allErrors.push(...batchResult.errors);
      
      // Sleep between batches to avoid overwhelming the system
      if (i + BATCH_SIZE < dates.length) {
        console.log(`Waiting before processing next batch...`);
        await new Promise(resolve => setTimeout(resolve, 3000));
      }
    }
    
    // Get final reconciliation status
    console.log("\nChecking final reconciliation status...");
    const finalStatus = await getDetailedReconciliationStatus();
    
    console.log("\n=== Final Status ===");
    console.log(`Curtailment Records: ${finalStatus.overall.totalCurtailmentRecords}`);
    console.log(`Bitcoin Calculations: ${finalStatus.overall.totalBitcoinCalculations}`);
    console.log(`Expected Calculations: ${finalStatus.overall.expectedCalculations}`);
    console.log(`Initial Reconciliation: ${initialStatus.overall.reconciliationPercentage}%`);
    console.log(`Final Reconciliation: ${finalStatus.overall.reconciliationPercentage}%`);
    console.log(`Improvement: ${(finalStatus.overall.reconciliationPercentage - initialStatus.overall.reconciliationPercentage).toFixed(2)}%`);
    
    console.log("\n=== Reconciliation Summary ===");
    console.log(`Dates Processed: ${dates.length}`);
    console.log(`Successful: ${totalSuccessful}`);
    console.log(`Failed: ${totalFailed}`);
    
    if (allErrors.length > 0) {
      console.log("\nErrors:");
      allErrors.slice(0, 10).forEach(e => {
        console.log(`- ${e.date}: ${e.error}`);
      });
      
      if (allErrors.length > 10) {
        console.log(`  ... and ${allErrors.length - 10} more errors`);
      }
    }
    
    return {
      initialStatus,
      finalStatus,
      datesProcessed: dates.length,
      successful: totalSuccessful,
      failed: totalFailed,
      errors: allErrors
    };
    
  } catch (error) {
    console.error("Error during reconciliation process:", error);
    throw error;
  }
}

/**
 * Fix reconciliation for a specific date
 */
async function fixDateReconciliation(date: string) {
  try {
    console.log(`=== Processing Reconciliation for ${date} ===\n`);
    
    // Get initial status for the date
    console.log(`Checking current status for ${date}...`);
    const initialDetails = await getDateReconciliationDetails(date);
    
    console.log(`\nCurtailment Data:`);
    console.log(`- Settlement Periods: ${initialDetails.curtailment.periodCount}`);
    console.log(`- Total Farm-Period Combinations: ${initialDetails.curtailment.totalFarmPeriods}`);
    
    console.log(`\nBitcoin Calculations:`);
    console.log(`- Current Reconciliation: ${initialDetails.calculations.reconciliationPercentage}%`);
    console.log(`- Calculations: ${initialDetails.calculations.totalCalculations}/${initialDetails.calculations.expectedCalculations}`);
    
    console.log(`\nBreakdown by Miner Model:`);
    Object.entries(initialDetails.calculations.byModel).forEach(([model, data]) => {
      const expectedForModel = initialDetails.curtailment.totalFarmPeriods;
      const percentage = expectedForModel > 0 ? Math.round((data.combinationCount * 100) / expectedForModel) : 0;
      console.log(`- ${model}: ${data.combinationCount}/${expectedForModel} combinations (${percentage}%)`);
    });
    
    // If any miner model has missing calculations, display them
    let hasMissing = false;
    Object.entries(initialDetails.calculations.missing).forEach(([model, missing]) => {
      if (missing.length > 0) {
        hasMissing = true;
        console.log(`\nMissing ${model} Calculations:`);
        missing.slice(0, 5).forEach(m => {
          console.log(`- Period ${m.period}, Farm ${m.farmId}`);
        });
        
        if (missing.length > 5) {
          console.log(`  ... and ${missing.length - 5} more missing combinations`);
        }
      }
    });
    
    if (!hasMissing && initialDetails.calculations.reconciliationPercentage === 100) {
      console.log(`\n✅ ${date} already has 100% reconciliation. No action needed.`);
      return {
        date,
        initialStatus: initialDetails,
        finalStatus: initialDetails,
        changed: false,
        message: "Already reconciled"
      };
    }
    
    // Process the date
    console.log(`\nProcessing ${date} for reconciliation...`);
    
    // First check for discrepancies in the curtailment data and fix if needed
    await reconcileDay(date);
    
    // Then audit and fix the Bitcoin calculations
    const result = await auditAndFixBitcoinCalculations(date);
    
    console.log(`\nReconciliation result: ${result.success ? "Success" : "Failed"}`);
    console.log(`Message: ${result.message}`);
    
    // Get final status
    console.log(`\nChecking final status for ${date}...`);
    const finalDetails = await getDateReconciliationDetails(date);
    
    const isFullyReconciled = finalDetails.calculations.reconciliationPercentage === 100;
    
    console.log(`\n=== Final Status ===`);
    console.log(`- Final Reconciliation: ${finalDetails.calculations.reconciliationPercentage}%`);
    console.log(`- Calculations: ${finalDetails.calculations.totalCalculations}/${finalDetails.calculations.expectedCalculations}`);
    console.log(`- Improvement: ${(finalDetails.calculations.reconciliationPercentage - initialDetails.calculations.reconciliationPercentage).toFixed(2)}%`);
    
    if (isFullyReconciled) {
      console.log(`\n✅ ${date} now has 100% reconciliation!`);
    } else {
      console.log(`\n⚠️ ${date} still needs additional reconciliation.`);
      
      // Show any remaining missing calculations
      Object.entries(finalDetails.calculations.missing).forEach(([model, missing]) => {
        if (missing.length > 0) {
          console.log(`\nStill Missing ${model} Calculations:`);
          missing.slice(0, 5).forEach(m => {
            console.log(`- Period ${m.period}, Farm ${m.farmId}`);
          });
          
          if (missing.length > 5) {
            console.log(`  ... and ${missing.length - 5} more missing combinations`);
          }
        }
      });
    }
    
    return {
      date,
      initialStatus: initialDetails,
      finalStatus: finalDetails,
      changed: result.fixed,
      message: result.message
    };
    
  } catch (error) {
    console.error(`Error processing ${date}:`, error);
    throw error;
  }
}

/**
 * Fix reconciliation for a range of dates
 */
async function fixDateRangeReconciliation(startDate: string, endDate: string) {
  try {
    console.log(`=== Processing Reconciliation for Range ${startDate} to ${endDate} ===\n`);
    
    // Get all dates in range
    const start = parseISO(startDate);
    const end = parseISO(endDate);
    
    const allDates = eachDayOfInterval({ start, end }).map(date => format(date, 'yyyy-MM-dd'));
    console.log(`Found ${allDates.length} dates in range`);
    
    // Get initial status for the range
    console.log(`Checking current reconciliation status for range...`);
    const initialStatus = await getDetailedReconciliationStatus(startDate, endDate);
    
    console.log(`\n=== Initial Status for ${startDate} to ${endDate} ===`);
    console.log(`Curtailment Records: ${initialStatus.overall.totalCurtailmentRecords}`);
    console.log(`Unique Date-Period-Farm Combinations: ${initialStatus.overall.uniqueCombinations}`);
    console.log(`Unique Dates with Curtailment: ${initialStatus.overall.uniqueDates}`);
    console.log(`Current Reconciliation: ${initialStatus.overall.reconciliationPercentage}%`);
    
    // Find dates with missing calculations in the range
    console.log(`\nFinding dates with missing calculations in range...`);
    const missingDates = await findDatesWithMissingCalculations(startDate, endDate);
    
    if (missingDates.length === 0) {
      console.log(`No dates with missing calculations found in range ${startDate} to ${endDate}!`);
      return {
        startDate,
        endDate,
        initialStatus,
        finalStatus: initialStatus,
        datesProcessed: 0,
        successful: 0,
        failed: 0
      };
    }
    
    console.log(`\nFound ${missingDates.length} dates with missing calculations in range:`);
    missingDates.slice(0, 10).forEach(d => {
      console.log(`- ${d.date}: ${d.actual}/${d.expected} (${d.completionPercentage}%)`);
    });
    
    if (missingDates.length > 10) {
      console.log(`  ... and ${missingDates.length - 10} more dates`);
    }
    
    // Process in batches
    const BATCH_SIZE = Math.min(MAX_BATCH_SIZE, missingDates.length);
    let totalSuccessful = 0;
    let totalFailed = 0;
    const allErrors: Array<{date: string, error: string}> = [];
    
    const dates = missingDates.map(d => d.date);
    
    for (let i = 0; i < dates.length; i += BATCH_SIZE) {
      const batch = dates.slice(i, i + BATCH_SIZE);
      const batchProgress = Math.round(((i + batch.length) / dates.length) * 100);
      
      console.log(`\n=== Processing Batch ${Math.floor(i/BATCH_SIZE) + 1} (${batchProgress}% complete) ===`);
      const batchResult = await processDateBatch(batch);
      
      totalSuccessful += batchResult.successful;
      totalFailed += batchResult.failed;
      allErrors.push(...batchResult.errors);
      
      // Sleep between batches
      if (i + BATCH_SIZE < dates.length) {
        console.log(`Waiting before processing next batch...`);
        await new Promise(resolve => setTimeout(resolve, 3000));
      }
    }
    
    // Get final status
    console.log(`\nChecking final reconciliation status for range...`);
    const finalStatus = await getDetailedReconciliationStatus(startDate, endDate);
    
    console.log(`\n=== Final Status for ${startDate} to ${endDate} ===`);
    console.log(`Curtailment Records: ${finalStatus.overall.totalCurtailmentRecords}`);
    console.log(`Bitcoin Calculations: ${finalStatus.overall.totalBitcoinCalculations}`);
    console.log(`Expected Calculations: ${finalStatus.overall.expectedCalculations}`);
    console.log(`Initial Reconciliation: ${initialStatus.overall.reconciliationPercentage}%`);
    console.log(`Final Reconciliation: ${finalStatus.overall.reconciliationPercentage}%`);
    console.log(`Improvement: ${(finalStatus.overall.reconciliationPercentage - initialStatus.overall.reconciliationPercentage).toFixed(2)}%`);
    
    console.log(`\n=== Range Reconciliation Summary ===`);
    console.log(`Dates Processed: ${dates.length}`);
    console.log(`Successful: ${totalSuccessful}`);
    console.log(`Failed: ${totalFailed}`);
    
    if (allErrors.length > 0) {
      console.log(`\nErrors:`);
      allErrors.slice(0, 10).forEach(e => {
        console.log(`- ${e.date}: ${e.error}`);
      });
      
      if (allErrors.length > 10) {
        console.log(`  ... and ${allErrors.length - 10} more errors`);
      }
    }
    
    return {
      startDate,
      endDate,
      initialStatus,
      finalStatus,
      datesProcessed: dates.length,
      successful: totalSuccessful,
      failed: totalFailed,
      errors: allErrors
    };
    
  } catch (error) {
    console.error(`Error processing date range ${startDate} to ${endDate}:`, error);
    throw error;
  }
}

/**
 * Main function to process commands
 */
async function main() {
  try {
    const command = process.argv[2]?.toLowerCase();
    const arg1 = process.argv[3];
    const arg2 = process.argv[4];
    
    if (!command) {
      console.log(`
Comprehensive Reconciliation Tool

This tool ensures 100% reconciliation between curtailment_records and historical_bitcoin_calculations tables.

Usage:
  npx tsx comprehensive_reconcile.ts status                 - Check overall reconciliation status
  npx tsx comprehensive_reconcile.ts check-date YYYY-MM-DD  - Check a specific date
  npx tsx comprehensive_reconcile.ts fix-date YYYY-MM-DD    - Fix a specific date
  npx tsx comprehensive_reconcile.ts fix-all [limit]        - Fix all dates with missing calculations (optional limit)
  npx tsx comprehensive_reconcile.ts fix-range START END    - Fix a date range (YYYY-MM-DD format)
      `);
      
      // Show current status by default
      console.log("Current reconciliation status:\n");
      const status = await getDetailedReconciliationStatus();
      
      console.log(`Curtailment Records: ${status.overall.totalCurtailmentRecords}`);
      console.log(`Unique Date-Period-Farm Combinations: ${status.overall.uniqueCombinations}`);
      console.log(`Current Reconciliation: ${status.overall.reconciliationPercentage}%`);
      console.log(`Expected Calculations: ${status.overall.expectedCalculations}`);
      console.log(`Actual Calculations: ${status.overall.totalBitcoinCalculations}`);
      
      return;
    }
    
    switch (command) {
      case "status":
        console.log("=== Comprehensive Reconciliation Status ===\n");
        const status = await getDetailedReconciliationStatus();
        
        console.log("Overall Status:");
        console.log(`Curtailment Records: ${status.overall.totalCurtailmentRecords}`);
        console.log(`Unique Combinations: ${status.overall.uniqueCombinations}`);
        console.log(`Bitcoin Calculations: ${status.overall.totalBitcoinCalculations}`);
        console.log(`Expected Calculations: ${status.overall.expectedCalculations}`);
        console.log(`Reconciliation Percentage: ${status.overall.reconciliationPercentage}%`);
        
        console.log("\nBy Miner Model:");
        Object.entries(status.byModel).forEach(([model, data]) => {
          console.log(`- ${model}: ${data.count}/${data.expected} (${data.percentage}%)`);
        });
        
        // Check for any missing calculations
        if (status.overall.reconciliationPercentage < 100) {
          console.log("\nDates with Missing Calculations:");
          const missingDates = await findDatesWithMissingCalculations(undefined, undefined, 10);
          
          missingDates.forEach(d => {
            console.log(`- ${d.date}: ${d.actual}/${d.expected} (${d.completionPercentage}%)`);
          });
          
          console.log(`\nUse 'fix-all' to process all missing calculations or 'fix-date' for specific dates.`);
        } else {
          console.log("\n✅ 100% reconciliation achieved! All records are in sync.");
        }
        break;
        
      case "check-date":
        if (!arg1 || !arg1.match(/^\d{4}-\d{2}-\d{2}$/)) {
          console.error("Please provide a date in YYYY-MM-DD format");
          process.exit(1);
        }
        
        await getDateReconciliationDetails(arg1).then(details => {
          console.log(`=== Reconciliation Details for ${arg1} ===\n`);
          
          console.log(`Curtailment Data:`);
          console.log(`- Settlement Periods: ${details.curtailment.periodCount}`);
          console.log(`- Total Farm-Period Combinations: ${details.curtailment.totalFarmPeriods}`);
          
          console.log(`\nBitcoin Calculations:`);
          console.log(`- Current Reconciliation: ${details.calculations.reconciliationPercentage}%`);
          console.log(`- Calculations: ${details.calculations.totalCalculations}/${details.calculations.expectedCalculations}`);
          
          console.log(`\nBreakdown by Miner Model:`);
          Object.entries(details.calculations.byModel).forEach(([model, data]) => {
            const expectedForModel = details.curtailment.totalFarmPeriods;
            const percentage = expectedForModel > 0 ? Math.round((data.combinationCount * 100) / expectedForModel) : 0;
            console.log(`- ${model}: ${data.combinationCount}/${expectedForModel} combinations (${percentage}%)`);
          });
          
          // If any miner model has missing calculations, display them
          let hasMissing = false;
          Object.entries(details.calculations.missing).forEach(([model, missing]) => {
            if (missing.length > 0) {
              hasMissing = true;
              console.log(`\nMissing ${model} Calculations:`);
              missing.slice(0, 5).forEach(m => {
                console.log(`- Period ${m.period}, Farm ${m.farmId}`);
              });
              
              if (missing.length > 5) {
                console.log(`  ... and ${missing.length - 5} more missing combinations`);
              }
            }
          });
          
          if (!hasMissing && details.calculations.reconciliationPercentage === 100) {
            console.log(`\n✅ ${arg1} has 100% reconciliation.`);
          } else {
            console.log(`\nUse 'fix-date ${arg1}' to fix missing calculations.`);
          }
        });
        break;
        
      case "fix-date":
        if (!arg1 || !arg1.match(/^\d{4}-\d{2}-\d{2}$/)) {
          console.error("Please provide a date in YYYY-MM-DD format");
          process.exit(1);
        }
        
        await fixDateReconciliation(arg1);
        break;
        
      case "fix-all":
        const limit = arg1 ? parseInt(arg1, 10) : undefined;
        await fixAllMissingCalculations(limit);
        break;
        
      case "fix-range":
        if (!arg1 || !arg1.match(/^\d{4}-\d{2}-\d{2}$/) || !arg2 || !arg2.match(/^\d{4}-\d{2}-\d{2}$/)) {
          console.error("Please provide start and end dates in YYYY-MM-DD format");
          process.exit(1);
        }
        
        await fixDateRangeReconciliation(arg1, arg2);
        break;
        
      default:
        console.error(`Unknown command: ${command}`);
        process.exit(1);
    }
    
  } catch (error) {
    console.error("Error executing command:", error);
    process.exit(1);
  }
}

// Run the main function if this script is executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
  main()
    .then(() => {
      console.log("\n=== Comprehensive Reconciliation Tool Complete ===");
      process.exit(0);
    })
    .catch(error => {
      console.error("Fatal error:", error);
      process.exit(1);
    });
}

export {
  getDetailedReconciliationStatus,
  findDatesWithMissingCalculations,
  getDateReconciliationDetails,
  fixDateReconciliation,
  fixDateRangeReconciliation,
  fixAllMissingCalculations
};