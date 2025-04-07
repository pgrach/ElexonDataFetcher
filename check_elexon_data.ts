/**
 * Elexon Data Verification Script
 * 
 * This script checks curtailment_records against Elexon API data to verify data consistency.
 * It offers multiple sampling strategies to efficiently check data integrity while
 * minimizing API calls and avoiding timeouts.
 * 
 * Basic usage:
 *   npx tsx check_elexon_data.ts [date] [sampling-method]
 * 
 * Examples:
 *   npx tsx check_elexon_data.ts                     # Checks today's data using progressive sampling
 *   npx tsx check_elexon_data.ts 2025-03-25          # Checks specific date using progressive sampling
 *   npx tsx check_elexon_data.ts 2025-03-25 random   # Uses random sampling of 10 periods
 *   npx tsx check_elexon_data.ts 2025-03-25 fixed    # Uses fixed key periods (1, 12, 24, 36, 48)
 *   npx tsx check_elexon_data.ts 2025-03-25 full     # Checks all 48 periods (warning: may hit API limits)
 * 
 * Sampling methods:
 *   progressive (p) - Starts with key periods, expands if issues found (default)
 *   random (r)      - Checks 10 random periods for broader coverage
 *   fixed (f)       - Only checks 5 fixed key periods (1, 12, 24, 36, 48)
 *   full (a)        - Checks all 48 periods (warning: may hit API rate limits)
 * 
 * If discrepancies are found, the script will provide commands to run for data reingestion:
 * 1. Reingest curtailment data: npx tsx server/services/curtailment.ts process-date <DATE>
 * 2. Recalculate Bitcoin calculations: npx tsx server/services/bitcoinService.ts process-date <DATE> <MODEL>
 * 3. Update monthly summaries: npx tsx server/services/bitcoinService.ts recalculate-monthly <YEAR-MONTH>
 * 4. Update yearly summaries: npx tsx server/services/bitcoinService.ts recalculate-yearly <YEAR>
 */

import { db } from "./db";
import { curtailmentRecords, dailySummaries } from "./db/schema";
import { eq, and, sql } from "drizzle-orm";
import { fetchBidsOffers } from "./server/services/elexon";
import { processDailyCurtailment } from "./server/services/curtailment";
import { 
  processSingleDay, 
  calculateMonthlyBitcoinSummary, 
  manualUpdateYearlyBitcoinSummary 
} from "./server/services/bitcoinService";

// Configuration
// Default target date, but can be overridden via command line argument
const DEFAULT_TARGET_DATE = '2025-03-22';
// Get target date from command line args if provided, otherwise use default
const TARGET_DATE = process.argv[2] || DEFAULT_TARGET_DATE;

// Options for sampling settlement periods to avoid timeouts
// Option 1: Fixed key periods (fastest but may miss issues)
const KEY_PERIODS = [1, 12, 24, 36, 48];

// Option 2: Randomized sampling for better coverage (more reliable but slower)
function getRandomPeriods(count: number = 10): number[] {
  const periods = new Set<number>();
  
  // Always include period 1 and 48 (first and last)
  periods.add(1);
  periods.add(48);
  
  // Add random periods until we reach the desired count
  while (periods.size < count) {
    const randomPeriod = Math.floor(Math.random() * 46) + 2; // Random period between 2-47
    periods.add(randomPeriod);
  }
  
  // Return as sorted array
  return Array.from(periods).sort((a, b) => a - b);
}

// Option 3: Progressive sampling (check key periods first, then more if issues found)
// This will be implemented in the verifyOverallData function

// Default to using randomized periods for better coverage
const SAMPLE_PERIODS = getRandomPeriods(10); 
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S']; // Standard miner models

// Simple date validation function
function isValidDate(dateStr: string): boolean {
  const regex = /^\d{4}-\d{2}-\d{2}$/;
  if (!regex.test(dateStr)) return false;
  
  const date = new Date(dateStr);
  return date instanceof Date && !isNaN(date.getTime());
}

/**
 * Check a specific settlement period against Elexon API
 */
async function checkPeriod(period: number): Promise<{
  mismatched: boolean;
  apiRecords: number;
  dbRecords: number;
  apiVolume: number;
  dbVolume: number;
  apiPayment: number;
  dbPayment: number;
}> {
  try {
    console.log(`Checking period ${period}...`);
    
    // Get data from Elexon API
    const apiData = await fetchBidsOffers(TARGET_DATE, period);
    
    // Get data from database
    const dbData = await db
      .select({
        recordCount: sql<number>`COUNT(*)`,
        totalVolume: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
        totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(
        and(
          eq(curtailmentRecords.settlementDate, TARGET_DATE),
          eq(curtailmentRecords.settlementPeriod, period)
        )
      );
    
    // Calculate API totals
    const apiVolume = apiData.reduce((sum, record) => sum + Math.abs(record.volume), 0);
    // In our database, payments are stored with a negative sign (indicating money paid out to farms)
    // This produces a negative value which should match what's in the database
    const apiPayment = -1 * Math.abs(apiData.reduce((sum, record) => sum + (Math.abs(record.volume) * record.originalPrice), 0));
    
    // Extract DB totals
    const dbRecords = Number(dbData[0]?.recordCount || 0); // Ensure we have a valid number
    const dbVolume = parseFloat(dbData[0]?.totalVolume || '0');
    const dbPayment = parseFloat(dbData[0]?.totalPayment || '0');
    
    // Check for mismatch with a small tolerance for floating point differences
    const volumeDiff = Math.abs(apiVolume - dbVolume);
    const paymentDiff = Math.abs(apiPayment - dbPayment);
    const recordsDiff = apiData.length !== dbRecords;
    
    const mismatched = volumeDiff > 0.01 || paymentDiff > 0.01 || recordsDiff;
    
    if (mismatched) {
      console.log(`Period ${period} has differences:`);
      if (recordsDiff) console.log(`- Records: API=${apiData.length}, DB=${dbRecords}`);
      if (volumeDiff > 0.01) console.log(`- Volume: API=${apiVolume.toFixed(2)}, DB=${dbVolume.toFixed(2)}, Diff=${volumeDiff.toFixed(2)}`);
      if (paymentDiff > 0.01) console.log(`- Payment: API=${apiPayment.toFixed(2)}, DB=${dbPayment.toFixed(2)}, Diff=${paymentDiff.toFixed(2)}`);
    } else {
      console.log(`Period ${period} is consistent with Elexon data.`);
    }
    
    return {
      mismatched,
      apiRecords: apiData.length,
      dbRecords,
      apiVolume,
      dbVolume,
      apiPayment,
      dbPayment
    };
  } catch (error) {
    console.error(`Error checking period ${period}:`, error);
    return {
      mismatched: true, // Assume mismatch on error to trigger reingestion
      apiRecords: 0,
      dbRecords: 0,
      apiVolume: 0,
      dbVolume: 0,
      apiPayment: 0,
      dbPayment: 0
    };
  }
}

/**
 * Verify overall data and provide a summary
 * @param useProgressiveSampling If true, will start with key periods and expand to more if issues are found
 * @param sampleSize Number of periods to sample if not using progressive sampling
 */
async function verifyOverallData(
  useProgressiveSampling: boolean = false,
  sampleSize: number = 10
): Promise<{
  totalMismatches: number;
  totalApiRecords: number;
  totalDbRecords: number;
  mismatchedPeriods: number[];
  sampledPeriods: number[];
  samplingMethod: string;
}> {
  let periodsToCheck: number[] = [];
  let samplingMethod = '';
  
  if (useProgressiveSampling) {
    // Step 1: Start with key periods for quick check
    samplingMethod = 'progressive';
    console.log('Using progressive sampling approach - starting with key periods...');
    
    const keyResults = await Promise.all(KEY_PERIODS.map(checkPeriod));
    const keyMismatches = KEY_PERIODS.filter((period, i) => keyResults[i].mismatched);
    
    if (keyMismatches.length > 0) {
      // Issues detected in key periods, expand sampling for better coverage
      console.log(`\nFound issues in ${keyMismatches.length} key periods. Expanding to random sampling for better coverage...`);
      
      // Get additional random periods (excluding already checked key periods)
      const additionalPeriods = getAdditionalRandomPeriods(15, KEY_PERIODS); 
      console.log(`Checking ${additionalPeriods.length} additional periods: ${additionalPeriods.join(', ')}...`);
      
      // Check additional periods
      const additionalResults = await Promise.all(additionalPeriods.map(checkPeriod));
      
      // Combine results
      periodsToCheck = [...KEY_PERIODS, ...additionalPeriods];
      const results = [...keyResults, ...additionalResults];
      
      const totalMismatches = results.filter(r => r.mismatched).length;
      const totalApiRecords = results.reduce((sum, r) => sum + r.apiRecords, 0);
      const totalDbRecords = results.reduce((sum, r) => sum + r.dbRecords, 0);
      const mismatchedPeriods = periodsToCheck.filter((period, i) => results[i].mismatched);
      
      return {
        totalMismatches,
        totalApiRecords,
        totalDbRecords,
        mismatchedPeriods,
        sampledPeriods: periodsToCheck,
        samplingMethod: 'progressive (expanded)'
      };
    } else {
      // No issues in key periods, return key period results
      const totalApiRecords = keyResults.reduce((sum, r) => sum + r.apiRecords, 0);
      const totalDbRecords = keyResults.reduce((sum, r) => sum + r.dbRecords, 0);
      
      return {
        totalMismatches: 0,
        totalApiRecords,
        totalDbRecords,
        mismatchedPeriods: [],
        sampledPeriods: KEY_PERIODS,
        samplingMethod: 'progressive (key periods only)'
      };
    }
  } else {
    // Just use the provided sample periods (random or fixed)
    samplingMethod = 'random sampling';
    periodsToCheck = SAMPLE_PERIODS;
    
    const results = await Promise.all(periodsToCheck.map(checkPeriod));
    
    const totalMismatches = results.filter(r => r.mismatched).length;
    const totalApiRecords = results.reduce((sum, r) => sum + r.apiRecords, 0);
    const totalDbRecords = results.reduce((sum, r) => sum + r.dbRecords, 0);
    const mismatchedPeriods = periodsToCheck.filter((period, i) => results[i].mismatched);
    
    return {
      totalMismatches,
      totalApiRecords,
      totalDbRecords,
      mismatchedPeriods,
      sampledPeriods: periodsToCheck,
      samplingMethod
    };
  }
}

/**
 * Get additional random periods excluding already checked periods
 */
function getAdditionalRandomPeriods(count: number, excludePeriods: number[]): number[] {
  const additionalPeriods = new Set<number>();
  const excludeSet = new Set(excludePeriods);
  
  // Create an array of all possible settlement periods excluding those already checked
  const availablePeriods = Array.from({ length: 48 }, (_, i) => i + 1)
    .filter(p => !excludeSet.has(p));
  
  // Shuffle the array using Fisher-Yates algorithm
  for (let i = availablePeriods.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [availablePeriods[i], availablePeriods[j]] = [availablePeriods[j], availablePeriods[i]];
  }
  
  // Take the first 'count' elements, or all if there are fewer than 'count'
  return availablePeriods.slice(0, Math.min(count, availablePeriods.length));
}

/**
 * Update all cascade dependencies after reingestion
 */
async function updateCascadeDependencies() {
  try {
    console.log('Updating Bitcoin calculations for all miner models...');
    
    // Process Bitcoin calculations for all miner models
    for (const minerModel of MINER_MODELS) {
      await processSingleDay(TARGET_DATE, minerModel);
      console.log(`✓ Updated Bitcoin calculations for ${minerModel}`);
    }
    
    // Update monthly Bitcoin summary
    const yearMonth = TARGET_DATE.substring(0, 7); // YYYY-MM format
    console.log(`\nUpdating monthly Bitcoin summaries for ${yearMonth}...`);
    
    for (const minerModel of MINER_MODELS) {
      await calculateMonthlyBitcoinSummary(yearMonth, minerModel);
      console.log(`✓ Updated monthly summary for ${minerModel}`);
    }
    
    // Update yearly Bitcoin summary
    const year = TARGET_DATE.substring(0, 4); // YYYY format
    console.log(`\nUpdating yearly Bitcoin summaries for ${year}...`);
    await manualUpdateYearlyBitcoinSummary(year);
    console.log(`✓ Updated yearly summaries`);
    
    console.log('\nAll cascade dependencies updated successfully!');
  } catch (error) {
    console.error('Error updating cascade dependencies:', error);
    throw error;
  }
}

/**
 * Display the final verification statistics
 */
async function displayFinalStats() {
  try {
    // Get curtailment records stats
    const curtailmentStats = await db
      .select({
        recordCount: sql<number>`COUNT(*)`,
        periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
        farmCount: sql<number>`COUNT(DISTINCT farm_id)`,
        totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
        totalPayment: sql<string>`SUM(payment::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
      
    // Get daily summary stats  
    const summary = await db
      .select()
      .from(dailySummaries)
      .where(eq(dailySummaries.summaryDate, TARGET_DATE));
      
    console.log('\n=== Final Verification Results ===');
    console.log(`Date: ${TARGET_DATE}`);
    console.log(`Records: ${curtailmentStats[0]?.recordCount || 0}`);
    console.log(`Periods: ${curtailmentStats[0]?.periodCount || 0} of 48`);
    console.log(`Farms: ${curtailmentStats[0]?.farmCount || 0}`);
    console.log(`Total Volume: ${Number(curtailmentStats[0]?.totalVolume || 0).toFixed(2)} MWh`);
    console.log(`Total Payment: £${Number(curtailmentStats[0]?.totalPayment || 0).toFixed(2)}`);
    
    if (summary.length > 0) {
      console.log('\nDaily Summary:');
      console.log(`Energy: ${Number(summary[0].totalCurtailedEnergy).toFixed(2)} MWh`);
      console.log(`Payment: £${Number(summary[0].totalPayment).toFixed(2)}`);
      
      // Check if daily summary is consistent with curtailment records
      const energyDiff = Math.abs(Number(curtailmentStats[0]?.totalVolume || 0) - Number(summary[0].totalCurtailedEnergy));
      const paymentDiff = Math.abs(Number(curtailmentStats[0]?.totalPayment || 0) - Number(summary[0].totalPayment));
      
      if (energyDiff > 0.01 || paymentDiff > 0.01) {
        console.log('\nWarning: Daily summary does not match curtailment records!');
        console.log(`Energy difference: ${energyDiff.toFixed(2)} MWh`);
        console.log(`Payment difference: £${paymentDiff.toFixed(2)}`);
      } else {
        console.log('\nDaily summary is consistent with curtailment records.');
      }
    } else {
      console.log('\nNo daily summary found!');
    }
  } catch (error) {
    console.error('Error displaying final stats:', error);
  }
}

/**
 * Main execution function
 */
async function main() {
  console.log(`\n=== ELEXON DATA VERIFICATION ===`);
  
  // Validate the date format
  if (!isValidDate(TARGET_DATE)) {
    console.error(`Error: Invalid date format "${TARGET_DATE}". Please use YYYY-MM-DD format.`);
    console.log(`Example usage: npx tsx check_elexon_data.ts 2025-03-22`);
    process.exit(1);
  }
  
  console.log(`Target Date: ${TARGET_DATE}`);
  
  // Parse command line arguments for sampling method
  const samplingMethod = process.argv[3] || 'progressive';
  let verificationResults;
  
  try {
    // Determine which sampling approach to use
    if (samplingMethod === 'progressive' || samplingMethod === 'p') {
      // Use progressive sampling (key periods first, then expand if issues found)
      verificationResults = await verifyOverallData(true);
    } else if (samplingMethod === 'random' || samplingMethod === 'r') {
      // Use random sampling (10 random periods)
      console.log(`Using random sampling of 10 periods...`);
      verificationResults = await verifyOverallData(false, 10);
    } else if (samplingMethod === 'fixed' || samplingMethod === 'f') {
      // Use fixed key periods only
      console.log(`Using fixed key periods: ${KEY_PERIODS.join(', ')}...`);
      const SAMPLE_PERIODS_BACKUP = [...SAMPLE_PERIODS];
      SAMPLE_PERIODS.length = 0;
      SAMPLE_PERIODS.push(...KEY_PERIODS);
      verificationResults = await verifyOverallData(false);
      SAMPLE_PERIODS.length = 0;
      SAMPLE_PERIODS.push(...SAMPLE_PERIODS_BACKUP);
    } else if (samplingMethod === 'full' || samplingMethod === 'a') {
      // Check all 48 periods (may take a long time and hit API limits)
      console.log(`Using full verification of all 48 periods (this may take a while and hit API limits)...`);
      const allPeriods = Array.from({ length: 48 }, (_, i) => i + 1);
      const SAMPLE_PERIODS_BACKUP = [...SAMPLE_PERIODS];
      SAMPLE_PERIODS.length = 0;
      SAMPLE_PERIODS.push(...allPeriods);
      verificationResults = await verifyOverallData(false);
      SAMPLE_PERIODS.length = 0;
      SAMPLE_PERIODS.push(...SAMPLE_PERIODS_BACKUP);
    } else {
      // Default to progressive
      console.log(`Unrecognized sampling method '${samplingMethod}'. Using progressive sampling...`);
      verificationResults = await verifyOverallData(true);
    }
    
    console.log(`\n=== Verification Summary ===`);
    console.log(`Sampling Method: ${verificationResults.samplingMethod}`);
    console.log(`Periods Checked: ${verificationResults.sampledPeriods.length} of 48 (${verificationResults.sampledPeriods.join(', ')})`);
    console.log(`Total API Records: ${verificationResults.totalApiRecords}`);
    console.log(`Total DB Records: ${verificationResults.totalDbRecords}`);
    console.log(`Mismatched Periods: ${verificationResults.totalMismatches} of ${verificationResults.sampledPeriods.length}`);
    
    if (verificationResults.totalMismatches > 0) {
      console.log(`\nMismatched Settlement Periods: ${verificationResults.mismatchedPeriods.join(', ')}`);
      
      // Display current data stats
      await displayFinalStats();
      
      const percentMissing = Math.round((verificationResults.mismatchedPeriods.length / verificationResults.sampledPeriods.length) * 100);
      
      console.log(`\n=== Data Quality Assessment ===`);
      if (percentMissing > 75) {
        console.log(`Critical: ${percentMissing}% of checked periods have missing or inconsistent data`);
      } else if (percentMissing > 50) {
        console.log(`Major: ${percentMissing}% of checked periods have missing or inconsistent data`);
      } else if (percentMissing > 25) {
        console.log(`Moderate: ${percentMissing}% of checked periods have missing or inconsistent data`);
      } else {
        console.log(`Minor: ${percentMissing}% of checked periods have missing or inconsistent data`);
      }
      
      console.log(`\n=== Reingestion Required ===`);
      console.log(`Run the following commands to update the data:`);
      console.log(`1. npx tsx server/services/curtailment.ts process-date ${TARGET_DATE}`);
      console.log(`2. For each model (S19J_PRO, S9, M20S):`);
      console.log(`   npx tsx server/services/bitcoinService.ts process-date ${TARGET_DATE} MODEL_NAME`);
      console.log(`3. npx tsx server/services/bitcoinService.ts recalculate-monthly ${TARGET_DATE.substring(0, 7)}`);
      console.log(`4. npx tsx server/services/bitcoinService.ts recalculate-yearly ${TARGET_DATE.substring(0, 4)}`);
    } else {
      console.log(`\nAll sampled periods match Elexon API data.`);
      console.log(`This suggests that the data is consistent, but a full verification would be needed for complete confidence.`);
      await displayFinalStats();
    }
  } catch (error) {
    console.error('Error during verification process:', error);
    process.exit(1);
  }
}

// Execute the script
main()
  .then(() => {
    console.log('\nScript completed successfully!');
    process.exit(0);
  })
  .catch(error => {
    console.error('Script failed:', error);
    process.exit(1);
  });