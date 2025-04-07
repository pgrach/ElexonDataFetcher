/**
 * Data Verification and Repair Utility
 * 
 * This utility provides a comprehensive approach to verifying and fixing data integrity
 * issues for specific dates in the system. It combines verification with automatic
 * repair capabilities.
 * 
 * Features:
 * 1. Verification of curtailment records against Elexon API data
 * 2. Automatic reprocessing of incomplete or inconsistent data
 * 3. Full cascade updates for Bitcoin calculations and summaries
 * 4. Detailed logs and summaries of actions taken
 * 
 * Usage:
 *   npx tsx verify_and_fix_data.ts [date] [action] [sampling-method]
 * 
 * Examples:
 *   npx tsx verify_and_fix_data.ts                        # Verifies today's data using progressive sampling
 *   npx tsx verify_and_fix_data.ts 2025-04-01             # Verifies specific date using progressive sampling
 *   npx tsx verify_and_fix_data.ts 2025-04-01 verify      # Only verifies without fixing
 *   npx tsx verify_and_fix_data.ts 2025-04-01 fix         # Verifies and fixes if needed
 *   npx tsx verify_and_fix_data.ts 2025-04-01 fix random  # Uses random sampling instead of progressive
 * 
 * Actions:
 *   verify        - Only performs verification without fixing (default)
 *   fix           - Verifies and automatically fixes if issues are found
 *   force-fix     - Skips verification and forces a complete reprocessing of the date
 * 
 * Sampling methods (for verification):
 *   progressive   - Starts with key periods, expands if issues found (default)
 *   random        - Checks 10 random periods for broader coverage
 *   fixed         - Only checks 5 fixed key periods (1, 12, 24, 36, 48)
 *   full          - Checks all 48 periods (warning: may hit API rate limits)
 */

import { db } from './db';
import { curtailmentRecords, dailySummaries } from './db/schema';
import { eq, sql } from 'drizzle-orm';
import { fetchBidsOffers } from './server/services/elexon';
import { processAllPeriods } from './process_all_periods';
import { processFullCascade } from './process_bitcoin_optimized';
import { format } from 'date-fns';
import fs from 'fs/promises';
import path from 'path';

// Configuration
const API_RATE_LIMIT_DELAY_MS = 500;
const LOG_DIR = 'logs';

// Define types for verification results
interface VerificationResult {
  isPassing: boolean;
  totalChecked: number;
  totalMismatch: number;
  mismatchedPeriods: number[];
  missingPeriods: number[];
  details: {
    period: number;
    status: 'match' | 'mismatch' | 'missing' | 'error';
    dbCount?: number;
    apiCount?: number;
    dbVolume?: number;
    apiVolume?: number;
    dbPayment?: number;
    apiPayment?: number;
    error?: string;
  }[];
}

interface DatabaseSummary {
  recordCount: number;
  periodsCovered: number;
  totalVolume: number;
  totalPayment: number;
  periodsPresent: number[];
}

interface RepairResult {
  date: string;
  initialState: DatabaseSummary;
  verificationResult: VerificationResult;
  repairNeeded: boolean;
  repairSuccess: boolean;
  finalState?: DatabaseSummary;
  curtailmentResult?: any;
  bitcoinResult?: any;
  error?: string;
}

// BMU mapping cache
let bmuMapping: Record<string, { name: string, leadParty: string }> | null = null;
let windFarmIds: Set<string> | null = null;

/**
 * Load the BMU mapping file once
 */
async function loadBmuMapping(): Promise<Record<string, { name: string, leadParty: string }>> {
  if (bmuMapping) return bmuMapping;
  
  try {
    console.log('Loading BMU mapping from data/bmu_mapping.json...');
    const mappingFile = await fs.readFile(path.join('data', 'bmu_mapping.json'), 'utf-8');
    bmuMapping = JSON.parse(mappingFile);
    console.log(`Loaded ${Object.keys(bmuMapping || {}).length} BMU mappings`);
    return bmuMapping || {};
  } catch (error) {
    console.error('Error loading BMU mapping:', error);
    bmuMapping = {};
    return {};
  }
}

/**
 * Filter for valid wind farm BMUs
 */
async function loadWindFarmIds(): Promise<Set<string>> {
  if (windFarmIds) return windFarmIds;
  
  const mapping = await loadBmuMapping();
  windFarmIds = new Set<string>();
  
  for (const [id, details] of Object.entries(mapping)) {
    windFarmIds.add(id);
  }
  
  console.log(`Loaded ${windFarmIds.size} wind farm BMU IDs`);
  return windFarmIds;
}

/**
 * Get database summary for a specific date
 */
async function getDatabaseSummary(date: string): Promise<DatabaseSummary> {
  try {
    // Get all records for the date
    const records = await db.query.curtailmentRecords.findMany({
      where: eq(curtailmentRecords.settlementDate, date)
    });
    
    if (records.length === 0) {
      return {
        recordCount: 0,
        periodsCovered: 0,
        totalVolume: 0,
        totalPayment: 0,
        periodsPresent: []
      };
    }
    
    // Calculate summary
    const periodSet = new Set<number>();
    let totalVolume = 0;
    let totalPayment = 0;
    
    for (const record of records) {
      periodSet.add(record.settlementPeriod);
      totalVolume += Math.abs(Number(record.volume));
      totalPayment += Math.abs(Number(record.payment));
    }
    
    const periodsPresent = Array.from(periodSet).sort((a, b) => a - b);
    
    return {
      recordCount: records.length,
      periodsCovered: periodSet.size,
      totalVolume,
      totalPayment,
      periodsPresent
    };
  } catch (error) {
    console.error(`Error getting database summary for ${date}:`, error);
    throw error;
  }
}

/**
 * Check a specific settlement period against Elexon API
 */
async function checkPeriod(date: string, period: number): Promise<{
  status: 'match' | 'mismatch' | 'missing' | 'error';
  dbCount?: number;
  apiCount?: number;
  dbVolume?: number;
  apiVolume?: number;
  dbPayment?: number;
  apiPayment?: number;
  error?: string;
}> {
  try {
    // Get records from database
    const dbRecords = await db.query.curtailmentRecords.findMany({
      where: (fields) => 
        sql`${fields.settlementDate} = ${date} AND ${fields.settlementPeriod} = ${period}`
    });
    
    // Get records from API
    const validWindFarmIds = await loadWindFarmIds();
    const apiRecords = await fetchBidsOffers(date, period);
    
    if (!apiRecords) {
      return {
        status: 'error',
        error: 'API returned no data or an error occurred'
      };
    }
    
    // Filter API records based on the same criteria used in processing
    const validApiRecords = apiRecords.filter(record => 
      record.volume < 0 &&
      (record.soFlag || record.cadlFlag) &&
      validWindFarmIds.has(record.id)
    );
    
    // Calculate totals for API records
    const apiVolume = validApiRecords.reduce((sum, r) => sum + Math.abs(r.volume), 0);
    const apiPayment = validApiRecords.reduce((sum, r) => sum + Math.abs(r.volume) * r.originalPrice * -1, 0);
    
    // Calculate totals for DB records
    const dbVolume = dbRecords.reduce((sum, r) => sum + Math.abs(Number(r.volume)), 0);
    const dbPayment = dbRecords.reduce((sum, r) => sum + Math.abs(Number(r.payment)), 0);
    
    // Handle missing data
    if (validApiRecords.length === 0 && dbRecords.length === 0) {
      return {
        status: 'match',
        dbCount: 0,
        apiCount: 0,
        dbVolume: 0,
        apiVolume: 0,
        dbPayment: 0,
        apiPayment: 0
      };
    }
    
    if (validApiRecords.length > 0 && dbRecords.length === 0) {
      return {
        status: 'missing',
        dbCount: 0,
        apiCount: validApiRecords.length,
        dbVolume: 0,
        apiVolume,
        dbPayment: 0,
        apiPayment
      };
    }
    
    // Compare counts first
    const countMatches = dbRecords.length === validApiRecords.length;
    
    // Allow for small rounding differences in volumes and payments (0.1% tolerance)
    const volumeMatches = Math.abs(dbVolume - apiVolume) < (apiVolume * 0.001);
    const paymentMatches = Math.abs(dbPayment - apiPayment) < (apiPayment * 0.001);
    
    return {
      status: (countMatches && volumeMatches && paymentMatches) ? 'match' : 'mismatch',
      dbCount: dbRecords.length,
      apiCount: validApiRecords.length,
      dbVolume,
      apiVolume,
      dbPayment,
      apiPayment
    };
  } catch (error) {
    console.error(`Error checking period ${period} for ${date}:`, error);
    return {
      status: 'error',
      error: error.message || 'Unknown error'
    };
  }
}

/**
 * Get a list of periods to check based on the sampling method
 */
function getPeriodsToCheck(method: string): number[] {
  switch (method) {
    case 'full':
    case 'a':
      return Array.from({ length: 48 }, (_, i) => i + 1);
    
    case 'fixed':
    case 'f':
      return [1, 12, 24, 36, 48];
    
    case 'random':
    case 'r':
      return getRandomPeriods(10);
    
    case 'progressive':
    case 'p':
    default:
      return [1, 12, 24, 36, 48]; // Progressive starts with key periods
  }
}

/**
 * Get random periods for sampling
 */
function getRandomPeriods(count: number = 10): number[] {
  const periods = Array.from({ length: 48 }, (_, i) => i + 1);
  const shuffled = [...periods].sort(() => 0.5 - Math.random());
  return shuffled.slice(0, count);
}

/**
 * Verify data for a specific date
 */
async function verifyData(date: string, samplingMethod: string = 'progressive'): Promise<VerificationResult> {
  console.log(`\n=== Verifying Data for ${date} ===\n`);
  
  // Get the periods to check based on sampling method
  let periodsToCheck = getPeriodsToCheck(samplingMethod);
  console.log(`Using ${samplingMethod} sampling method to check ${periodsToCheck.length} periods`);
  
  const result: VerificationResult = {
    isPassing: true,
    totalChecked: 0,
    totalMismatch: 0,
    mismatchedPeriods: [],
    missingPeriods: [],
    details: []
  };
  
  // Phase 1: Check initial periods
  console.log('\n--- Phase 1: Initial Data Check ---\n');
  for (const period of periodsToCheck) {
    console.log(`Checking period ${period}...`);
    
    // Add a small delay between API calls to avoid rate limiting
    if (result.totalChecked > 0) {
      await new Promise(resolve => setTimeout(resolve, API_RATE_LIMIT_DELAY_MS));
    }
    
    const checkResult = await checkPeriod(date, period);
    result.totalChecked++;
    
    if (checkResult.status === 'mismatch') {
      result.totalMismatch++;
      result.mismatchedPeriods.push(period);
      result.isPassing = false;
    } else if (checkResult.status === 'missing' && checkResult.apiCount && checkResult.apiCount > 0) {
      result.missingPeriods.push(period);
      result.isPassing = false;
    }
    
    result.details.push({
      period,
      ...checkResult
    });
    
    // Log the result
    if (checkResult.status === 'match') {
      console.log(`✅ Period ${period}: Match (${checkResult.dbCount} records, ${checkResult.dbVolume?.toFixed(2)} MWh)`);
    } else if (checkResult.status === 'mismatch') {
      console.log(`❌ Period ${period}: Mismatch!`);
      console.log(`   DB: ${checkResult.dbCount} records, ${checkResult.dbVolume?.toFixed(2)} MWh, £${checkResult.dbPayment?.toFixed(2)}`);
      console.log(`   API: ${checkResult.apiCount} records, ${checkResult.apiVolume?.toFixed(2)} MWh, £${checkResult.apiPayment?.toFixed(2)}`);
    } else if (checkResult.status === 'missing') {
      console.log(`⚠️ Period ${period}: Missing DB records!`);
      console.log(`   API has ${checkResult.apiCount} records (${checkResult.apiVolume?.toFixed(2)} MWh, £${checkResult.apiPayment?.toFixed(2)})`);
    } else if (checkResult.status === 'error') {
      console.log(`❓ Period ${period}: Error: ${checkResult.error}`);
    }
  }
  
  // Phase 2: If using progressive sampling and issues were found, expand to more periods
  if (samplingMethod === 'progressive' && !result.isPassing) {
    console.log('\n--- Phase 2: Expanded Data Check ---\n');
    console.log('Issues found, checking additional random periods...');
    
    // Get 10 additional random periods, excluding those already checked
    const additionalPeriods = getAdditionalRandomPeriods(10, periodsToCheck);
    
    for (const period of additionalPeriods) {
      console.log(`Checking additional period ${period}...`);
      
      // Add a small delay between API calls
      await new Promise(resolve => setTimeout(resolve, API_RATE_LIMIT_DELAY_MS));
      
      const checkResult = await checkPeriod(date, period);
      result.totalChecked++;
      
      if (checkResult.status === 'mismatch') {
        result.totalMismatch++;
        result.mismatchedPeriods.push(period);
      } else if (checkResult.status === 'missing' && checkResult.apiCount && checkResult.apiCount > 0) {
        result.missingPeriods.push(period);
      }
      
      result.details.push({
        period,
        ...checkResult
      });
      
      // Log the result
      if (checkResult.status === 'match') {
        console.log(`✅ Period ${period}: Match (${checkResult.dbCount} records, ${checkResult.dbVolume?.toFixed(2)} MWh)`);
      } else if (checkResult.status === 'mismatch') {
        console.log(`❌ Period ${period}: Mismatch!`);
        console.log(`   DB: ${checkResult.dbCount} records, ${checkResult.dbVolume?.toFixed(2)} MWh, £${checkResult.dbPayment?.toFixed(2)}`);
        console.log(`   API: ${checkResult.apiCount} records, ${checkResult.apiVolume?.toFixed(2)} MWh, £${checkResult.apiPayment?.toFixed(2)}`);
      } else if (checkResult.status === 'missing') {
        console.log(`⚠️ Period ${period}: Missing DB records!`);
        console.log(`   API has ${checkResult.apiCount} records (${checkResult.apiVolume?.toFixed(2)} MWh, £${checkResult.apiPayment?.toFixed(2)})`);
      } else if (checkResult.status === 'error') {
        console.log(`❓ Period ${period}: Error: ${checkResult.error}`);
      }
    }
  }
  
  // Calculate overall mismatch percentage
  const mismatchPercentage = (result.totalMismatch / result.totalChecked) * 100;
  
  // Final verification summary
  console.log('\n=== Verification Summary ===\n');
  console.log(`Date: ${date}`);
  console.log(`Total Periods Checked: ${result.totalChecked}`);
  console.log(`Mismatched Periods: ${result.mismatchedPeriods.length} (${mismatchPercentage.toFixed(1)}%)`);
  console.log(`Missing Periods with Data: ${result.missingPeriods.length}`);
  
  if (result.isPassing) {
    console.log('✅ VERDICT: All checked periods have matched data');
  } else {
    if (result.missingPeriods.length > 0) {
      console.log(`⚠️ VERDICT: Missing data for ${result.missingPeriods.length} periods: ${result.missingPeriods.join(', ')}`);
    }
    if (result.mismatchedPeriods.length > 0) {
      console.log(`❌ VERDICT: Mismatched data for ${result.mismatchedPeriods.length} periods: ${result.mismatchedPeriods.join(', ')}`);
    }
  }
  
  return result;
}

/**
 * Get additional random periods excluding already checked periods
 */
function getAdditionalRandomPeriods(count: number, excludePeriods: number[]): number[] {
  const allPeriods = Array.from({ length: 48 }, (_, i) => i + 1);
  const availablePeriods = allPeriods.filter(p => !excludePeriods.includes(p));
  
  // Shuffle the available periods
  const shuffled = [...availablePeriods].sort(() => 0.5 - Math.random());
  
  // Return the requested count or all available if there are fewer
  return shuffled.slice(0, Math.min(count, availablePeriods.length));
}

/**
 * Fix data for a specific date by reprocessing it
 */
async function fixData(date: string): Promise<{
  curtailmentResult: any;
  bitcoinResult: any;
}> {
  console.log(`\n=== Fixing Data for ${date} ===\n`);
  
  // Step 1: Process curtailment data
  console.log('Step 1: Processing curtailment records...');
  const curtailmentResult = await processAllPeriods(date);
  
  // Step 2: Process Bitcoin calculations with full cascade updates
  console.log('\nStep 2: Processing Bitcoin calculations and updating summaries...');
  await processFullCascade(date);
  
  return {
    curtailmentResult,
    bitcoinResult: 'Completed successfully'
  };
}

/**
 * Verify and fix data for a specific date
 */
async function verifyAndFixData(date: string, action: string = 'verify', samplingMethod: string = 'progressive'): Promise<RepairResult> {
  console.log(`\n============================================`);
  console.log(` Data Verification and Repair: ${date}`);
  console.log(`============================================\n`);
  
  console.log(`Action: ${action}`);
  console.log(`Sampling Method: ${samplingMethod}`);
  
  const startTime = new Date();
  const initialState = await getDatabaseSummary(date);
  
  console.log('\n--- Initial Database State ---\n');
  console.log(`Records: ${initialState.recordCount}`);
  console.log(`Periods: ${initialState.periodsCovered}/48`);
  console.log(`Volume: ${initialState.totalVolume.toFixed(2)} MWh`);
  console.log(`Payment: £${initialState.totalPayment.toFixed(2)}`);
  
  if (initialState.periodsCovered > 0) {
    console.log(`Periods present: ${initialState.periodsPresent.join(', ')}`);
  }
  
  const result: RepairResult = {
    date,
    initialState,
    verificationResult: null as any,
    repairNeeded: false,
    repairSuccess: false
  };
  
  try {
    // For force-fix, skip verification
    if (action === 'force-fix') {
      console.log('\nForce fixing data without verification...');
      result.repairNeeded = true;
    } else {
      // Verify the data first
      const verificationResult = await verifyData(date, samplingMethod);
      result.verificationResult = verificationResult;
      
      // Determine if repair is needed
      result.repairNeeded = !verificationResult.isPassing;
      
      if (result.repairNeeded && action === 'fix') {
        console.log('\nVerification failed, proceeding with data repair...');
      } else if (result.repairNeeded && action === 'verify') {
        console.log('\nVerification failed, but repair action not specified.');
        console.log('To fix this data, run the script with the "fix" action:');
        console.log(`npx tsx verify_and_fix_data.ts ${date} fix`);
        return result;
      } else if (!result.repairNeeded) {
        console.log('\nVerification passed, no repair needed.');
        return result;
      }
    }
    
    // Fix the data if needed and action is 'fix' or 'force-fix'
    if (result.repairNeeded && (action === 'fix' || action === 'force-fix')) {
      const fixResult = await fixData(date);
      result.curtailmentResult = fixResult.curtailmentResult;
      result.bitcoinResult = fixResult.bitcoinResult;
      result.repairSuccess = true;
      
      // Get the final state after repair
      result.finalState = await getDatabaseSummary(date);
      
      console.log('\n--- Final Database State ---\n');
      console.log(`Records: ${result.finalState.recordCount}`);
      console.log(`Periods: ${result.finalState.periodsCovered}/48`);
      console.log(`Volume: ${result.finalState.totalVolume.toFixed(2)} MWh`);
      console.log(`Payment: £${result.finalState.totalPayment.toFixed(2)}`);
      
      // Calculate the changes
      const recordDiff = result.finalState.recordCount - initialState.recordCount;
      const periodDiff = result.finalState.periodsCovered - initialState.periodsCovered;
      const volumeDiff = result.finalState.totalVolume - initialState.totalVolume;
      const paymentDiff = result.finalState.totalPayment - initialState.totalPayment;
      
      console.log('\n--- Changes Made ---\n');
      console.log(`Records: ${recordDiff >= 0 ? '+' : ''}${recordDiff}`);
      console.log(`Periods: ${periodDiff >= 0 ? '+' : ''}${periodDiff}`);
      console.log(`Volume: ${volumeDiff >= 0 ? '+' : ''}${volumeDiff.toFixed(2)} MWh`);
      console.log(`Payment: ${paymentDiff >= 0 ? '+' : ''}£${paymentDiff.toFixed(2)}`);
    }
  } catch (error) {
    console.error('Error during verification/repair process:', error);
    result.error = error.message || 'Unknown error';
    result.repairSuccess = false;
  }
  
  const endTime = new Date();
  const executionTime = (endTime.getTime() - startTime.getTime()) / 1000;
  
  console.log(`\n=== Process Completed in ${executionTime.toFixed(1)}s ===\n`);
  
  return result;
}

/**
 * Validate date string format (YYYY-MM-DD)
 */
function isValidDate(dateStr: string): boolean {
  const regex = /^\d{4}-\d{2}-\d{2}$/;
  if (!regex.test(dateStr)) return false;
  
  const date = new Date(dateStr);
  return date instanceof Date && !isNaN(date.getTime());
}

/**
 * Generate log file path
 */
async function getLogFilePath(date: string): Promise<string> {
  // Ensure logs directory exists
  try {
    await fs.mkdir(LOG_DIR, { recursive: true });
  } catch (error) {
    console.error('Error creating logs directory:', error);
  }
  
  const timestamp = format(new Date(), 'yyyyMMdd_HHmmss');
  return path.join(LOG_DIR, `verify_and_fix_${date}_${timestamp}.log`);
}

/**
 * Main function
 */
async function main() {
  try {
    // Get command-line arguments
    const dateArg = process.argv[2];
    const actionArg = process.argv[3] || 'verify';
    const samplingArg = process.argv[4] || 'progressive';
    
    // Validate date or use today
    const dateToProcess = dateArg && isValidDate(dateArg) 
      ? dateArg 
      : format(new Date(), 'yyyy-MM-dd');
    
    // Validate action
    const validActions = ['verify', 'fix', 'force-fix'];
    const action = validActions.includes(actionArg) ? actionArg : 'verify';
    
    // Validate sampling method
    const validSamplingMethods = ['progressive', 'p', 'random', 'r', 'fixed', 'f', 'full', 'a'];
    const samplingMethod = validSamplingMethods.includes(samplingArg) ? samplingArg : 'progressive';
    
    // Process the request
    const result = await verifyAndFixData(dateToProcess, action, samplingMethod);
    
    // Save log file
    const logFilePath = await getLogFilePath(dateToProcess);
    await fs.writeFile(logFilePath, JSON.stringify(result, null, 2));
    console.log(`Log saved to ${logFilePath}`);
    
    // Display verification result
    if (result.repairNeeded && result.repairSuccess) {
      console.log('✅ Data verification and repair completed successfully');
    } else if (result.repairNeeded && !result.repairSuccess && action !== 'verify') {
      console.log('❌ Data repair failed');
      process.exit(1);
    } else if (!result.repairNeeded) {
      console.log('✅ Data verification completed successfully');
    } else {
      console.log('ℹ️ Data verification completed, repair needed but not performed');
    }
  } catch (error) {
    console.error('Error in main process:', error);
    process.exit(1);
  }
}

main();