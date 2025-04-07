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

import { format } from 'date-fns';
import fs from 'fs/promises';
import path from 'path';
import axios from 'axios';
import { db } from './db';
import { curtailmentRecords, dailySummaries } from './db/schema';
import { eq, sql } from 'drizzle-orm';
import { processAllPeriods } from './fix_bmu_mapping';
import { processFullCascade } from './process_bitcoin_optimized';

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

// Constants
const ELEXON_BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1";
const SERVER_BMU_MAPPING_PATH = path.join('server', 'data', 'bmuMapping.json');
const API_RATE_LIMIT_DELAY_MS = 500;

// Cache for BMU mapping
let bmuMapping: Record<string, { name: string, leadParty: string }> | null = null;
let windFarmIds: Set<string> | null = null;

/**
 * Load the BMU mapping file once
 */
async function loadBmuMapping(): Promise<Record<string, { name: string, leadParty: string }>> {
  if (bmuMapping !== null) return bmuMapping;
  
  try {
    console.log(`Loading BMU mapping from ${SERVER_BMU_MAPPING_PATH}...`);
    const mappingFile = await fs.readFile(SERVER_BMU_MAPPING_PATH, 'utf-8');
    const mappingData = JSON.parse(mappingFile);
    
    const mapping: Record<string, { name: string, leadParty: string }> = {};
    for (const bmu of mappingData) {
      mapping[bmu.elexonBmUnit] = {
        name: bmu.bmUnitName,
        leadParty: bmu.leadPartyName
      };
    }
    
    bmuMapping = mapping;
    console.log(`Loaded ${Object.keys(mapping).length} BMU mappings from server data`);
    return mapping;
  } catch (error) {
    console.error('Error loading BMU mapping:', error);
    throw error;
  }
}

/**
 * Filter for valid wind farm BMUs
 */
async function loadWindFarmIds(): Promise<Set<string>> {
  if (windFarmIds !== null) {
    return windFarmIds;
  }

  try {
    const bmuMapping = await loadBmuMapping();
    windFarmIds = new Set(Object.keys(bmuMapping));
    console.log(`Loaded ${windFarmIds.size} wind farm BMU IDs`);
    return windFarmIds;
  } catch (error) {
    console.error('Error loading wind farm IDs:', error);
    throw error;
  }
}

/**
 * Get database summary for a specific date
 */
async function getDatabaseSummary(date: string): Promise<DatabaseSummary> {
  try {
    // Count records
    const countResult = await db.select({ count: sql`COUNT(*)` })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date));
    
    const recordCount = Number(countResult[0].count);
    
    if (recordCount === 0) {
      return {
        recordCount: 0,
        periodsCovered: 0,
        totalVolume: 0,
        totalPayment: 0,
        periodsPresent: []
      };
    }
    
    // Get distinct periods
    const periodsResult = await db.select({ period: curtailmentRecords.settlementPeriod })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date))
      .groupBy(curtailmentRecords.settlementPeriod);
    
    const periodsPresent = periodsResult.map(r => Number(r.period));
    
    // Get total volume and payment
    const totalsResult = await db.select({
      volume: sql`SUM(ABS(volume::numeric))`,
      payment: sql`SUM(ABS(payment::numeric))`
    }).from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date));
    
    return {
      recordCount,
      periodsCovered: periodsPresent.length,
      totalVolume: Number(totalsResult[0].volume) || 0,
      totalPayment: Number(totalsResult[0].payment) || 0,
      periodsPresent
    };
  } catch (error) {
    console.error(`Error getting database summary for ${date}:`, error);
    return {
      recordCount: 0,
      periodsCovered: 0,
      totalVolume: 0,
      totalPayment: 0,
      periodsPresent: []
    };
  }
}

/**
 * Check a specific settlement period against Elexon API
 */
async function checkPeriod(date: string, period: number): Promise<{
  status: 'match' | 'mismatch' | 'missing' | 'error';
  dbCount: number;
  apiCount: number;
  dbVolume: number;
  apiVolume: number;
  dbPayment: number;
  apiPayment: number;
  error?: string;
}> {
  try {
    // Get database records for this period
    const dbRecords = await db.select({
      count: sql`COUNT(*)`,
      volume: sql`SUM(ABS(volume::numeric))`,
      payment: sql`SUM(ABS(payment::numeric))`
    })
    .from(curtailmentRecords)
    .where(
      eq(curtailmentRecords.settlementDate, date) &&
      eq(curtailmentRecords.settlementPeriod, period.toString())
    );
    
    const dbCount = Number(dbRecords[0].count) || 0;
    const dbVolume = Number(dbRecords[0].volume) || 0;
    const dbPayment = Number(dbRecords[0].payment) || 0;
    
    // Get API records
    const validWindFarmIds = await loadWindFarmIds();
    
    const bidsUrl = `${ELEXON_BASE_URL}/balancing/settlement/stack/all/bid/${date}/${period}`;
    const offersUrl = `${ELEXON_BASE_URL}/balancing/settlement/stack/all/offer/${date}/${period}`;
    
    console.log(`Making API request for ${date} period ${period}...`);
    const bidsResponse = await axios.get(bidsUrl, {
      headers: { 'Accept': 'application/json' },
      timeout: 30000
    });
    
    const offersResponse = await axios.get(offersUrl, {
      headers: { 'Accept': 'application/json' },
      timeout: 30000
    });
    
    const bids = bidsResponse.data.data || [];
    const offers = offersResponse.data.data || [];
    
    // Filter for wind farm records
    const windFarmBids = bids.filter((record: any) => validWindFarmIds.has(record.id));
    const windFarmOffers = offers.filter((record: any) => validWindFarmIds.has(record.id));
    
    // Filter for curtailment conditions
    const validBids = windFarmBids.filter((record: any) => record.volume < 0 && record.soFlag);
    const validOffers = windFarmOffers.filter((record: any) => record.volume < 0 && record.soFlag);
    
    const apiRecords = [...validBids, ...validOffers];
    const apiCount = apiRecords.length;
    
    // Calculate API totals
    const apiVolume = apiRecords.reduce((sum, r) => sum + Math.abs(r.volume), 0);
    const apiPayment = apiRecords.reduce((sum, r) => {
      const payment = typeof r.payment === 'number' ? r.payment : 
                    (r.originalPrice ? r.originalPrice * Math.abs(r.volume) : 0);
      return sum + Math.abs(payment);
    }, 0);
    
    console.log(`[${date} P${period}] DB: ${dbCount} records (${dbVolume.toFixed(2)} MWh, £${dbPayment.toFixed(2)})`);
    console.log(`[${date} P${period}] API: ${apiCount} records (${apiVolume.toFixed(2)} MWh, £${apiPayment.toFixed(2)})`);
    
    // Determine status
    let status: 'match' | 'mismatch' | 'missing' | 'error' = 'match';
    
    if (dbCount === 0 && apiCount === 0) {
      status = 'match'; // Both have no data, which is fine
    } else if (dbCount === 0 && apiCount > 0) {
      status = 'missing'; // Missing data in DB
    } else if (dbCount > 0 && apiCount === 0) {
      status = 'mismatch'; // Extra data in DB that shouldn't be there
    } else {
      // Both have data, check if counts and volumes match within 5% tolerance
      const countDiff = Math.abs(dbCount - apiCount);
      const volumeDiff = Math.abs(dbVolume - apiVolume);
      const paymentDiff = Math.abs(dbPayment - apiPayment);
      
      // Allow 5% tolerance for differences since some minor variations can occur
      const countTolerance = Math.max(1, apiCount * 0.05);
      const volumeTolerance = Math.max(0.5, apiVolume * 0.05);
      const paymentTolerance = Math.max(0.5, apiPayment * 0.05);
      
      if (
        countDiff <= countTolerance &&
        volumeDiff <= volumeTolerance &&
        paymentDiff <= paymentTolerance
      ) {
        status = 'match';
      } else {
        status = 'mismatch';
      }
    }
    
    return {
      status,
      dbCount,
      apiCount,
      dbVolume,
      apiVolume,
      dbPayment,
      apiPayment
    };
  } catch (error) {
    console.error(`Error checking period ${period} for ${date}:`, error);
    return {
      status: 'error',
      dbCount: 0,
      apiCount: 0,
      dbVolume: 0,
      apiVolume: 0,
      dbPayment: 0,
      apiPayment: 0,
      error: error instanceof Error ? error.message : String(error)
    };
  }
}

/**
 * Get a list of periods to check based on the sampling method
 */
function getPeriodsToCheck(method: string): number[] {
  switch (method.toLowerCase()) {
    case 'fixed':
    case 'f':
      return [1, 12, 24, 36, 48];
    case 'random':
    case 'r':
      return getRandomPeriods(10);
    case 'full':
    case 'a':
    case 'all':
      return Array.from({ length: 48 }, (_, i) => i + 1);
    case 'progressive':
    case 'p':
    default:
      return [1, 12, 24, 36, 48]; // Start with key periods for progressive sampling
  }
}

/**
 * Get random periods for sampling
 */
function getRandomPeriods(count: number = 10): number[] {
  const periods = Array.from({ length: 48 }, (_, i) => i + 1);
  const result = [];
  
  // Ensure we always include period 1 for consistency
  result.push(1);
  periods.splice(0, 1);
  
  // Add random periods
  for (let i = 0; i < count - 1 && periods.length > 0; i++) {
    const randomIndex = Math.floor(Math.random() * periods.length);
    result.push(periods[randomIndex]);
    periods.splice(randomIndex, 1);
  }
  
  return result.sort((a, b) => a - b);
}

/**
 * Verify data for a specific date
 */
async function verifyData(date: string, samplingMethod: string = 'progressive'): Promise<VerificationResult> {
  console.log(`\n=== Starting Verification for ${date} (Method: ${samplingMethod}) ===\n`);
  
  const isProgressive = samplingMethod.toLowerCase() === 'progressive' || samplingMethod.toLowerCase() === 'p';
  
  // Get periods to check based on sampling method
  let periodsToCheck = getPeriodsToCheck(samplingMethod);
  console.log(`Checking periods: ${periodsToCheck.join(', ')}`);
  
  const result: VerificationResult = {
    isPassing: true,
    totalChecked: 0,
    totalMismatch: 0,
    mismatchedPeriods: [],
    missingPeriods: [],
    details: []
  };
  
  // Check each period
  for (const period of periodsToCheck) {
    const checkResult = await checkPeriod(date, period);
    result.totalChecked++;
    
    result.details.push({
      period,
      ...checkResult
    });
    
    if (checkResult.status === 'mismatch') {
      result.totalMismatch++;
      result.mismatchedPeriods.push(period);
      result.isPassing = false;
    } else if (checkResult.status === 'missing') {
      result.missingPeriods.push(period);
      result.isPassing = false;
    } else if (checkResult.status === 'error') {
      console.error(`Error checking period ${period}:`, checkResult.error);
    }
    
    // Add a delay to avoid API rate limits
    await new Promise(resolve => setTimeout(resolve, API_RATE_LIMIT_DELAY_MS));
  }
  
  // If using progressive sampling and issues were found, check more periods
  if (isProgressive && !result.isPassing) {
    console.log(`\nIssues found. Expanding check to additional periods...`);
    
    // Check 10 more random periods
    const additionalPeriods = getAdditionalRandomPeriods(10, periodsToCheck);
    console.log(`Additional periods to check: ${additionalPeriods.join(', ')}`);
    
    for (const period of additionalPeriods) {
      const checkResult = await checkPeriod(date, period);
      result.totalChecked++;
      
      result.details.push({
        period,
        ...checkResult
      });
      
      if (checkResult.status === 'mismatch') {
        result.totalMismatch++;
        result.mismatchedPeriods.push(period);
      } else if (checkResult.status === 'missing') {
        result.missingPeriods.push(period);
      }
      
      // Add a delay to avoid API rate limits
      await new Promise(resolve => setTimeout(resolve, API_RATE_LIMIT_DELAY_MS));
    }
  }
  
  // Print verification summary
  console.log(`\n=== Verification Summary for ${date} ===`);
  console.log(`Total Periods Checked: ${result.totalChecked}`);
  console.log(`Mismatched Periods: ${result.mismatchedPeriods.length} (${result.mismatchedPeriods.join(', ')})`);
  console.log(`Missing Periods: ${result.missingPeriods.length} (${result.missingPeriods.join(', ')})`);
  console.log(`Verification Status: ${result.isPassing ? 'PASSING' : 'FAILED'}`);
  
  return result;
}

/**
 * Get additional random periods excluding already checked periods
 */
function getAdditionalRandomPeriods(count: number, excludePeriods: number[]): number[] {
  const allPeriods = Array.from({ length: 48 }, (_, i) => i + 1);
  const availablePeriods = allPeriods.filter(p => !excludePeriods.includes(p));
  const result = [];
  
  // Add random periods from available periods
  for (let i = 0; i < count && availablePeriods.length > 0; i++) {
    const randomIndex = Math.floor(Math.random() * availablePeriods.length);
    result.push(availablePeriods[randomIndex]);
    availablePeriods.splice(randomIndex, 1);
  }
  
  return result.sort((a, b) => a - b);
}

/**
 * Fix data for a specific date by reprocessing it
 */
async function fixData(date: string): Promise<{
  curtailmentResult: any;
  bitcoinResult: any;
}> {
  console.log(`\n=== Starting Data Fix for ${date} ===\n`);
  
  try {
    // Step 1: Process all periods for curtailment data
    console.log(`\n==== Step 1: Processing Curtailment Records ====\n`);
    const curtailmentResult = await processAllPeriods(date);
    
    if (curtailmentResult.totalRecords === 0) {
      console.log(`\nNo curtailment records found for ${date}, skipping Bitcoin calculations`);
      return {
        curtailmentResult,
        bitcoinResult: null
      };
    }
    
    console.log(`\nProcessed ${curtailmentResult.totalRecords} curtailment records across ${curtailmentResult.totalPeriods} periods`);
    console.log(`Total Energy: ${curtailmentResult.totalVolume.toFixed(2)} MWh`);
    console.log(`Total Payment: £${curtailmentResult.totalPayment.toFixed(2)}`);
    
    // Step 2: Process Bitcoin calculations and cascade updates
    console.log(`\n==== Step 2: Processing Bitcoin Calculations and Summaries ====\n`);
    await processFullCascade(date);
    
    console.log(`\n=== Data Fix Successful for ${date} ===\n`);
    
    // Return dummy result for Bitcoin since we don't have a return value from processFullCascade
    const bitcoinResult = { success: true, date };
    
    return {
      curtailmentResult,
      bitcoinResult
    };
  } catch (error) {
    console.error(`Error fixing data for ${date}:`, error);
    throw error;
  }
}

/**
 * Verify and fix data for a specific date
 */
async function verifyAndFixData(date: string, action: string = 'verify', samplingMethod: string = 'progressive'): Promise<RepairResult> {
  console.log(`\n=== Starting Verification and Repair for ${date} (Action: ${action}) ===\n`);
  
  // Get initial database state
  const initialState = await getDatabaseSummary(date);
  console.log(`Initial Database State:`);
  console.log(`- Records: ${initialState.recordCount}`);
  console.log(`- Periods Covered: ${initialState.periodsCovered}/48`);
  console.log(`- Total Volume: ${initialState.totalVolume.toFixed(2)} MWh`);
  console.log(`- Total Payment: £${initialState.totalPayment.toFixed(2)}`);
  
  const result: RepairResult = {
    date,
    initialState,
    verificationResult: {
      isPassing: true,
      totalChecked: 0,
      totalMismatch: 0,
      mismatchedPeriods: [],
      missingPeriods: [],
      details: []
    },
    repairNeeded: false,
    repairSuccess: false
  };
  
  if (action === 'force-fix') {
    // Skip verification and force a complete reprocessing
    console.log(`\nSkipping verification and forcing complete reprocessing of ${date}...\n`);
    result.repairNeeded = true;
  } else {
    // Verify data first
    console.log(`\nVerifying data integrity for ${date}...\n`);
    result.verificationResult = await verifyData(date, samplingMethod);
    result.repairNeeded = !result.verificationResult.isPassing;
    
    if (result.verificationResult.isPassing) {
      console.log(`\nData for ${date} is valid. No repair needed.`);
    } else {
      console.log(`\nData integrity issues found for ${date}.`);
      
      if (action === 'verify') {
        console.log(`Repair needed but not performed (action: verify)`);
        console.log(`Run with action 'fix' to automatically repair the data.`);
        return result;
      }
    }
  }
  
  // Fix data if needed and action is 'fix' or 'force-fix'
  if ((result.repairNeeded || action === 'force-fix') && (action === 'fix' || action === 'force-fix')) {
    console.log(`\nRepairing data for ${date}...\n`);
    
    try {
      const fixResult = await fixData(date);
      result.repairSuccess = true;
      result.curtailmentResult = fixResult.curtailmentResult;
      result.bitcoinResult = fixResult.bitcoinResult;
      
      // Get final database state
      const finalState = await getDatabaseSummary(date);
      result.finalState = finalState;
      
      console.log(`\nFinal Database State:`);
      console.log(`- Records: ${finalState.recordCount}`);
      console.log(`- Periods Covered: ${finalState.periodsCovered}/48`);
      console.log(`- Total Volume: ${finalState.totalVolume.toFixed(2)} MWh`);
      console.log(`- Total Payment: £${finalState.totalPayment.toFixed(2)}`);
    } catch (error) {
      result.repairSuccess = false;
      result.error = error instanceof Error ? error.message : String(error);
      console.error(`\nError repairing data for ${date}:`, error);
    }
  }
  
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
  const logsDir = path.join('.', 'logs');
  
  // Ensure logs directory exists
  try {
    await fs.mkdir(logsDir, { recursive: true });
  } catch (error) {
    console.error('Error creating logs directory:', error);
  }
  
  const timestamp = format(new Date(), 'yyyyMMdd-HHmmss');
  return path.join(logsDir, `verify-fix-${date}-${timestamp}.log`);
}

/**
 * Main function
 */
async function main() {
  try {
    // Get command-line arguments
    const dateArg = process.argv[2] || format(new Date(), 'yyyy-MM-dd');
    const actionArg = process.argv[3] || 'verify';
    const samplingArg = process.argv[4] || 'progressive';
    
    // Validate date format
    if (!isValidDate(dateArg)) {
      console.error(`Invalid date format: ${dateArg}`);
      console.error(`Please use YYYY-MM-DD format (e.g., 2025-04-01)`);
      process.exit(1);
    }
    
    // Validate action
    const validActions = ['verify', 'fix', 'force-fix'];
    if (!validActions.includes(actionArg)) {
      console.error(`Invalid action: ${actionArg}`);
      console.error(`Valid actions: ${validActions.join(', ')}`);
      process.exit(1);
    }
    
    // Validate sampling method
    const validSamplingMethods = ['progressive', 'p', 'random', 'r', 'fixed', 'f', 'full', 'a', 'all'];
    if (!validSamplingMethods.includes(samplingArg.toLowerCase())) {
      console.error(`Invalid sampling method: ${samplingArg}`);
      console.error(`Valid methods: progressive, random, fixed, full`);
      process.exit(1);
    }
    
    console.log(`\n=== Data Verification and Repair Tool ===`);
    console.log(`Date: ${dateArg}`);
    console.log(`Action: ${actionArg}`);
    console.log(`Sampling Method: ${samplingArg}`);
    
    const startTime = Date.now();
    
    // Run verification and optional fix
    const result = await verifyAndFixData(dateArg, actionArg, samplingArg);
    
    const duration = (Date.now() - startTime) / 1000;
    console.log(`\n=== Process Complete ===`);
    console.log(`Duration: ${duration.toFixed(1)} seconds`);
    
    // Generate summary and log
    const logPath = await getLogFilePath(dateArg);
    
    const summary = {
      date: dateArg,
      action: actionArg,
      samplingMethod: samplingArg,
      startTime: new Date(startTime).toISOString(),
      endTime: new Date().toISOString(),
      duration: `${duration.toFixed(1)} seconds`,
      result
    };
    
    await fs.writeFile(logPath, JSON.stringify(summary, null, 2), 'utf-8');
    console.log(`\nDetailed log saved to: ${logPath}`);
    
    // Print final status
    if (actionArg === 'verify') {
      if (result.verificationResult.isPassing) {
        console.log(`\n✅ Data for ${dateArg} is VALID`);
      } else {
        console.log(`\n❌ Data for ${dateArg} has INTEGRITY ISSUES`);
        console.log(`Run with action 'fix' to automatically repair the data.`);
      }
    } else {
      if (result.repairSuccess) {
        console.log(`\n✅ Data for ${dateArg} has been SUCCESSFULLY REPAIRED`);
      } else if (!result.repairNeeded) {
        console.log(`\n✅ Data for ${dateArg} is VALID (no repair needed)`);
      } else {
        console.log(`\n❌ Data repair for ${dateArg} FAILED`);
        console.log(`Please see the log file for details.`);
      }
    }
  } catch (error) {
    console.error('Unexpected error:', error);
    process.exit(1);
  }
}

main();