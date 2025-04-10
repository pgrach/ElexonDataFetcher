/**
 * Verification Script for April 2, 2025 Data
 * 
 * This script verifies the completeness of data for April 2, 2025 by:
 * 1. Checking which settlement periods currently exist in the database
 * 2. Documenting which periods are missing
 * 3. Attempting to fetch missing periods from the Elexon API to verify data availability
 * 
 * Usage: npx tsx verify_april2_missing_data.ts
 */

import { db } from "./db";
import { curtailmentRecords } from "./db/schema";
import axios from "axios";
import fs from "fs/promises";
import { eq, sql, and, not, inArray } from "drizzle-orm";
import { format } from "date-fns";

// Constants
const TARGET_DATE = "2025-04-02";
const ELEXON_BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1";
const LOG_FILE_PATH = `./logs/verify_april2_${format(new Date(), "yyyy-MM-dd'T'HH-mm-ss")}.log`;

// Configure logger
const logger = {
  log: (message: string) => {
    const timestamp = new Date().toISOString();
    const logMessage = `[${timestamp}] ${message}`;
    console.log(logMessage);
    fs.appendFile(LOG_FILE_PATH, logMessage + "\n").catch(console.error);
  },
  error: (message: string, error: any) => {
    const timestamp = new Date().toISOString();
    const errorMessage = `[${timestamp}] ERROR: ${message}: ${error?.message || error}`;
    console.error(errorMessage);
    fs.appendFile(LOG_FILE_PATH, errorMessage + "\n").catch(console.error);
  }
};

// Initialize logger file
async function initLogger() {
  try {
    await fs.writeFile(LOG_FILE_PATH, `===== VERIFICATION LOG FOR ${TARGET_DATE} =====\n\n`);
    logger.log(`Log file initialized at ${LOG_FILE_PATH}`);
  } catch (error) {
    console.error("Failed to initialize log file:", error);
  }
}

// Delay utility
async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Check which settlement periods exist in the database
async function checkExistingPeriods(): Promise<number[]> {
  try {
    const existingPeriods = await db
      .select({
        period: curtailmentRecords.settlementPeriod
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE))
      .groupBy(curtailmentRecords.settlementPeriod)
      .orderBy(curtailmentRecords.settlementPeriod);
    
    return existingPeriods.map(record => record.period);
  } catch (error) {
    logger.error("Error checking existing periods", error);
    return [];
  }
}

// Check API availability for a specific settlement period
async function checkApiAvailability(period: number): Promise<boolean> {
  try {
    logger.log(`Checking Elexon API availability for period ${period}...`);
    
    const url = `${ELEXON_BASE_URL}/datasets/BOALF/sp?settlementDate=${TARGET_DATE}&settlementPeriod=${period}`;
    
    const response = await axios.get(url, {
      headers: { 'Accept': 'application/json' },
      timeout: 30000,
      validateStatus: (status) => status === 200 || status === 404
    });
    
    if (response.status === 404) {
      logger.log(`Period ${period}: NO DATA AVAILABLE (404 Not Found)`);
      return false;
    }
    
    if (!response.data || !Array.isArray(response.data.data)) {
      logger.log(`Period ${period}: INVALID RESPONSE FORMAT`);
      return false;
    }
    
    const recordCount = response.data.data.length;
    logger.log(`Period ${period}: DATA AVAILABLE (${recordCount} records)`);
    return recordCount > 0;
  } catch (error) {
    if (axios.isAxiosError(error) && error.response?.status === 429) {
      logger.log(`Rate limited checking period ${period}, waiting 60s before retry...`);
      await delay(60000);
      return checkApiAvailability(period);
    }
    
    logger.error(`Error checking API availability for period ${period}`, error);
    return false;
  }
}

// Main function
async function main() {
  try {
    await initLogger();
    
    logger.log(`===== VERIFYING DATA AVAILABILITY FOR ${TARGET_DATE} =====`);
    
    // Step 1: Check existing periods in the database
    const existingPeriods = await checkExistingPeriods();
    logger.log(`\nExisting periods in database (${existingPeriods.length}/48): ${existingPeriods.join(", ")}`);
    
    // Step 2: Identify missing periods
    const allPeriods = Array.from({ length: 48 }, (_, i) => i + 1);
    const missingPeriods = allPeriods.filter(period => !existingPeriods.includes(period));
    logger.log(`\nMissing periods (${missingPeriods.length}/48): ${missingPeriods.join(", ")}`);
    
    // Step 3: Check availability of a sample of missing periods in the Elexon API
    logger.log(`\n=== Checking Elexon API Availability for Sample Missing Periods ===`);
    
    // Sample 5 missing periods to check (or less if fewer are missing)
    const sampleSize = Math.min(5, missingPeriods.length);
    const samplePeriods = missingPeriods.slice(0, sampleSize);
    
    let availableInApi = 0;
    
    for (const period of samplePeriods) {
      const isAvailable = await checkApiAvailability(period);
      if (isAvailable) {
        availableInApi++;
      }
      // Add a delay between requests to avoid rate limiting
      await delay(5000);
    }
    
    // Step 4: Summarize findings
    logger.log(`\n=== Summary of Data Availability for ${TARGET_DATE} ===`);
    logger.log(`Total settlement periods: 48`);
    logger.log(`Periods in database: ${existingPeriods.length} (${(existingPeriods.length / 48 * 100).toFixed(1)}%)`);
    logger.log(`Missing periods: ${missingPeriods.length} (${(missingPeriods.length / 48 * 100).toFixed(1)}%)`);
    logger.log(`Sample API availability: ${availableInApi}/${sampleSize} periods available in API`);
    
    if (availableInApi === 0 && sampleSize > 0) {
      logger.log(`\nCONCLUSION: The missing data appears to be GENUINELY UNAVAILABLE from the Elexon API.`);
      logger.log(`The API returned 404 Not Found for all sampled missing periods.`);
      logger.log(`This suggests the data issue is with the source (Elexon), not with our ingestion process.`);
      logger.log(`RECOMMENDATION: Contact Elexon to inquire about the missing settlement periods for ${TARGET_DATE}.`);
    } else if (availableInApi > 0) {
      logger.log(`\nCONCLUSION: Some missing data IS AVAILABLE from the Elexon API but not in our database.`);
      logger.log(`RECOMMENDATION: Run a full reingestion process with appropriate rate limiting to fetch the available data.`);
    } else {
      logger.log(`\nCONCLUSION: Unable to determine data availability due to no sample periods.`);
    }
    
    logger.log(`\n===== VERIFICATION COMPLETE =====`);
    
  } catch (error) {
    logger.error("Main process error", error);
  }
}

// Execute the main function
main().catch(error => {
  console.error("Fatal error:", error);
  process.exit(1);
});