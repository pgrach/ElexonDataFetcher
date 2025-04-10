/**
 * Fixed Reprocessing Script for 2025-04-03
 * 
 * This script uses the correct Elexon API endpoints to fetch data for all 48 settlement periods.
 * It matches the implementation in the server/services/elexon.ts file.
 */

import { db } from './db';
import { curtailmentRecords, dailySummaries } from './db/schema';
import { eq, and, sql } from 'drizzle-orm';
import axios from 'axios';
import * as fs from 'fs';
import * as path from 'path';

// Configuration
const TARGET_DATE = '2025-04-03';
const ALL_PERIODS = Array.from({ length: 48 }, (_, i) => i + 1);
const LOG_FILE_PATH = `./logs/reprocess_elexon_${TARGET_DATE.replace(/-/g, '_')}_${new Date().toISOString().replace(/:/g, '-')}.log`;
const ELEXON_BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1";

// Try to load BMU mapping from both possible locations
const BMU_MAPPING_PATH_1 = './data/bmu_mapping.json';
const BMU_MAPPING_PATH_2 = './server/data/bmuMapping.json';

/**
 * Simple logging utility with timestamps
 */
function log(message: string): void {
  const timestamp = new Date().toISOString().substring(11, 19);
  const logMessage = `[${timestamp}] ${message}`;
  console.log(logMessage);
  
  // Append to log file
  fs.appendFileSync(LOG_FILE_PATH, logMessage + '\n');
}

/**
 * Load wind farm BMU IDs
 */
async function loadWindFarmIds(): Promise<Set<string>> {
  let bmuMappingPath = '';
  
  // Check which mapping file exists
  if (fs.existsSync(BMU_MAPPING_PATH_1)) {
    bmuMappingPath = BMU_MAPPING_PATH_1;
  } else if (fs.existsSync(BMU_MAPPING_PATH_2)) {
    bmuMappingPath = BMU_MAPPING_PATH_2;
  } else {
    throw new Error('BMU mapping file not found');
  }
  
  try {
    log(`Loading BMU mapping from: ${bmuMappingPath}`);
    const mappingContent = fs.readFileSync(bmuMappingPath, 'utf8');
    const bmuMapping = JSON.parse(mappingContent);
    const windFarmIds = new Set(bmuMapping.map((bmu: any) => bmu.elexonBmUnit));
    log(`Loaded ${windFarmIds.size} wind farm BMU IDs`);
    return windFarmIds;
  } catch (error) {
    log(`Error loading BMU mapping: ${(error as Error).message}`);
    throw error;
  }
}

/**
 * Fetch bids and offers from Elexon API
 */
async function fetchBidsOffers(date: string, period: number): Promise<any[]> {
  try {
    log(`Fetching data for ${date} Period ${period}...`);
    const validWindFarmIds = await loadWindFarmIds();
    
    // Make parallel requests for bids and offers
    const bidsUrl = `${ELEXON_BASE_URL}/balancing/settlement/stack/all/bid/${date}/${period}`;
    const offersUrl = `${ELEXON_BASE_URL}/balancing/settlement/stack/all/offer/${date}/${period}`;
    
    log(`Bid URL: ${bidsUrl}`);
    log(`Offer URL: ${offersUrl}`);
    
    try {
      const [bidsResponse, offersResponse] = await Promise.all([
        axios.get(bidsUrl, {
          headers: { 'Accept': 'application/json' },
          timeout: 30000 // 30 second timeout
        }),
        axios.get(offersUrl, {
          headers: { 'Accept': 'application/json' },
          timeout: 30000 // 30 second timeout
        })
      ]);
      
      if (!bidsResponse.data?.data || !offersResponse.data?.data) {
        log(`Invalid API response format for ${date} P${period}`);
        return [];
      }
      
      const validBids = bidsResponse.data.data.filter((record: any) => 
        record.volume < 0 && record.soFlag && validWindFarmIds.has(record.id)
      );
      
      const validOffers = offersResponse.data.data.filter((record: any) => 
        record.volume < 0 && record.soFlag && validWindFarmIds.has(record.id)
      );
      
      const allRecords = [...validBids, ...validOffers];
      
      if (allRecords.length > 0) {
        const periodTotal = allRecords.reduce((sum, r) => sum + Math.abs(r.volume), 0);
        const periodPayment = allRecords.reduce((sum, r) => sum + (Math.abs(r.volume) * r.originalPrice * -1), 0);
        log(`[${date} P${period}] Records: ${allRecords.length} (${periodTotal.toFixed(2)} MWh, £${periodPayment.toFixed(2)})`);
      } else {
        log(`No valid curtailment records for ${date} Period ${period}`);
      }
      
      return allRecords;
    } catch (error) {
      if (axios.isAxiosError(error)) {
        log(`[${date} P${period}] Elexon API error: ${error.response?.status} - ${error.message}`);
        return [];
      }
      log(`[${date} P${period}] Unexpected error: ${(error as Error).message}`);
      return [];
    }
  } catch (error) {
    log(`Error fetching data for ${date} P${period}: ${(error as Error).message}`);
    return [];
  }
}

/**
 * Clear existing curtailment records for the target date
 */
async function clearExistingCurtailmentRecords(): Promise<void> {
  log(`Clearing existing curtailment records for ${TARGET_DATE}...`);
  
  const result = await db.delete(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE))
    .returning({
      id: curtailmentRecords.id
    });
  
  log(`Cleared ${result.length} existing curtailment records`);
}

/**
 * Process and store curtailment records
 */
async function processCurtailmentRecord(record: any, period: number): Promise<boolean> {
  if (!record || !record.id) return false;
  
  try {
    const volume = Math.abs(parseFloat(record.volume));
    const payment = parseFloat(record.originalPrice) * volume * -1;
    
    await db.insert(curtailmentRecords).values({
      settlementDate: TARGET_DATE,
      settlementPeriod: period,
      volume: volume.toString(),
      payment: payment.toString(),
      farmId: record.id,
      leadParty: record.leadParty || null,
      ngcBmUnit: record.ngcBmUnit || null,
      curtailmentType: 'bid_offer',
      createdBy: 'data_reprocessing_script',
      createdAt: new Date()
    });
    
    log(`[${TARGET_DATE} P${period}] Added record for ${record.id}: ${volume.toFixed(2)} MWh, £${payment.toFixed(2)}`);
    return true;
  } catch (error) {
    log(`Error inserting record for ${record.id}: ${(error as Error).message}`);
    return false;
  }
}

/**
 * Update daily summary based on curtailment records
 */
async function updateDailySummary(): Promise<void> {
  log(`Updating daily summary for ${TARGET_DATE}...`);
  
  // Calculate totals from curtailment records
  const totals = await db
    .select({
      totalCurtailedEnergy: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
      totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
  
  const totalCurtailedEnergy = parseFloat(totals[0]?.totalCurtailedEnergy || '0');
  const totalPayment = parseFloat(totals[0]?.totalPayment || '0');
  
  log(`Calculated totals: ${totalCurtailedEnergy.toFixed(2)} MWh, £${totalPayment.toFixed(2)}`);
  
  // Update existing summary or insert new one
  await db.execute(sql`
    INSERT INTO daily_summaries (
      summary_date, total_curtailed_energy, total_payment, created_at, last_updated
    )
    VALUES (
      ${TARGET_DATE}, ${totalCurtailedEnergy.toString()}, ${totalPayment.toString()}, 
      NOW(), NOW()
    )
    ON CONFLICT (summary_date) 
    DO UPDATE SET
      total_curtailed_energy = ${totalCurtailedEnergy.toString()},
      total_payment = ${totalPayment.toString()},
      last_updated = NOW()
  `);
  
  // Get updated summary
  const updatedSummary = await db
    .select()
    .from(dailySummaries)
    .where(eq(dailySummaries.summaryDate, TARGET_DATE));
  
  if (updatedSummary.length > 0) {
    log(`Updated summary: ${parseFloat(updatedSummary[0].totalCurtailedEnergy?.toString() || '0').toFixed(2)} MWh, £${parseFloat(updatedSummary[0].totalPayment?.toString() || '0').toFixed(2)}`);
  }
}

/**
 * Run the complete reprocessing process
 */
async function runReprocessing(): Promise<void> {
  log(`Starting reprocessing for ${TARGET_DATE}...`);
  
  // Step 1: Clear existing curtailment records
  await clearExistingCurtailmentRecords();
  
  // Step 2: Process all 48 settlement periods
  let totalRecords = 0;
  
  for (const period of ALL_PERIODS) {
    const records = await fetchBidsOffers(TARGET_DATE, period);
    
    // Process each record
    let periodRecords = 0;
    for (const record of records) {
      const inserted = await processCurtailmentRecord(record, period);
      if (inserted) {
        periodRecords++;
        totalRecords++;
      }
    }
    
    log(`Processed ${periodRecords} records for Period ${period}`);
    
    // Small delay to avoid overwhelming the database
    await new Promise(resolve => setTimeout(resolve, 500));
  }
  
  log(`Inserted ${totalRecords} curtailment records for ${TARGET_DATE}`);
  
  // Step 3: Update daily summary
  await updateDailySummary();
  
  log(`Reprocessing for ${TARGET_DATE} finished successfully`);
}

// Create logs directory if it doesn't exist
if (!fs.existsSync('./logs')) {
  fs.mkdirSync('./logs');
}

// Execute the reprocessing
runReprocessing()
  .then(() => {
    console.log('\nReprocessing completed successfully.');
    process.exit(0);
  })
  .catch((error) => {
    console.error('\nReprocessing failed with error:', error);
    process.exit(1);
  });