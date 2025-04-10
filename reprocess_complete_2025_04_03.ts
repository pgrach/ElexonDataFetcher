/**
 * Complete Reprocessing Script for 2025-04-03
 * 
 * This script performs a complete reprocessing of data for 2025-04-03
 * by fetching all 48 settlement periods from the Elexon API.
 */

import { db } from './db';
import { curtailmentRecords, dailySummaries } from './db/schema';
import { eq, and, sql } from 'drizzle-orm';
import axios from 'axios';
import * as fs from 'fs';

// Configuration
const TARGET_DATE = '2025-04-03';
const ALL_PERIODS = Array.from({ length: 48 }, (_, i) => i + 1);
const LOG_FILE_PATH = `./logs/reprocess_complete_${TARGET_DATE.replace(/-/g, '_')}_${new Date().toISOString().replace(/:/g, '-')}.log`;
const ELEXON_API_URL = 'https://data.elexon.co.uk/bmrs/api/v1/curtailment/derived';

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
 * Fetch Elexon API data for a specific settlement period
 */
async function fetchElexonData(period: number): Promise<any> {
  log(`Fetching data for period ${period}...`);
  
  try {
    const url = `${ELEXON_API_URL}/${TARGET_DATE}/${period}`;
    const response = await axios.get(url);
    return response.data;
  } catch (error) {
    log(`Error fetching data for period ${period}: ${(error as Error).message}`);
    return null;
  }
}

/**
 * Process and store curtailment records for a specific period
 */
async function processPeriodData(period: number, periodData: any): Promise<number> {
  if (!periodData || !Array.isArray(periodData.data)) {
    log(`No data available for period ${period}`);
    return 0;
  }
  
  let insertedCount = 0;
  let totalVolume = 0;
  let totalPayment = 0;
  
  for (const record of periodData.data) {
    if (!record.bmUnit) continue;
    
    const volume = Math.abs(parseFloat(record.volume));
    const payment = parseFloat(record.curtailmentPayment);
    
    totalVolume += volume;
    totalPayment += payment;
    
    try {
      await db.insert(curtailmentRecords).values({
        settlementDate: TARGET_DATE,
        settlementPeriod: period,
        volume: volume.toString(),
        payment: payment.toString(),
        farmId: record.bmUnit,
        leadParty: record.leadParty || null,
        ngcBmUnit: record.ngcBmUnit || null,
        curtailmentType: record.curtailmentType || null,
        createdBy: 'data_reprocessing_script',
        createdAt: new Date()
      });
      
      insertedCount++;
      log(`[${TARGET_DATE} P${period}] Added record for ${record.bmUnit}: ${volume.toFixed(2)} MWh, £${payment.toFixed(2)}`);
    } catch (error) {
      log(`Error inserting record for ${record.bmUnit}: ${(error as Error).message}`);
    }
  }
  
  log(`[${TARGET_DATE} P${period}] Total: ${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)}`);
  return insertedCount;
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
async function runCompleteReprocessing(): Promise<void> {
  log(`Starting complete reprocessing for ${TARGET_DATE}...`);
  
  // Step 1: Clear existing curtailment records
  await clearExistingCurtailmentRecords();
  
  // Step 2: Process all 48 settlement periods
  let totalRecords = 0;
  
  for (const period of ALL_PERIODS) {
    const periodData = await fetchElexonData(period);
    const insertedCount = await processPeriodData(period, periodData);
    totalRecords += insertedCount;
  }
  
  log(`Inserted ${totalRecords} curtailment records for ${TARGET_DATE}`);
  
  // Step 3: Update daily summary
  await updateDailySummary();
  
  log(`Complete reprocessing for ${TARGET_DATE} finished successfully`);
}

// Execute the reprocessing
runCompleteReprocessing()
  .then(() => {
    console.log('\nReprocessing completed successfully.');
    process.exit(0);
  })
  .catch((error) => {
    console.error('\nReprocessing failed with error:', error);
    process.exit(1);
  });