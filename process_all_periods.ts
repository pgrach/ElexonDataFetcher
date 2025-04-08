/**
 * Process All 48 Periods for a Specific Date
 * 
 * This script ensures all 48 settlement periods are processed for a specific date,
 * with enhanced error handling and retries for more robust data ingestion.
 */

import { db } from './db';
import { curtailmentRecords, dailySummaries } from './db/schema';
import { eq, sql } from 'drizzle-orm';
import { fetchBidsOffers } from './server/services/elexon';
import fs from 'fs/promises';
import path from 'path';
import { format, addMinutes } from 'date-fns';

// Configuration
const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 2000;
const API_RATE_LIMIT_DELAY_MS = 500;
const SMALL_BATCH_SIZE = 4; // Number of periods to process in parallel

// BMU mapping cache
let bmuMapping: Record<string, { name: string, leadParty: string }> | null = null;

/**
 * Load the BMU mapping file once
 */
async function loadBmuMapping(): Promise<Record<string, { name: string, leadParty: string }>> {
  if (bmuMapping) return bmuMapping;
  
  try {
    console.log('Loading BMU mapping from data/bmu_mapping.json...');
    const mappingFile = await fs.readFile(path.join('data', 'bmu_mapping.json'), 'utf-8');
    bmuMapping = JSON.parse(mappingFile);
    console.log(`Loaded ${Object.keys(bmuMapping).length} BMU mappings`);
    return bmuMapping;
  } catch (error) {
    console.error('Error loading BMU mapping:', error);
    throw error;
  }
}

/**
 * Filter for valid wind farm BMUs
 */
async function loadWindFarmIds(): Promise<Set<string>> {
  const mapping = await loadBmuMapping();
  const windFarmIds = new Set<string>();
  
  for (const [id, details] of Object.entries(mapping)) {
    windFarmIds.add(id);
  }
  
  console.log(`Loaded ${windFarmIds.size} wind farm BMU IDs`);
  return windFarmIds;
}

/**
 * Process a single settlement period with retries
 */
async function processSettlementPeriod(
  date: string, 
  period: number, 
  validWindFarmIds: Set<string>,
  retryCount: number = 0
): Promise<{
  records: number;
  volume: number;
  payment: number;
}> {
  try {
    const records = await fetchBidsOffers(date, period);
    
    if (!records || records.length === 0) {
      console.log(`[${date} P${period}] No records found`);
      return { records: 0, volume: 0, payment: 0 };
    }
    
    // Filter for valid curtailment records (negative volume, flagged, valid wind farm)
    const validRecords = records.filter(record => 
      record.volume < 0 &&
      (record.soFlag || record.cadlFlag) &&
      validWindFarmIds.has(record.id)
    );
    
    if (validRecords.length === 0) {
      console.log(`[${date} P${period}] No valid curtailment records found`);
      return { records: 0, volume: 0, payment: 0 };
    }
    
    // Log the records we're about to process
    const totalVolume = validRecords.reduce((sum, r) => sum + Math.abs(r.volume), 0);
    const totalPayment = validRecords.reduce((sum, r) => sum + Math.abs(r.payment), 0);
    console.log(`[${date} P${period}] Records: ${validRecords.length} (${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)})`);
    
    // Insert all records in a single batch
    const batchInserts = validRecords.map(record => {
      // Store payment values as negative in the database (they're positive in API)
      const paymentValue = -Math.abs(record.payment);
      
      return {
        id: `${date}_${period}_${record.id}_${Math.abs(record.volume)}`,
        settlementDate: date,
        settlementPeriod: period,
        bmUnitId: record.id,
        volume: record.volume,
        payment: paymentValue.toString(),
        acceptanceNumber: record.acceptanceNumber || null,
        soFlag: record.soFlag || false,
        cadlFlag: record.cadlFlag || false,
        leadPartyName: record.leadPartyName || null,
        bmUnitName: record.bmUnitName || null,
        timestamp: addMinutes(new Date(`${date}T00:00:00Z`), (period - 1) * 30)
      };
    });
    
    // Insert all records
    await db.insert(curtailmentRecords).values(batchInserts)
      .onConflictDoNothing({
        target: [curtailmentRecords.id]
      });
    
    return {
      records: validRecords.length,
      volume: totalVolume,
      payment: totalPayment
    };
  } catch (error) {
    console.error(`Error processing period ${period} for ${date}:`, error);
    
    // Retry logic
    if (retryCount < MAX_RETRIES) {
      console.log(`Retrying period ${period} for ${date} (attempt ${retryCount + 1} of ${MAX_RETRIES})...`);
      await new Promise(resolve => setTimeout(resolve, RETRY_DELAY_MS));
      return processSettlementPeriod(date, period, validWindFarmIds, retryCount + 1);
    }
    
    console.error(`Failed to process period ${period} for ${date} after ${MAX_RETRIES} attempts`);
    return { records: 0, volume: 0, payment: 0 };
  }
}

/**
 * Process all 48 settlement periods for a specific date
 */
export async function processAllPeriods(date: string): Promise<{
  totalRecords: number;
  totalPeriods: number;
  totalVolume: number;
  totalPayment: number;
}> {
  console.log(`\n=== Processing All Periods for ${date} ===\n`);
  
  // Load wind farm IDs
  const validWindFarmIds = await loadWindFarmIds();
  
  // First, clear existing records for the date to avoid duplicates
  console.log(`Clearing existing records for ${date}...`);
  await db.delete(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date));
  
  // Process in small batches to avoid hitting API rate limits
  let totalRecords = 0;
  let periodsProcessed = 0;
  let totalVolume = 0;
  let totalPayment = 0;
  
  // Create a queue of all 48 periods
  const allPeriods = Array.from({ length: 48 }, (_, i) => i + 1);
  
  // Process in small batches
  for (let i = 0; i < allPeriods.length; i += SMALL_BATCH_SIZE) {
    const batchPeriods = allPeriods.slice(i, i + SMALL_BATCH_SIZE);
    console.log(`Processing periods ${batchPeriods.join(', ')}...`);
    
    // Process each period in the batch concurrently
    const batchResults = await Promise.all(
      batchPeriods.map(async period => {
        // Add a small delay between API calls to avoid rate limiting
        await new Promise(resolve => setTimeout(resolve, API_RATE_LIMIT_DELAY_MS));
        return processSettlementPeriod(date, period, validWindFarmIds);
      })
    );
    
    // Aggregate results from this batch
    for (const result of batchResults) {
      if (result.records > 0) {
        totalRecords += result.records;
        periodsProcessed++;
        totalVolume += result.volume;
        totalPayment += result.payment;
      }
    }
    
    console.log(`Progress: ${periodsProcessed}/48 periods processed (${totalRecords} records)`);
    
    // Add a delay between batches to avoid API rate limits
    if (i + SMALL_BATCH_SIZE < allPeriods.length) {
      console.log(`Waiting ${API_RATE_LIMIT_DELAY_MS}ms before next batch...`);
      await new Promise(resolve => setTimeout(resolve, API_RATE_LIMIT_DELAY_MS * 3));
    }
  }
  
  // Update daily summary
  if (totalRecords > 0) {
    console.log(`\nUpdating daily summary for ${date}...`);
    
    // Payment values are stored as negative in the database, but displayed as positive in logs
    await db.insert(dailySummaries).values({
      summaryDate: date,
      totalCurtailedEnergy: totalVolume.toString(),
      totalPayment: (-totalPayment).toString(), // Store as negative in the database
      totalWindGeneration: '0',
      windOnshoreGeneration: '0',
      windOffshoreGeneration: '0',
      lastUpdated: new Date()
    }).onConflictDoUpdate({
      target: [dailySummaries.summaryDate],
      set: {
        totalCurtailedEnergy: totalVolume.toString(),
        totalPayment: (-totalPayment).toString(), // Store as negative in the database
        lastUpdated: new Date()
      }
    });
    
    console.log(`Daily summary updated for ${date}:`);
    console.log(`- Energy: ${totalVolume.toFixed(2)} MWh`);
    console.log(`- Payment: £${totalPayment.toFixed(2)}`);
  }
  
  console.log(`\n=== Processing Summary for ${date} ===`);
  console.log(`Total Records: ${totalRecords}`);
  console.log(`Periods Processed: ${periodsProcessed}/48`);
  console.log(`Total Volume: ${totalVolume.toFixed(2)} MWh`);
  console.log(`Total Payment: £${totalPayment.toFixed(2)}`);
  
  return {
    totalRecords,
    totalPeriods: periodsProcessed,
    totalVolume,
    totalPayment
  };
}

/**
 * Main function
 */
async function main() {
  try {
    // Get the date from command-line arguments or use default
    const dateToProcess = process.argv[2] || format(new Date(), 'yyyy-MM-dd');
    
    const result = await processAllPeriods(dateToProcess);
    
    console.log(`\n=== Processing Complete for ${dateToProcess} ===\n`);
    
    if (result.totalPeriods > 0) {
      console.log(`Next steps:`);
      console.log(`1. Process Bitcoin calculations with optimized DynamoDB access:`);
      console.log(`   npx tsx process_bitcoin_optimized.ts ${dateToProcess}`);
      console.log(`2. Or update the full cascade (Bitcoin, monthly, yearly summaries):`);
      console.log(`   npx tsx process_complete_cascade.ts ${dateToProcess}`);
    } else {
      console.log(`No curtailment data found for ${dateToProcess}`);
    }
  } catch (error) {
    console.error('Error processing all periods:', error);
    process.exit(1);
  }
}

main();