/**
 * March 21, 2025 Reingest Script
 * 
 * This script is designed to reingest all settlement period data for March 21, 2025.
 * It will clear all existing data for the date and fetch new data from the Elexon API.
 * The target values for reconciliation are:
 * - Subsidies Paid: £1,240,439.58
 * - Energy Curtailed: 50,518.72 MWh
 */

import { db } from './db';
import { eq, and, sql, desc } from 'drizzle-orm';
import pg from 'pg';
import * as fs from 'fs';
import * as path from 'path';
import axios from 'axios';
import { curtailmentRecords } from './db/schema';

const { Pool } = pg;

// Configuration
const TARGET_DATE = '2025-03-21';
const BATCH_SIZE = 6; // Optimal batch size to avoid timeouts
const LOG_FILE = `reingest_${TARGET_DATE}.log`;
const API_THROTTLE_MS = 1000; // Time to wait between API calls to avoid rate limiting

// Initialize database pool with more generous timeout
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: 10,
  idleTimeoutMillis: 60000,
  statement_timeout: 60000
});

// Create a log file stream
const logStream = fs.createWriteStream(path.join(process.cwd(), LOG_FILE), { flags: 'a' });

/**
 * Log a message to both console and file
 */
function log(message: string, type: "info" | "success" | "warning" | "error" = "info"): void {
  const timestamp = new Date().toISOString();
  let prefix = '';
  
  switch (type) {
    case "success":
      prefix = "[SUCCESS]";
      break;
    case "warning":
      prefix = "[WARNING]";
      break;
    case "error":
      prefix = "[ERROR]";
      break;
    default:
      prefix = "[INFO]";
  }
  
  const formattedMessage = `${timestamp} ${prefix} ${message}`;
  console.log(formattedMessage);
  logStream.write(formattedMessage + '\n');
}

/**
 * Utility to delay execution
 */
async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Load BMU mappings from the mapping file
 */
async function loadBmuMappings(): Promise<{
  windFarmIds: Set<string>;
  bmuLeadPartyMap: Map<string, string>;
}> {
  try {
    // First try the optimized path used in newer scripts
    let bmuMappingPath = path.join(process.cwd(), 'data', 'bmu_mapping.json');
    
    // If not found, try the legacy path
    if (!fs.existsSync(bmuMappingPath)) {
      bmuMappingPath = path.join(process.cwd(), 'server', 'data', 'bmuMapping.json');
    }
    
    log(`Loading BMU mapping from: ${bmuMappingPath}`, "info");
    const data = await fs.promises.readFile(bmuMappingPath, 'utf8');
    const bmuMapping = JSON.parse(data);
    
    const windFarmIds = new Set<string>();
    const bmuLeadPartyMap = new Map<string, string>();
    
    for (const bmu of bmuMapping) {
      // Handle different property names in different mapping files
      const bmUnitId = bmu.elexonBmUnit || bmu.bmuId;
      const leadParty = bmu.leadPartyName || bmu.leadParty;
      
      if (bmUnitId) {
        windFarmIds.add(bmUnitId);
      }
      
      if (bmUnitId && leadParty) {
        bmuLeadPartyMap.set(bmUnitId, leadParty);
      }
    }
    
    log(`Found ${windFarmIds.size} wind farm BMUs`, "success");
    return { windFarmIds, bmuLeadPartyMap };
  } catch (error) {
    log(`Error loading BMU mapping: ${error}`, "error");
    return { windFarmIds: new Set(), bmuLeadPartyMap: new Map() };
  }
}

/**
 * Clear existing data for the target date to avoid duplicates
 */
async function clearExistingData(): Promise<void> {
  try {
    log(`Clearing existing data for ${TARGET_DATE}...`, "info");
    
    // Use Drizzle ORM for the delete operation
    const deleteResult = await db.delete(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE))
      .returning({ id: curtailmentRecords.id });
    
    log(`Cleared ${deleteResult.length} existing records for ${TARGET_DATE}`, "success");
  } catch (error) {
    log(`Error clearing existing data: ${error}`, "error");
    throw error;
  }
}

/**
 * Process a single settlement period by fetching data from Elexon API
 */
async function processPeriod(
  period: number,
  windFarmIds: Set<string>,
  bmuLeadPartyMap: Map<string, string>
): Promise<{
  count: number;
  volume: number;
  payment: number;
}> {
  try {
    log(`Processing period ${period}...`, "info");
    
    // Define API endpoints for bids and offers
    const bidsUrl = `https://data.elexon.co.uk/bmrs/api/v1/balancing/settlement/stack/all/bid/${TARGET_DATE}/${period}`;
    const offersUrl = `https://data.elexon.co.uk/bmrs/api/v1/balancing/settlement/stack/all/offer/${TARGET_DATE}/${period}`;
    
    // Fetch data from the Elexon API in parallel
    const [bidsResponse, offersResponse] = await Promise.all([
      axios.get(bidsUrl, {
        headers: { 'Accept': 'application/json' },
        timeout: 30000
      }),
      axios.get(offersUrl, {
        headers: { 'Accept': 'application/json' },
        timeout: 30000
      })
    ]);
    
    // Combine and process the data
    const bidsData = bidsResponse.data.data || [];
    const offersData = offersResponse.data.data || [];
    const allData = [...bidsData, ...offersData];
    
    // Filter for valid wind farm records (negative volume indicates curtailment)
    const validRecords = allData.filter(record => 
      windFarmIds.has(record.id) && 
      record.volume < 0 && 
      record.soFlag
    );
    
    // Calculate totals for this period
    const totalVolume = validRecords.reduce((sum, record) => sum + Math.abs(record.volume), 0);
    const totalPayment = validRecords.reduce((sum, record) => sum + (Math.abs(record.volume) * record.originalPrice), 0);
    
    log(`[${TARGET_DATE} P${period}] Found ${validRecords.length} valid records (${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)})`, "info");
    
    // Prepare records for insertion into the database
    const recordsToInsert = validRecords.map(record => {
      const volume = Math.abs(record.volume);
      const payment = volume * record.originalPrice;
      
      return {
        settlementDate: TARGET_DATE,
        settlementPeriod: period,
        farmId: record.id,
        leadPartyName: bmuLeadPartyMap.get(record.id) || 'Unknown',
        volume: record.volume.toString(), // Keep negative value to indicate curtailment
        payment: payment.toString(),
        originalPrice: record.originalPrice.toString(),
        finalPrice: record.finalPrice.toString(),
        soFlag: record.soFlag || false,
        cadlFlag: record.cadlFlag || false
      };
    });
    
    // Insert the records into the database
    if (recordsToInsert.length > 0) {
      await db.insert(curtailmentRecords).values(recordsToInsert);
      
      // Log individual records for visibility
      for (const record of recordsToInsert) {
        log(`[${TARGET_DATE} P${period}] Added record for ${record.farmId}: ${Math.abs(parseFloat(record.volume))} MWh, £${record.payment}`, "success");
      }
    }
    
    log(`[${TARGET_DATE} P${period}] Total: ${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)}`, "success");
    
    return {
      count: validRecords.length,
      volume: totalVolume,
      payment: totalPayment
    };
  } catch (error) {
    log(`Error processing period ${period}: ${error}`, "error");
    throw error;
  }
}

/**
 * Process a batch of settlement periods
 */
async function processBatch(
  periods: number[],
  windFarmIds: Set<string>,
  bmuLeadPartyMap: Map<string, string>
): Promise<{ count: number, volume: number, payment: number }> {
  let totalCount = 0;
  let totalVolume = 0;
  let totalPayment = 0;
  
  for (const period of periods) {
    try {
      const result = await processPeriod(period, windFarmIds, bmuLeadPartyMap);
      totalCount += result.count;
      totalVolume += result.volume;
      totalPayment += result.payment;
      
      // Add delay between API calls to avoid rate limiting
      await delay(API_THROTTLE_MS);
    } catch (error) {
      log(`Error processing period ${period}: ${error}`, "error");
      // Continue with the next period despite the error
    }
  }
  
  return { count: totalCount, volume: totalVolume, payment: totalPayment };
}

/**
 * Update daily, monthly, and yearly summaries
 */
async function updateSummaries(): Promise<void> {
  try {
    log(`\nUpdating summary tables...`, "info");
    
    const client = await pool.connect();
    try {
      // 1. Update daily summary
      log(`Updating daily summary for ${TARGET_DATE}...`, "info");
      await client.query(`
        INSERT INTO daily_summaries (
          summary_date, 
          total_curtailed_energy, 
          total_payment,
          total_wind_generation,
          wind_onshore_generation,
          wind_offshore_generation
        )
        SELECT 
            settlement_date, 
            SUM(ABS(CAST(volume AS DECIMAL))), 
            SUM(CAST(payment AS DECIMAL)),
            0, -- total_wind_generation (will be updated by other process)
            0, -- wind_onshore_generation
            0  -- wind_offshore_generation
        FROM 
            curtailment_records
        WHERE 
            settlement_date = $1
        GROUP BY 
            settlement_date
        ON CONFLICT (summary_date) 
        DO UPDATE SET
            total_curtailed_energy = EXCLUDED.total_curtailed_energy,
            total_payment = EXCLUDED.total_payment,
            last_updated = NOW()
      `, [TARGET_DATE]);
      
      // 2. Update monthly summary
      const yearMonth = TARGET_DATE.substring(0, 7); // YYYY-MM format
      log(`Updating monthly summary for ${yearMonth}...`, "info");
      await client.query(`
        INSERT INTO monthly_summaries (
          year_month, 
          total_curtailed_energy, 
          total_payment
        )
        SELECT 
            SUBSTRING(settlement_date, 1, 7), 
            SUM(ABS(CAST(volume AS DECIMAL))), 
            SUM(CAST(payment AS DECIMAL))
        FROM 
            curtailment_records
        WHERE 
            SUBSTRING(settlement_date, 1, 7) = $1
        GROUP BY 
            SUBSTRING(settlement_date, 1, 7)
        ON CONFLICT (year_month) 
        DO UPDATE SET
            total_curtailed_energy = EXCLUDED.total_curtailed_energy,
            total_payment = EXCLUDED.total_payment,
            last_updated = NOW()
      `, [yearMonth]);
      
      // 3. Update yearly summary
      const year = TARGET_DATE.substring(0, 4); // YYYY format
      log(`Updating yearly summary for ${year}...`, "info");
      await client.query(`
        INSERT INTO yearly_summaries (
          year, 
          total_curtailed_energy, 
          total_payment
        )
        SELECT 
            SUBSTRING(settlement_date, 1, 4), 
            SUM(ABS(CAST(volume AS DECIMAL))), 
            SUM(CAST(payment AS DECIMAL))
        FROM 
            curtailment_records
        WHERE 
            SUBSTRING(settlement_date, 1, 4) = $1
        GROUP BY 
            SUBSTRING(settlement_date, 1, 4)
        ON CONFLICT (year) 
        DO UPDATE SET
            total_curtailed_energy = EXCLUDED.total_curtailed_energy,
            total_payment = EXCLUDED.total_payment,
            last_updated = NOW()
      `, [year]);
      
      log(`Summary tables updated successfully`, "success");
    } finally {
      client.release();
    }
  } catch (error) {
    log(`Error updating summaries: ${error}`, "error");
    throw error;
  }
}

/**
 * Update Bitcoin mining calculations
 */
async function updateBitcoinCalculations(): Promise<void> {
  try {
    log(`\nUpdating Bitcoin calculations for ${TARGET_DATE}...`, "info");
    
    // List of miner models to process
    const minerModels = ['S19J_PRO', 'S9', 'M20S'];
    
    // Import the Bitcoin service
    const { processSingleDay } = await import('./server/services/bitcoinService');
    
    // Process each miner model
    for (const minerModel of minerModels) {
      log(`Processing ${TARGET_DATE} with model ${minerModel}...`);
      await processSingleDay(TARGET_DATE, minerModel);
      log(`Completed Bitcoin calculations for ${minerModel}`, "success");
    }
    
    log(`Bitcoin calculations updated for all models: ${minerModels.join(', ')}`, "success");
    
    // Update monthly summaries
    const yearMonth = TARGET_DATE.substring(0, 7); // YYYY-MM format
    const bitcoinService = await import('./server/services/bitcoinService');
    await bitcoinService.manualUpdateMonthlyBitcoinSummary(yearMonth);
    log(`Updated monthly Bitcoin summary for ${yearMonth}`, "success");
    
    // Update yearly summaries
    const year = TARGET_DATE.substring(0, 4); // YYYY format
    await bitcoinService.manualUpdateYearlyBitcoinSummary(year);
    log(`Updated yearly Bitcoin summary for ${year}`, "success");
  } catch (error) {
    log(`Error updating Bitcoin calculations: ${error}`, "error");
    throw error;
  }
}

/**
 * Main function to orchestrate the entire reingestion process
 */
async function main(): Promise<void> {
  const startTime = Date.now();
  
  log(`=== Starting data reingest for ${TARGET_DATE} ===`, "info");
  log(`Target values for reconciliation:`, "info");
  log(`- Subsidies Paid: £1,240,439.58`, "info");
  log(`- Energy Curtailed: 50,518.72 MWh`, "info");
  
  try {
    // Step 1: Load BMU mappings
    const { windFarmIds, bmuLeadPartyMap } = await loadBmuMappings();
    
    // Step 2: Clear existing data
    await clearExistingData();
    
    // Step 3: Process all 48 settlement periods in batches
    let totalRecords = 0;
    let totalVolume = 0;
    let totalPayment = 0;
    
    // Create batches of settlement periods
    const allPeriods = Array.from({ length: 48 }, (_, i) => i + 1);
    const batches = [];
    for (let i = 0; i < allPeriods.length; i += BATCH_SIZE) {
      batches.push(allPeriods.slice(i, i + BATCH_SIZE));
    }
    
    // Process each batch
    for (let i = 0; i < batches.length; i++) {
      log(`Processing batch ${i + 1} of ${batches.length} (periods ${batches[i][0]}-${batches[i][batches[i].length - 1]})`, "info");
      
      const batchResult = await processBatch(batches[i], windFarmIds, bmuLeadPartyMap);
      totalRecords += batchResult.count;
      totalVolume += batchResult.volume;
      totalPayment += batchResult.payment;
      
      // Small delay between batches
      await delay(500);
    }
    
    log(`\nCompleted processing all settlement periods:`, "success");
    log(`- Total records: ${totalRecords}`, "info");
    log(`- Total volume: ${totalVolume.toFixed(2)} MWh`, "info");
    log(`- Total payment: £${totalPayment.toFixed(2)}`, "info");
    
    // Step 4: Update summary tables
    await updateSummaries();
    
    // Step 5: Update Bitcoin calculations
    await updateBitcoinCalculations();
    
    // Step 6: Verify the results
    const client = await pool.connect();
    try {
      // Get the final figures from the database
      const result = await client.query(`
        SELECT 
          COUNT(*) as record_count,
          COUNT(DISTINCT settlement_period) as period_count,
          SUM(ABS(CAST(volume AS DECIMAL))) as total_volume,
          SUM(CAST(payment AS DECIMAL)) as total_payment
        FROM 
          curtailment_records
        WHERE 
          settlement_date = $1
      `, [TARGET_DATE]);
      
      const verificationResult = result.rows[0];
      
      log(`\nVerification Results for ${TARGET_DATE}:`, "info");
      log(`- Records: ${verificationResult.record_count}`, "info");
      log(`- Periods: ${verificationResult.period_count}`, "info");
      log(`- Volume: ${Number(verificationResult.total_volume).toFixed(2)} MWh (Target: 50,518.72 MWh)`, "info");
      log(`- Payment: £${Number(verificationResult.total_payment).toFixed(2)} (Target: £1,240,439.58)`, "info");
      
      // Check if we hit the targets
      const volumeDiff = Math.abs(Number(verificationResult.total_volume) - 50518.72);
      const paymentDiff = Math.abs(Number(verificationResult.total_payment) - 1240439.58);
      
      const volumeAccuracy = 100 - (volumeDiff / 50518.72 * 100);
      const paymentAccuracy = 100 - (paymentDiff / 1240439.58 * 100);
      
      log(`\nAccuracy Check:`, "info");
      log(`- Volume Accuracy: ${volumeAccuracy.toFixed(2)}%`, volumeAccuracy > 99 ? "success" : "warning");
      log(`- Payment Accuracy: ${paymentAccuracy.toFixed(2)}%`, paymentAccuracy > 99 ? "success" : "warning");
      
      if (volumeAccuracy > 99 && paymentAccuracy > 99) {
        log(`✅ Reingest successful! Values match the expected targets.`, "success");
      } else {
        log(`⚠️ Reingest completed, but values don't match targets exactly.`, "warning");
      }
    } finally {
      client.release();
    }
    
    const duration = (Date.now() - startTime) / 1000;
    log(`\n=== Reingest completed in ${duration.toFixed(1)} seconds ===`, "success");
    log(`Data for ${TARGET_DATE} has been successfully reingested and processed.`, "success");
  } catch (error) {
    log(`Fatal error during reingest: ${error}`, "error");
    process.exit(1);
  } finally {
    // Close the database pool and log file
    await pool.end();
    logStream.end();
  }
}

// Run the script
main().catch(error => {
  console.error('Unhandled error:', error);
  process.exit(1);
});