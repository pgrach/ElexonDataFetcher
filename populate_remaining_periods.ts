/**
 * Targeted period processor for March 21, 2025
 * 
 * This script focuses specifically on processing a targeted range of periods
 * (periods 13-24) for March 21, 2025, without attempting to process everything
 * or delete existing data.
 */

import pkg from 'pg';
const { Pool } = pkg;
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Configuration
const TARGET_DATE = '2025-03-21';
const START_PERIOD = 39;
const END_PERIOD = 48;
const LOG_FILE = `targeted_periods_${TARGET_DATE}.log`;
const API_THROTTLE_MS = 500; // Time to wait between API calls to avoid rate limiting
const BMU_MAPPING_PATH = path.join(__dirname, "data/bmu_mapping.json");

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
  bmuMap: Map<string, string>,
  bmuLeadPartyMap: Map<string, string>
}> {
  try {
    log(`Loading BMU mapping from: ${BMU_MAPPING_PATH}`);
    const bmuMappingData = JSON.parse(fs.readFileSync(BMU_MAPPING_PATH, 'utf-8'));
    
    const bmuMap = new Map<string, string>();
    const bmuLeadPartyMap = new Map<string, string>();
    
    for (const farm of bmuMappingData) {
      if (farm.elexonBmUnit) {
        // Use the BMU ID itself as the farm ID if id field is not available
        bmuMap.set(farm.elexonBmUnit, farm.id || farm.elexonBmUnit);
      }
      if (farm.elexonBmUnit && farm.leadPartyName) {
        bmuLeadPartyMap.set(farm.elexonBmUnit, farm.leadPartyName);
      }
    }
    
    log(`Loaded ${bmuMap.size} BMU mappings`, "success");
    return { bmuMap, bmuLeadPartyMap };
  } catch (error) {
    log(`Failed to load BMU mappings: ${error}`, "error");
    throw error;
  }
}

/**
 * Collects data for records to insert 
 */
interface RecordData {
  settlement_date: string;
  settlement_period: number;
  farm_id: string;
  lead_party_name: string;
  volume: number;
  price: number;
  payment: number;
  original_price: number;
  final_price: number;
  so_flag: boolean;
}

/**
 * Process a single settlement period by fetching data from Elexon API 
 */
async function processPeriod(
  period: number,
  bmuMap: Map<string, string>,
  bmuLeadPartyMap: Map<string, string>
): Promise<{ count: number, volume: number, payment: number, records: RecordData[] }> {
  try {
    log(`[${TARGET_DATE} P${period}] Processing settlement period ${period}...`);
    
    // Import the Elexon service
    const { fetchBidsOffers } = await import('./server/services/elexon.js');
    
    // Fetch data using the existing service
    const data = await fetchBidsOffers(TARGET_DATE, period);
    
    let recordCount = 0;
    let totalVolume = 0;
    let totalPayment = 0;
    const records: RecordData[] = [];
    
    // Process each record
    for (const item of data) {
      // Extract BMU ID - in the Elexon service, valid records have "id" containing the BMU ID
      const bmuId = item.id?.trim();
      if (!bmuId || !bmuMap.has(bmuId)) continue;
      
      // Get farm ID from the BMU mapping
      const farmId = bmuMap.get(bmuId)!;
      const leadParty = item.leadPartyName || bmuLeadPartyMap.get(bmuId) || 'Unknown';
      
      // Extract volume and payment data
      const volume = Math.abs(parseFloat(item.volume.toString()));
      const price = parseFloat(item.originalPrice.toString());
      const payment = -1 * volume * price; // Negative because payments are costs
      
      if (isNaN(volume) || isNaN(payment) || volume === 0) continue;
      
      // Collect record data
      records.push({
        settlement_date: TARGET_DATE,
        settlement_period: period,
        farm_id: farmId,
        lead_party_name: leadParty,
        volume,
        price: price,
        payment,
        original_price: price,
        final_price: price,
        so_flag: true
      });
      
      recordCount++;
      totalVolume += volume;
      totalPayment += payment;
      
      // Log the record that would be inserted
      log(`[${TARGET_DATE} P${period}] Will add record for ${farmId}: ${volume.toFixed(2)} MWh, £${payment.toFixed(2)}`);
    }
    
    log(`[${TARGET_DATE} P${period}] Total: ${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)}`);
    return { count: recordCount, volume: totalVolume, payment: totalPayment, records };
  } catch (error) {
    log(`Failed to process period ${period}: ${error}`, "error");
    throw error;
  }
}

/**
 * Insert records for a single period with robust error handling
 */
async function insertPeriodRecords(period: number, records: RecordData[]): Promise<boolean> {
  try {
    if (records.length === 0) {
      log(`No records to insert for period ${period}`, "warning");
      return false;
    }
    
    log(`Inserting ${records.length} records for period ${period}...`);
    
    // First, check if we already have records for this period
    const client = await pool.connect();
    try {
      const existingRecords = await client.query(
        'SELECT COUNT(*) FROM curtailment_records WHERE settlement_date = $1 AND settlement_period = $2',
        [TARGET_DATE, period]
      );
      
      if (parseInt(existingRecords.rows[0].count) > 0) {
        log(`Period ${period} already has ${existingRecords.rows[0].count} records, skipping.`, "warning");
        return false;
      }
      
      // Begin transaction for period insert
      await client.query('BEGIN');
      
      // Prepare the values for the bulk insert
      const values = records.map(r => {
        const escapedLeadParty = r.lead_party_name.replace(/'/g, "''"); // Escape single quotes in string
        return `('${r.settlement_date}', ${r.settlement_period}, '${r.farm_id}', '${escapedLeadParty}', ${r.volume}, ${r.payment}, ${r.original_price}, ${r.final_price}, ${r.so_flag}, NOW())`;
      }).join(',');
      
      // Insert all records for this period at once
      const query = `
        INSERT INTO curtailment_records 
        (settlement_date, settlement_period, farm_id, lead_party_name, volume, payment, original_price, final_price, so_flag, created_at)
        VALUES ${values}
      `;
      
      await client.query(query);
      await client.query('COMMIT');
      
      log(`Successfully inserted ${records.length} records for period ${period}`, "success");
      return true;
    } catch (error) {
      await client.query('ROLLBACK');
      log(`Transaction failed for period ${period}, rolling back: ${error}`, 'error');
      return false;
    } finally {
      client.release();
    }
  } catch (error) {
    log(`Error inserting data for period ${period}: ${error}`, "error");
    return false;
  }
}

/**
 * Update daily, monthly, and yearly summaries
 */
async function updateSummaries(): Promise<void> {
  try {
    log(`[${TARGET_DATE}] Updating summaries...`);
    
    const client = await pool.connect();
    try {
      // Step 1: Update daily_summaries
      await client.query(
        `INSERT INTO daily_summaries (summary_date, total_curtailed_energy, total_payment, created_at, last_updated)
         SELECT 
           settlement_date, 
           ROUND(SUM(ABS(volume))::numeric, 2) as total_curtailed_energy, 
           ROUND(SUM(payment)::numeric, 2) as total_payment,
           NOW(), 
           NOW()
         FROM curtailment_records
         WHERE settlement_date = $1
         GROUP BY settlement_date
         ON CONFLICT (summary_date) DO UPDATE SET
           total_curtailed_energy = EXCLUDED.total_curtailed_energy,
           total_payment = EXCLUDED.total_payment,
           last_updated = NOW()`,
        [TARGET_DATE]
      );
      
      // Step 2: Extract year and month from the target date
      const date = new Date(TARGET_DATE);
      const year = date.getUTCFullYear().toString();
      const month = (date.getUTCMonth() + 1).toString().padStart(2, '0');
      const yearMonth = `${year}-${month}`;
      
      // Step 3: Update monthly_summaries
      await client.query(
        `INSERT INTO monthly_summaries (year_month, total_curtailed_energy, total_payment, created_at, updated_at)
         SELECT 
           SUBSTRING(summary_date::text, 1, 7) as year_month, 
           ROUND(SUM(total_curtailed_energy)::numeric, 2) as total_curtailed_energy, 
           ROUND(SUM(total_payment)::numeric, 2) as total_payment,
           NOW(), 
           NOW()
         FROM daily_summaries
         WHERE SUBSTRING(summary_date::text, 1, 7) = $1
         GROUP BY year_month
         ON CONFLICT (year_month) DO UPDATE SET
           total_curtailed_energy = EXCLUDED.total_curtailed_energy,
           total_payment = EXCLUDED.total_payment,
           updated_at = NOW()`,
        [yearMonth]
      );
      
      // Step 4: Update yearly_summaries
      await client.query(
        `INSERT INTO yearly_summaries (year, total_curtailed_energy, total_payment, created_at, updated_at)
         SELECT 
           SUBSTRING(summary_date::text, 1, 4) as year, 
           ROUND(SUM(total_curtailed_energy)::numeric, 2) as total_curtailed_energy, 
           ROUND(SUM(total_payment)::numeric, 2) as total_payment,
           NOW(), 
           NOW()
         FROM daily_summaries
         WHERE SUBSTRING(summary_date::text, 1, 4) = $1
         GROUP BY year
         ON CONFLICT (year) DO UPDATE SET
           total_curtailed_energy = EXCLUDED.total_curtailed_energy,
           total_payment = EXCLUDED.total_payment,
           updated_at = NOW()`,
        [year]
      );
      
      // Fetch updated values for verification
      const dailyResult = await client.query(
        'SELECT total_curtailed_energy, total_payment FROM daily_summaries WHERE summary_date = $1',
        [TARGET_DATE]
      );
      
      if (dailyResult.rows.length > 0) {
        const { total_curtailed_energy, total_payment } = dailyResult.rows[0];
        log(`[${TARGET_DATE}] Processing complete: { energy: '${total_curtailed_energy} MWh', payment: '£${total_payment}' }`, "success");
      }
    } finally {
      client.release();
    }
  } catch (error) {
    log(`Failed to update summaries: ${error}`, "error");
    throw error;
  }
}

/**
 * Update Bitcoin mining calculations
 */
async function updateBitcoinCalculations(): Promise<void> {
  try {
    log(`[${TARGET_DATE}] Updating Bitcoin calculations...`);
    
    // Import the bitcoinService to use the processSingleDay function
    const { processSingleDay } = await import('./server/services/bitcoinService.js');
    
    // List of miner models to process
    const minerModels = ['S19J_PRO', 'S9', 'M20S'];
    
    for (const minerModel of minerModels) {
      await processSingleDay(TARGET_DATE, minerModel);
      log(`Processed ${TARGET_DATE} with miner model ${minerModel}`);
    }
    
    log(`Bitcoin calculations updated successfully`, "success");
  } catch (error) {
    log(`Failed to update Bitcoin calculations: ${error}`, "error");
    throw error;
  }
}

/**
 * Process the targeted periods one by one
 */
async function processTargetedPeriods(): Promise<void> {
  const startTime = Date.now();
  let successCount = 0;
  let failureCount = 0;

  try {
    // Validate target date
    if (!TARGET_DATE.match(/^\d{4}-\d{2}-\d{2}$/)) {
      log('Invalid date format. Please use YYYY-MM-DD format.', "error");
      return;
    }
    
    log(`Starting targeted data processing for ${TARGET_DATE} (periods ${START_PERIOD}-${END_PERIOD})`);
    
    // Load BMU mappings
    const { bmuMap, bmuLeadPartyMap } = await loadBmuMappings();
    
    // Process each period individually
    for (let period = START_PERIOD; period <= END_PERIOD; period++) {
      try {
        log(`------- Processing period ${period} -------`);
        
        // Get the data for this period
        const result = await processPeriod(period, bmuMap, bmuLeadPartyMap);
        
        // Insert the records for this period
        const success = await insertPeriodRecords(period, result.records);
        
        if (success) {
          successCount++;
          log(`Period ${period} processed successfully with ${result.count} records (${result.volume.toFixed(2)} MWh, £${result.payment.toFixed(2)})`, "success");
        } else {
          failureCount++;
          log(`Failed to process period ${period}`, "warning");
        }
        
        // Add delay between periods to avoid rate limiting
        await delay(API_THROTTLE_MS);
      } catch (error) {
        failureCount++;
        log(`Error processing period ${period}: ${error}`, "error");
        // Continue with the next period despite the error
      }
    }
    
    // Update summary tables (daily, monthly, yearly)
    await updateSummaries();
    
    // Update Bitcoin calculations
    await updateBitcoinCalculations();
    
    // Final verification
    const client = await pool.connect();
    try {
      const verificationResult = await client.query(
        `SELECT 
          (SELECT COUNT(*) FROM curtailment_records WHERE settlement_date = $1) as records,
          (SELECT COUNT(DISTINCT settlement_period) FROM curtailment_records WHERE settlement_date = $1) as periods,
          (SELECT ROUND(SUM(ABS(volume))::numeric, 2) FROM curtailment_records WHERE settlement_date = $1) as volume,
          (SELECT ROUND(SUM(payment)::numeric, 2) FROM curtailment_records WHERE settlement_date = $1) as payment`,
        [TARGET_DATE]
      );
      
      log(`Verification Check for ${TARGET_DATE}: ${JSON.stringify(verificationResult.rows[0], null, 2)}`);
      
      // List the periods we have data for
      const periodsResult = await client.query(
        'SELECT settlement_period, COUNT(*) as record_count FROM curtailment_records WHERE settlement_date = $1 GROUP BY settlement_period ORDER BY settlement_period',
        [TARGET_DATE]
      );
      
      const existingPeriods = periodsResult.rows.map(r => r.settlement_period);
      log(`Periods in database: ${existingPeriods.join(', ')}`, "info");
      
      // Calculate missing periods
      const allPeriods = Array.from({ length: 48 }, (_, i) => i + 1);
      const missingPeriods = allPeriods.filter(p => !existingPeriods.includes(p));
      
      if (missingPeriods.length > 0) {
        log(`Missing periods: ${missingPeriods.join(', ')}`, "warning");
      } else {
        log(`All 48 settlement periods are now in the database!`, "success");
      }
    } finally {
      client.release();
    }
    
    const endTime = Date.now();
    const duration = ((endTime - startTime) / 1000).toFixed(1);
    
    log(`Processing completed at ${new Date().toISOString()} (${duration}s)`, "success");
    log(`=== Processing Summary ===`);
    log(`Target range: Periods ${START_PERIOD}-${END_PERIOD}`);
    log(`Successful periods: ${successCount}`);
    log(`Failed periods: ${failureCount}`);
  } catch (error) {
    log(`Critical error in processing: ${error}`, "error");
  } finally {
    // Close connections
    logStream.end();
    await pool.end();
  }
}

// Start the process
processTargetedPeriods();