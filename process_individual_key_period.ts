/**
 * Process Individual Key Period for March 28, 2025
 * 
 * This script processes a single key period (11, 25, or 37) for March 28, 2025.
 * It's designed to be simple and focused on just one task to avoid timeouts.
 * 
 * Usage: npx tsx process_individual_key_period.ts <period_number>
 * Example: npx tsx process_individual_key_period.ts 11
 */

import axios from 'axios';
import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';
import { db } from './db';
import { sql } from 'drizzle-orm';

// Get command line argument
const periodArg = process.argv[2];
if (!periodArg || isNaN(Number(periodArg))) {
  console.error('Error: Please provide a valid period number (11, 25, or 37)');
  process.exit(1);
}

const period = Number(periodArg);
if (![11, 25, 37].includes(period)) {
  console.error('Error: Period must be one of: 11, 25, or 37');
  process.exit(1);
}

// Configuration
const TARGET_DATE = '2025-03-28';
const API_BASE_URL = 'https://data.elexon.co.uk/bmrs/api/v1';

// ES module support for __dirname equivalent
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const BMU_MAPPING_PATH = path.join(__dirname, 'server', 'data', 'bmuMapping.json');

// Color console output
const colors = {
  info: '\x1b[36m',    // Cyan
  success: '\x1b[32m', // Green
  warning: '\x1b[33m', // Yellow
  error: '\x1b[31m',   // Red
  reset: '\x1b[0m'     // Reset
};

function log(message: string, type: 'info' | 'success' | 'warning' | 'error' = 'info'): void {
  const timestamp = new Date().toLocaleTimeString();
  const color = colors[type];
  const icon = type === 'info' ? 'ℹ' : 
              type === 'success' ? '✓' : 
              type === 'warning' ? '⚠' : 
              type === 'error' ? '✗' : '';
              
  console.log(`${color}${icon} [${timestamp}] ${message}${colors.reset}`);
}

/**
 * Load BMU mapping for wind farms
 */
async function loadBmuMappings(): Promise<{
  windFarmIds: Set<string>;
  bmuLeadPartyMap: Map<string, string>;
}> {
  try {
    log(`Loading BMU mapping from: ${BMU_MAPPING_PATH}`);
    const data = await fs.readFile(BMU_MAPPING_PATH, 'utf8');
    const bmuMapping = JSON.parse(data);
    
    // Create maps for quick lookups
    const windFarmIds = new Set<string>();
    const bmuLeadPartyMap = new Map<string, string>();
    
    for (const bmu of bmuMapping) {
      windFarmIds.add(bmu.elexonBmUnit);
      bmuLeadPartyMap.set(bmu.elexonBmUnit, bmu.leadPartyName);
    }
    
    log(`Found ${windFarmIds.size} wind farm BMUs`, 'success');
    return { windFarmIds, bmuLeadPartyMap };
  } catch (error) {
    log(`Error loading BMU mapping: ${error}`, 'error');
    return { windFarmIds: new Set(), bmuLeadPartyMap: new Map() };
  }
}

/**
 * Clear existing records for the period
 */
async function clearExistingRecords(period: number): Promise<void> {
  try {
    const result = await db.execute(sql`
      DELETE FROM curtailment_records
      WHERE settlement_date = ${TARGET_DATE}
      AND settlement_period = ${period}
    `);
    
    log(`Cleared existing records for period ${period}`);
  } catch (error) {
    log(`Error clearing records: ${error}`, 'error');
    throw error;
  }
}

/**
 * Fetch records from Elexon API
 */
async function fetchElexonRecords(period: number): Promise<any[]> {
  const bidUrl = `${API_BASE_URL}/balancing/settlement/stack/all/bid/${TARGET_DATE}/${period}`;
  const offerUrl = `${API_BASE_URL}/balancing/settlement/stack/all/offer/${TARGET_DATE}/${period}`;
  
  log(`Fetching data for period ${period}`);
  log(`Bid URL: ${bidUrl}`);
  log(`Offer URL: ${offerUrl}`);
  
  try {
    // Get bid records
    const bidResponse = await axios.get(bidUrl, {
      headers: { 'Accept': 'application/json' },
      timeout: 30000
    });
    
    // Get offer records
    const offerResponse = await axios.get(offerUrl, {
      headers: { 'Accept': 'application/json' },
      timeout: 30000
    });
    
    // Combine both arrays
    const bidRecords = bidResponse.data?.data || [];
    const offerRecords = offerResponse.data?.data || [];
    
    log(`Retrieved ${bidRecords.length} bid records and ${offerRecords.length} offer records`, 'success');
    
    return [...bidRecords, ...offerRecords];
  } catch (error) {
    log(`Error fetching Elexon data: ${error}`, 'error');
    throw error;
  }
}

/**
 * Filter for wind farm records with negative volume (curtailment)
 */
function filterWindFarmRecords(records: any[], windFarmIds: Set<string>): any[] {
  return records.filter(record => {
    return windFarmIds.has(record.id) && record.volume < 0;
  });
}

/**
 * Insert wind farm records into database
 */
async function insertWindFarmRecords(records: any[], bmuLeadPartyMap: Map<string, string>): Promise<void> {
  if (records.length === 0) {
    log('No wind farm records to insert');
    return;
  }
  
  let totalVolume = 0;
  let totalPayment = 0;
  
  for (const record of records) {
    const volume = Math.abs(record.volume); // Convert to positive value for storage
    const payment = record.finalPrice * Math.abs(record.volume); // Calculate payment (price * volume)
    totalVolume += volume;
    totalPayment += payment;
    
    try {
      await db.execute(sql`
        INSERT INTO curtailment_records (
          settlement_date,
          settlement_period,
          farm_id,
          lead_party_name,
          volume,
          payment,
          original_price,
          final_price,
          so_flag,
          cadl_flag
        )
        VALUES (
          ${TARGET_DATE},
          ${record.settlementPeriod},
          ${record.id},
          ${bmuLeadPartyMap.get(record.id) || 'Unknown'},
          ${volume},
          ${payment},
          ${record.originalPrice},
          ${record.finalPrice},
          ${record.soFlag},
          ${record.cadlFlag}
        )
      `);
      
      log(`Added ${record.id} (${volume.toFixed(2)} MWh, £${payment.toFixed(2)})`);
    } catch (error) {
      log(`Error inserting record for ${record.id}: ${error}`, 'error');
    }
  }
  
  log(`Period ${period} complete: ${records.length} records, ${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)}`, 'success');
}

/**
 * Update daily summary
 */
async function updateDailySummary(): Promise<void> {
  try {
    // Calculate total curtailed energy and total payment
    const totals = await db.execute(sql`
      SELECT 
        SUM(volume) AS total_volume, 
        SUM(payment) AS total_payment
      FROM curtailment_records
      WHERE settlement_date = ${TARGET_DATE}
    `);
    
    if (!totals.rows || totals.rows.length === 0) {
      log('No data to update summary with', 'warning');
      return;
    }
    
    const totalVolume = Number(totals.rows[0].total_volume) || 0;
    const totalPayment = Number(totals.rows[0].total_payment) || 0;
    
    // Check if a daily summary exists
    const existingSummary = await db.execute(sql`
      SELECT id FROM daily_summaries 
      WHERE date = ${TARGET_DATE}
    `);
    
    if (existingSummary.rows && existingSummary.rows.length > 0) {
      // Update existing summary
      await db.execute(sql`
        UPDATE daily_summaries 
        SET 
          curtailed_energy = ${totalVolume},
          total_payment = ${totalPayment},
          updated_at = NOW()
        WHERE date = ${TARGET_DATE}
      `);
    } else {
      // Create new summary
      await db.execute(sql`
        INSERT INTO daily_summaries (
          date, 
          curtailed_energy, 
          total_payment,
          created_at,
          updated_at
        )
        VALUES (
          ${TARGET_DATE}, 
          ${totalVolume}, 
          ${totalPayment},
          NOW(),
          NOW()
        )
      `);
    }
    
    log(`Daily summary updated for ${TARGET_DATE}:
- Energy: ${totalVolume.toFixed(2)} MWh
- Payment: £${totalPayment.toFixed(2)}`, 'success');
  } catch (error) {
    log(`Error updating daily summary: ${error}`, 'error');
  }
}

/**
 * Main function to process the period
 */
async function processPeriod(): Promise<void> {
  try {
    console.log(`\n=== Processing Period ${period} for ${TARGET_DATE} ===`);
    console.log(`Started at: ${new Date().toISOString()}`);
    
    // 1. Load BMU mappings
    const { windFarmIds, bmuLeadPartyMap } = await loadBmuMappings();
    
    // 2. Clear existing records for the period
    await clearExistingRecords(period);
    
    // 3. Fetch records from Elexon API
    const allRecords = await fetchElexonRecords(period);
    
    // 4. Filter for wind farm records with negative volume
    const windFarmRecords = filterWindFarmRecords(allRecords, windFarmIds);
    log(`Found ${windFarmRecords.length} wind farm records with negative volume`);
    
    // 5. Insert into database
    await insertWindFarmRecords(windFarmRecords, bmuLeadPartyMap);
    
    // 6. Update daily summary
    await updateDailySummary();
    
    log(`Period ${period} processing completed successfully`, 'success');
  } catch (error) {
    log(`Failed to process period ${period}: ${error}`, 'error');
  }
}

// Execute the process
processPeriod().catch(error => {
  log(`Unhandled error: ${error}`, 'error');
  process.exit(1);
});