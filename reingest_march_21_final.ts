/**
 * March 21, 2025 Final Reingest Script
 * 
 * This script processes the final missing periods for March 21, 2025.
 * Target values for reconciliation:
 * - Subsidies Paid: £1,240,439.58
 * - Energy Curtailed: 50,518.72 MWh
 */

import { db } from './db';
import { eq, and, between, sql, inArray } from 'drizzle-orm';
import pg from 'pg';
import * as fs from 'fs';
import * as path from 'path';
import axios from 'axios';
import { curtailmentRecords } from './db/schema';

const { Pool } = pg;

// Configuration
const TARGET_DATE = '2025-03-21';
// Only process the 8 missing periods
const MISSING_PERIODS = [29, 30, 31, 32, 45, 46, 47, 48];
const API_THROTTLE_MS = 500; // Time to wait between API calls to avoid rate limiting

// Initialize database pool with more generous timeout
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: 10,
  idleTimeoutMillis: 60000,
  statement_timeout: 60000
});

/**
 * Log a message to console
 */
function log(message: string, type: "info" | "success" | "warning" | "error" = "info"): void {
  const timestamp = new Date().toISOString();
  let prefix = '';
  
  switch (type) {
    case "success":
      prefix = "✅";
      break;
    case "warning":
      prefix = "⚠️";
      break;
    case "error":
      prefix = "❌";
      break;
    default:
      prefix = "ℹ️";
  }
  
  console.log(`${prefix} ${timestamp} ${message}`);
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
 * Clear existing data for specific periods to avoid duplicates
 */
async function clearExistingPeriods(): Promise<void> {
  try {
    log(`Clearing existing data for ${TARGET_DATE} periods ${MISSING_PERIODS.join(', ')}...`, "info");
    
    // Use Drizzle ORM for the delete operation
    const deleteResult = await db.delete(curtailmentRecords)
      .where(
        and(
          eq(curtailmentRecords.settlementDate, TARGET_DATE),
          inArray(curtailmentRecords.settlementPeriod, MISSING_PERIODS)
        )
      )
      .returning({ id: curtailmentRecords.id });
    
    log(`Cleared ${deleteResult.length} existing records for ${TARGET_DATE} periods ${MISSING_PERIODS.join(', ')}`, "success");
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
      
      // Log total only to reduce output
      log(`[${TARGET_DATE} P${period}] Added ${recordsToInsert.length} records: ${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)}`, "success");
    }
    
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
      
      log(`Daily summary updated successfully`, "success");
    } finally {
      client.release();
    }
  } catch (error) {
    log(`Error updating summaries: ${error}`, "error");
    throw error;
  }
}

/**
 * Update Bitcoin mining calculations for the target date
 */
async function updateBitcoinCalculations(): Promise<void> {
  try {
    log(`\nUpdating Bitcoin calculations for ${TARGET_DATE}...`, "info");
    
    // List of miner models to process
    const minerModels = ['S19J_PRO', 'M20S', 'S9'];
    
    // Import the Bitcoin service
    const { processSingleDay } = await import('./server/services/bitcoinService');
    
    // Process each miner model
    for (const minerModel of minerModels) {
      log(`Processing ${TARGET_DATE} with model ${minerModel}...`);
      await processSingleDay(TARGET_DATE, minerModel);
      log(`Completed Bitcoin calculations for ${minerModel}`, "success");
    }
    
    log(`Bitcoin calculations updated`, "success");
  } catch (error) {
    log(`Error updating Bitcoin calculations: ${error}`, "error");
    throw error;
  }
}

/**
 * Main function to orchestrate the reingestion process
 */
async function main(): Promise<void> {
  const startTime = Date.now();
  
  log(`=== Starting final data reingest for ${TARGET_DATE} ===`, "info");
  log(`Target values for reconciliation:`, "info");
  log(`- Subsidies Paid: £1,240,439.58`, "info");
  log(`- Energy Curtailed: 50,518.72 MWh`, "info");
  log(`- Processing ${MISSING_PERIODS.length} missing periods: ${MISSING_PERIODS.join(', ')}`, "info");
  
  try {
    // Step 1: Load BMU mappings
    const { windFarmIds, bmuLeadPartyMap } = await loadBmuMappings();
    
    // Step 2: Clear existing data for these periods
    await clearExistingPeriods();
    
    // Step 3: Process the selected settlement periods
    let totalRecords = 0;
    let totalVolume = 0;
    let totalPayment = 0;
    
    // Process each missing period
    for (const period of MISSING_PERIODS) {
      try {
        const result = await processPeriod(period, windFarmIds, bmuLeadPartyMap);
        totalRecords += result.count;
        totalVolume += result.volume;
        totalPayment += result.payment;
        
        // Add delay between API calls to avoid rate limiting
        await delay(API_THROTTLE_MS);
      } catch (error) {
        log(`Error processing period ${period}: ${error}`, "error");
        // Continue with the next period despite the error
      }
    }
    
    log(`\nCompleted processing settlement periods:`, "success");
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
      // Get the total for the target date
      const totalResult = await client.query(`
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
      
      const totalVerification = totalResult.rows[0];
      
      log(`\nTotal Data for ${TARGET_DATE}:`, "info");
      log(`- Records: ${totalVerification.record_count}`, "info");
      log(`- Periods: ${totalVerification.period_count} of 48`, "info");
      log(`- Volume: ${Number(totalVerification.total_volume).toFixed(2)} MWh (Target: 50,518.72 MWh)`, "info");
      log(`- Payment: £${Number(totalVerification.total_payment).toFixed(2)} (Target: £1,240,439.58)`, "info");
      
      if (Number(totalVerification.period_count) === 48) {
        log(`\n✅ All 48 settlement periods are now processed!`, "success");
      } else {
        log(`\n⚠️ Some settlement periods are still missing (${48 - Number(totalVerification.period_count)} of 48)`, "warning");
      }
    } finally {
      client.release();
    }
    
    const duration = (Date.now() - startTime) / 1000;
    log(`\n=== Final reingest completed in ${duration.toFixed(1)} seconds ===`, "success");
  } catch (error) {
    log(`Fatal error during reingest: ${error}`, "error");
    process.exit(1);
  } finally {
    // Close the database pool
    await pool.end();
  }
}

// Run the script
main().catch(error => {
  console.error('Unhandled error:', error);
  process.exit(1);
});