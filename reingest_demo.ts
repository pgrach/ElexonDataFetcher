/**
 * Data Reingest Demo Script for 2025-03-21
 * 
 * This is a simplified version of the complete data reingestion process
 * specifically for demonstration purposes. It provides a streamlined
 * way to reprocess data for March 21, 2025.
 */

import pg from 'pg';
import * as fs from 'fs';
import * as path from 'path';

const { Pool } = pg;

// Configuration
const TARGET_DATE = '2025-03-21';
const BATCH_SIZE = 2;
const MAX_PERIODS = 4; // Demo only processes 4 periods instead of all 48 for faster execution
const LOG_FILE = `reingest_demo_${TARGET_DATE}.log`;
const API_THROTTLE_MS = 500;

// Initialize database pool
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: 10
});

// Create log file stream
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
 * Load BMU mappings for demonstration
 */
async function loadBmuMappings(): Promise<{
  bmuMap: Map<string, string>,
  bmuLeadPartyMap: Map<string, string>
}> {
  try {
    // Sample mapping data for demonstration
    const bmuMappingData = [
      { elexonBmUnit: 'T_ABRBO-1', id: 'farm-001', leadParty: 'SP Renewables Limited' },
      { elexonBmUnit: 'T_ACHYW-1', id: 'farm-002', leadParty: 'Orsted Wind Power A/S' },
      { elexonBmUnit: 'T_DDLHW-1', id: 'farm-003', leadParty: 'Orsted Wind Power A/S' },
      { elexonBmUnit: 'T_GWSWW-1', id: 'farm-004', leadParty: 'ESB Wind Development UK Limited' },
      { elexonBmUnit: 'T_MRHLW-1', id: 'farm-005', leadParty: 'RWE Renewables UK Limited' }
    ];
    
    const bmuMap = new Map<string, string>();
    const bmuLeadPartyMap = new Map<string, string>();
    
    for (const farm of bmuMappingData) {
      if (farm.elexonBmUnit && farm.id) {
        bmuMap.set(farm.elexonBmUnit, farm.id);
      }
      if (farm.elexonBmUnit && farm.leadParty) {
        bmuLeadPartyMap.set(farm.elexonBmUnit, farm.leadParty);
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
 * Clear existing data for the target date
 */
async function clearExistingData(): Promise<void> {
  const client = await pool.connect();
  try {
    log(`Clearing existing data for ${TARGET_DATE}...`);
    
    await client.query('BEGIN');
    
    // Get counts before deletion for logging
    const countResult = await client.query(
      'SELECT COUNT(*) as count FROM curtailment_records WHERE settlement_date = $1',
      [TARGET_DATE]
    );
    const recordCount = parseInt(countResult.rows[0].count || '0');
    
    // Delete from curtailment_records
    await client.query(
      'DELETE FROM curtailment_records WHERE settlement_date = $1',
      [TARGET_DATE]
    );
    log(`Deleted ${recordCount} curtailment records`);
    
    // Get counts before deletion for logging
    const bitcoinCountResult = await client.query(
      'SELECT COUNT(*) as count FROM historical_bitcoin_calculations WHERE settlement_date = $1',
      [TARGET_DATE]
    );
    const bitcoinRecordCount = parseInt(bitcoinCountResult.rows[0].count || '0');
    
    // Delete from historical_bitcoin_calculations
    await client.query(
      'DELETE FROM historical_bitcoin_calculations WHERE settlement_date = $1',
      [TARGET_DATE]
    );
    log(`Deleted ${bitcoinRecordCount} Bitcoin calculation records`);
    
    // Get counts before deletion for logging
    const dailySummaryCountResult = await client.query(
      'SELECT COUNT(*) as count FROM daily_summaries WHERE summary_date = $1',
      [TARGET_DATE]
    );
    const dailySummaryCount = parseInt(dailySummaryCountResult.rows[0].count || '0');
    
    // Delete from daily_summaries
    await client.query(
      'DELETE FROM daily_summaries WHERE summary_date = $1',
      [TARGET_DATE]
    );
    log(`Deleted ${dailySummaryCount} daily summary records`);
    
    await client.query('COMMIT');
    log(`Successfully cleared existing data for ${TARGET_DATE}`, "success");
  } catch (error) {
    await client.query('ROLLBACK');
    log(`Failed to clear existing data: ${error}`, "error");
    throw error;
  } finally {
    client.release();
  }
}

/**
 * Process a single settlement period with sample data
 */
async function processPeriod(
  period: number,
  bmuMap: Map<string, string>,
  bmuLeadPartyMap: Map<string, string>
): Promise<{ count: number, volume: number, payment: number }> {
  try {
    log(`[${TARGET_DATE} P${period}] Processing settlement period ${period}...`);
    
    // Sample data for demonstration - in a real scenario, this would come from an API
    const sampleData = `
T_ABRBO-1,Wind Offshore,SP Renewables Limited,75.0,-28.55
T_ACHYW-1,Wind Offshore,Orsted Wind Power A/S,129.6,-40.22
T_DDLHW-1,Wind Offshore,Orsted Wind Power A/S,85.3,-31.77
T_GWSWW-1,Wind Offshore,ESB Wind Development UK Limited,45.2,-19.88
T_MRHLW-1,Wind Offshore,RWE Renewables UK Limited,62.8,-25.13
`;

    const lines = sampleData.trim().split('\n');
    let recordCount = 0;
    let totalVolume = 0;
    let totalPayment = 0;
    
    // Process each line
    for (const line of lines) {
      try {
        const [bmuId, type, leadPartyFromData, volumeStr, priceStr] = line.split(',');
        
        // Check if BMU is in our mapping
        if (!bmuMap.has(bmuId)) {
          log(`Skipping unknown BMU ID: ${bmuId}`, "warning");
          continue;
        }
        
        // Get farm ID from the BMU mapping
        const farmId = bmuMap.get(bmuId);
        const leadParty = bmuLeadPartyMap.get(bmuId) || leadPartyFromData || 'Unknown';
        
        // Parse volume and price data
        const volume = Math.abs(parseFloat(volumeStr));
        const price = parseFloat(priceStr);
        const payment = -1 * volume * price; // Negative because payments are costs
        
        if (isNaN(volume) || isNaN(payment) || volume === 0) continue;
        
        // Insert record into curtailment_records table
        const client = await pool.connect();
        try {
          await client.query(
            `INSERT INTO curtailment_records 
             (settlement_date, settlement_period, farm_id, lead_party_name, volume, payment, original_price, final_price, created_at, so_flag, cadl_flag) 
             VALUES ($1, $2, $3, $4, $5, $6, $7, $7, NOW(), false, false)`,
            [TARGET_DATE, period, farmId, leadParty, volume, payment, price]
          );
          
          recordCount++;
          totalVolume += volume;
          totalPayment += payment;
          
          log(`[${TARGET_DATE} P${period}] Added record for ${farmId}: ${volume.toFixed(2)} MWh, £${payment.toFixed(2)}`);
        } finally {
          client.release();
        }
      } catch (error) {
        log(`Error processing data line: ${error}`, "error");
        continue;
      }
    }
    
    log(`[${TARGET_DATE} P${period}] Total: ${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)}`);
    return { count: recordCount, volume: totalVolume, payment: totalPayment };
  } catch (error) {
    log(`Failed to process period ${period}: ${error}`, "error");
    throw error;
  }
}

/**
 * Process a batch of settlement periods
 */
async function processBatch(
  periods: number[],
  bmuMap: Map<string, string>,
  bmuLeadPartyMap: Map<string, string>
): Promise<{ count: number, volume: number, payment: number }> {
  let totalCount = 0;
  let totalVolume = 0;
  let totalPayment = 0;
  
  for (const period of periods) {
    try {
      const result = await processPeriod(period, bmuMap, bmuLeadPartyMap);
      totalCount += result.count;
      totalVolume += result.volume;
      totalPayment += result.payment;
      
      // Add delay between periods to avoid rate limiting
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
    log(`[${TARGET_DATE}] Updating summaries...`);
    
    const client = await pool.connect();
    try {
      // Update daily_summaries
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
      
      // Extract year and month
      const date = new Date(TARGET_DATE);
      const year = date.getUTCFullYear().toString();
      const month = (date.getUTCMonth() + 1).toString().padStart(2, '0');
      const yearMonth = `${year}-${month}`;
      
      // Update monthly_summaries
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
      
      // Update yearly_summaries
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
      
      // Verification logging
      const dailyResult = await client.query(
        'SELECT total_curtailed_energy, total_payment FROM daily_summaries WHERE summary_date = $1',
        [TARGET_DATE]
      );
      
      if (dailyResult.rows.length > 0) {
        const { total_curtailed_energy, total_payment } = dailyResult.rows[0];
        log(`[${TARGET_DATE}] Summary updated: Energy=${total_curtailed_energy}MWh, Payment=£${total_payment}`, "success");
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
 * Update Bitcoin mining calculations for each miner model
 */
async function updateBitcoinCalculations(): Promise<void> {
  try {
    log(`[${TARGET_DATE}] Updating Bitcoin calculations...`);
    
    const minerModels = ['S19J_PRO', 'S9', 'M20S'];
    const difficulty = 113757508810853; // Example difficulty value
    
    const client = await pool.connect();
    try {
      for (const minerModel of minerModels) {
        // Get all periods for the date
        const periodsResult = await client.query(
          'SELECT DISTINCT settlement_period FROM curtailment_records WHERE settlement_date = $1 ORDER BY settlement_period',
          [TARGET_DATE]
        );
        
        const periods = periodsResult.rows.map(r => r.settlement_period);
        
        // Process each period
        let insertCount = 0;
        for (const period of periods) {
          // Get all farms for this period
          const farmsResult = await client.query(
            'SELECT DISTINCT farm_id FROM curtailment_records WHERE settlement_date = $1 AND settlement_period = $2',
            [TARGET_DATE, period]
          );
          
          for (const farmRow of farmsResult.rows) {
            // Get total energy for this farm in this period
            const energyResult = await client.query(
              'SELECT SUM(ABS(volume)) as total_energy FROM curtailment_records WHERE settlement_date = $1 AND settlement_period = $2 AND farm_id = $3',
              [TARGET_DATE, period, farmRow.farm_id]
            );
            
            const totalEnergy = parseFloat(energyResult.rows[0].total_energy);
            
            // Calculate Bitcoin mined based on energy and miner model
            let bitcoinMined = 0;
            switch (minerModel) {
              case 'S19J_PRO':
                // 100 TH/s at 3250W - approximately 0.007 BTC per MWh at current difficulty
                bitcoinMined = totalEnergy * 0.007 * (100000000000000 / difficulty);
                break;
              case 'S9':
                // 13.5 TH/s at 1323W - approximately 0.0025 BTC per MWh at current difficulty
                bitcoinMined = totalEnergy * 0.0025 * (13500000000000 / difficulty);
                break;
              case 'M20S':
                // 68 TH/s at 3360W - approximately 0.005 BTC per MWh at current difficulty
                bitcoinMined = totalEnergy * 0.005 * (68000000000000 / difficulty);
                break;
              default:
                bitcoinMined = 0;
            }
            
            // Insert calculation
            await client.query(
              `INSERT INTO historical_bitcoin_calculations 
               (settlement_date, settlement_period, farm_id, miner_model, bitcoin_mined, difficulty, calculated_at)
               VALUES ($1, $2, $3, $4, $5, $6, NOW())
               ON CONFLICT (settlement_date, settlement_period, farm_id, miner_model)
               DO UPDATE SET bitcoin_mined = $5, difficulty = $6, calculated_at = NOW()`,
              [TARGET_DATE, period, farmRow.farm_id, minerModel, bitcoinMined, difficulty]
            );
            
            insertCount++;
          }
        }
        
        log(`Inserted ${insertCount} records for ${TARGET_DATE} with ${minerModel}`);
      }
      
      // Update monthly Bitcoin summaries
      const date = new Date(TARGET_DATE);
      const year = date.getUTCFullYear().toString();
      const month = (date.getUTCMonth() + 1).toString().padStart(2, '0');
      const yearMonth = `${year}-${month}`;
      
      for (const minerModel of minerModels) {
        // Sum up all Bitcoin mined for this month
        const result = await client.query(
          `SELECT SUM(bitcoin_mined) as total_bitcoin
           FROM historical_bitcoin_calculations 
           WHERE TO_CHAR(settlement_date, 'YYYY-MM') = $1 AND miner_model = $2`,
          [yearMonth, minerModel]
        );
        
        const totalBitcoin = parseFloat(result.rows[0].total_bitcoin || '0');
        
        // Update monthly_bitcoin_summaries
        await client.query(
          `INSERT INTO monthly_bitcoin_summaries (year_month, miner_model, bitcoin_mined, created_at, updated_at)
           VALUES ($1, $2, $3, NOW(), NOW())
           ON CONFLICT (year_month, miner_model) DO UPDATE SET
           bitcoin_mined = $3,
           updated_at = NOW()`,
          [yearMonth, minerModel, totalBitcoin]
        );
        
        log(`Updated monthly Bitcoin summary for ${yearMonth} with ${minerModel}: ${totalBitcoin.toFixed(8)} BTC`);
      }
      
      // Update yearly Bitcoin summaries
      for (const minerModel of minerModels) {
        // Get monthly summaries for this year
        const monthlyResult = await client.query(
          `SELECT bitcoin_mined FROM monthly_bitcoin_summaries 
           WHERE year_month LIKE $1 AND miner_model = $2`,
          [`${year}-%`, minerModel]
        );
        
        // Sum up all Bitcoin mined this year
        let yearlyTotal = 0;
        for (const row of monthlyResult.rows) {
          yearlyTotal += parseFloat(row.bitcoin_mined || '0');
        }
        
        // Update yearly_bitcoin_summaries
        await client.query(
          `INSERT INTO yearly_bitcoin_summaries (year, miner_model, bitcoin_mined, created_at, updated_at)
           VALUES ($1, $2, $3, NOW(), NOW())
           ON CONFLICT (year, miner_model) DO UPDATE SET
           bitcoin_mined = $3,
           updated_at = NOW()`,
          [year, minerModel, yearlyTotal]
        );
        
        log(`Updated yearly Bitcoin summary for ${year} with ${minerModel}: ${yearlyTotal.toFixed(8)} BTC`);
      }
    } finally {
      client.release();
    }
  } catch (error) {
    log(`Failed to update Bitcoin calculations: ${error}`, "error");
    throw error;
  }
}

/**
 * Verify the data ingestion results
 */
async function verifyResults(): Promise<void> {
  try {
    log(`=== Verification for ${TARGET_DATE} ===`);
    
    const client = await pool.connect();
    try {
      // Check curtailment records
      const recordsResult = await client.query(
        `SELECT 
          COUNT(*) as record_count,
          COUNT(DISTINCT settlement_period) as period_count,
          COUNT(DISTINCT farm_id) as farm_count,
          ROUND(SUM(ABS(volume))::numeric, 2) as total_volume,
          ROUND(SUM(payment)::numeric, 2) as total_payment
         FROM curtailment_records
         WHERE settlement_date = $1`,
        [TARGET_DATE]
      );
      
      if (recordsResult.rows.length > 0) {
        const { record_count, period_count, farm_count, total_volume, total_payment } = recordsResult.rows[0];
        log(`Curtailment Records: ${record_count} records across ${period_count} periods for ${farm_count} farms`);
        log(`Total Volume: ${total_volume} MWh`);
        log(`Total Payment: £${total_payment}`);
      }
      
      // Check daily summary
      const summaryResult = await client.query(
        `SELECT total_curtailed_energy, total_payment
         FROM daily_summaries
         WHERE summary_date = $1`,
        [TARGET_DATE]
      );
      
      if (summaryResult.rows.length > 0) {
        const { total_curtailed_energy, total_payment } = summaryResult.rows[0];
        log(`Daily Summary: ${total_curtailed_energy} MWh, £${total_payment}`);
      }
      
      // Check Bitcoin calculations
      const bitcoinResult = await client.query(
        `SELECT miner_model, COUNT(*) as record_count, ROUND(SUM(bitcoin_mined)::numeric, 8) as total_bitcoin
         FROM historical_bitcoin_calculations
         WHERE settlement_date = $1
         GROUP BY miner_model`,
        [TARGET_DATE]
      );
      
      log(`Bitcoin Calculation Records:`);
      for (const row of bitcoinResult.rows) {
        log(`- ${row.miner_model}: ${row.record_count} records, ${row.total_bitcoin} BTC`);
      }
      
    } finally {
      client.release();
    }
  } catch (error) {
    log(`Verification failed: ${error}`, "error");
  }
}

/**
 * Main function to orchestrate the entire reingestion process
 */
async function main(): Promise<void> {
  const startTime = Date.now();
  
  try {
    log(`Starting data reingest demo for ${TARGET_DATE}`);
    
    // Step 1: Load BMU mappings
    const { bmuMap, bmuLeadPartyMap } = await loadBmuMappings();
    
    // Step 2: Clear existing data
    await clearExistingData();
    
    // Step 3: Process settlement periods in batches
    let totalRecords = 0;
    let totalVolume = 0;
    let totalPayment = 0;
    
    // Create batches of settlement periods (limited for demo)
    const allPeriods = Array.from({ length: MAX_PERIODS }, (_, i) => i + 1);
    const batches = [];
    
    for (let i = 0; i < allPeriods.length; i += BATCH_SIZE) {
      batches.push(allPeriods.slice(i, i + BATCH_SIZE));
    }
    
    // Process each batch
    for (let i = 0; i < batches.length; i++) {
      const batch = batches[i];
      log(`Processing batch ${i + 1} of ${batches.length} (periods ${batch[0]}-${batch[batch.length - 1]})`);
      
      const batchResult = await processBatch(batch, bmuMap, bmuLeadPartyMap);
      totalRecords += batchResult.count;
      totalVolume += batchResult.volume;
      totalPayment += batchResult.payment;
      
      // Small delay between batches
      await delay(500);
    }
    
    log(`Successfully processed ${totalRecords} records for ${TARGET_DATE}`);
    log(`Total volume: ${totalVolume.toFixed(2)} MWh`);
    log(`Total payment: £${totalPayment.toFixed(2)}`);
    
    // Step 4: Update summary tables
    await updateSummaries();
    
    // Step 5: Verify results
    await verifyResults();
    
    // Note: Bitcoin calculations are skipped in this demo for brevity
    log(`Note: Bitcoin calculations are skipped in this demo for brevity`, "info");
    
    const endTime = Date.now();
    const duration = ((endTime - startTime) / 1000).toFixed(1);
    
    log(`Reingest demo completed successfully in ${duration}s`, "success");
  } catch (error) {
    log(`Critical error in main process: ${error}`, "error");
  } finally {
    // Close connections
    logStream.end();
    await pool.end();
  }
}

// Start the process
main();