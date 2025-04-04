/**
 * Complete Data Reingest Reference Guide
 * 
 * This file serves as a comprehensive reference for reingesting settlement data for a specific date.
 * Use this template when you need to fix incomplete or corrupted data for any date in the system.
 * 
 * @example
 * // To reingest data for April 10, 2025
 * npx tsx data_reingest_reference.ts 2025-04-10
 */

import { db } from './db';
import { eq, and, sql, desc } from 'drizzle-orm';
import { Pool } from 'pg';
import * as fs from 'fs';
import * as path from 'path';
import axios from 'axios';

// Configuration - Edit these values for your specific case
const TARGET_DATE = process.argv[2] || '2025-03-28'; // Set this to the date you want to reingest
const BATCH_SIZE = 6; // Optimal batch size to avoid timeouts (don't change unless necessary)
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
  bmuMap: Map<string, string>,
  bmuLeadPartyMap: Map<string, string>
}> {
  try {
    const bmuMappingPath = path.join(process.cwd(), 'server', 'data', 'bmuMapping.json');
    const bmuMappingData = JSON.parse(fs.readFileSync(bmuMappingPath, 'utf-8'));
    
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
 * Clear existing data for the target date to avoid duplicates
 */
async function clearExistingData(): Promise<void> {
  const client = await pool.connect();
  try {
    log(`Clearing existing data for ${TARGET_DATE}...`);
    
    await client.query('BEGIN');
    
    // Delete from curtailment_records
    const deleteResult = await client.query(
      'DELETE FROM curtailment_records WHERE settlement_date = $1 RETURNING COUNT(*)',
      [TARGET_DATE]
    );
    log(`Deleted ${deleteResult.rowCount} curtailment records`);
    
    // Delete from historical_bitcoin_calculations
    const deleteBitcoinResult = await client.query(
      'DELETE FROM historical_bitcoin_calculations WHERE settlement_date = $1 RETURNING COUNT(*)',
      [TARGET_DATE]
    );
    log(`Deleted ${deleteBitcoinResult.rowCount} Bitcoin calculation records`);
    
    // Delete from daily_summaries
    const deleteDailySummaryResult = await client.query(
      'DELETE FROM daily_summaries WHERE date = $1 RETURNING COUNT(*)',
      [TARGET_DATE]
    );
    log(`Deleted ${deleteDailySummaryResult.rowCount} daily summary records`);
    
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
 * Process a single settlement period by fetching data from Elexon API
 */
async function processPeriod(
  period: number,
  bmuMap: Map<string, string>,
  bmuLeadPartyMap: Map<string, string>
): Promise<{ count: number, volume: number, payment: number }> {
  try {
    log(`[${TARGET_DATE} P${period}] Processing settlement period ${period}...`);
    
    // Fetch data from Elexon API
    const elexonApiUrl = `https://api.bmreports.com/BMRS/B1620/v1?APIKey=${process.env.ELEXON_API_KEY}&SettlementDate=${TARGET_DATE}&Period=${period}&ServiceType=csv`;
    
    const response = await axios.get(elexonApiUrl);
    const lines = response.data.split('\n');
    
    // Skip header and empty lines
    const dataLines = lines.filter((line: string) => 
      line.trim().length > 0 && 
      !line.startsWith('*') && 
      !line.startsWith('H,') &&
      !line.startsWith('B1620')
    );
    
    let recordCount = 0;
    let totalVolume = 0;
    let totalPayment = 0;
    
    // Process each data line
    for (const line of dataLines) {
      const fields = line.split(',');
      
      // Extract BMU ID (position 5 in the CSV)
      const bmuId = fields[5]?.trim();
      if (!bmuId || !bmuMap.has(bmuId)) continue;
      
      // Get farm ID from the BMU mapping
      const farmId = bmuMap.get(bmuId)!;
      const leadParty = bmuLeadPartyMap.get(bmuId) || 'Unknown';
      
      // Extract volume and payment data
      const volume = Math.abs(parseFloat(fields[8]));
      const price = parseFloat(fields[9]);
      const payment = -1 * volume * price; // Negative because payments are costs
      
      if (isNaN(volume) || isNaN(payment) || volume === 0) continue;
      
      totalVolume += volume;
      totalPayment += payment;
      
      // Insert record into curtailment_records table
      const client = await pool.connect();
      try {
        await client.query(
          `INSERT INTO curtailment_records 
           (settlement_date, settlement_period, farm_id, lead_party, volume, price, payment, created_at, updated_at) 
           VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), NOW())`,
          [TARGET_DATE, period, farmId, leadParty, volume, price, payment]
        );
        
        recordCount++;
        log(`[${TARGET_DATE} P${period}] Added record for ${farmId}: ${volume.toFixed(2)} MWh, £${payment.toFixed(2)}`);
      } finally {
        client.release();
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
    log(`[${TARGET_DATE}] Updating summaries...`);
    
    const client = await pool.connect();
    try {
      // Step 1: Update daily_summaries
      await client.query(
        `INSERT INTO daily_summaries (date, energy, payment, created_at, updated_at)
         SELECT 
           settlement_date, 
           ROUND(SUM(ABS(volume))::numeric, 2) as energy, 
           ROUND(SUM(payment)::numeric, 2) as payment,
           NOW(), 
           NOW()
         FROM curtailment_records
         WHERE settlement_date = $1
         GROUP BY settlement_date
         ON CONFLICT (date) DO UPDATE SET
           energy = EXCLUDED.energy,
           payment = EXCLUDED.payment,
           updated_at = NOW()`,
        [TARGET_DATE]
      );
      
      // Step 2: Extract year and month from the target date
      const date = new Date(TARGET_DATE);
      const year = date.getUTCFullYear().toString();
      const month = (date.getUTCMonth() + 1).toString().padStart(2, '0');
      const yearMonth = `${year}-${month}`;
      
      // Step 3: Update monthly_summaries
      await client.query(
        `INSERT INTO monthly_summaries (year_month, energy, payment, created_at, updated_at)
         SELECT 
           SUBSTRING(date::text, 1, 7) as year_month, 
           ROUND(SUM(energy)::numeric, 2) as energy, 
           ROUND(SUM(payment)::numeric, 2) as payment,
           NOW(), 
           NOW()
         FROM daily_summaries
         WHERE SUBSTRING(date::text, 1, 7) = $1
         GROUP BY year_month
         ON CONFLICT (year_month) DO UPDATE SET
           energy = EXCLUDED.energy,
           payment = EXCLUDED.payment,
           updated_at = NOW()`,
        [yearMonth]
      );
      
      // Step 4: Update yearly_summaries
      await client.query(
        `INSERT INTO yearly_summaries (year, energy, payment, created_at, updated_at)
         SELECT 
           SUBSTRING(date::text, 1, 4) as year, 
           ROUND(SUM(energy)::numeric, 2) as energy, 
           ROUND(SUM(payment)::numeric, 2) as payment,
           NOW(), 
           NOW()
         FROM daily_summaries
         WHERE SUBSTRING(date::text, 1, 4) = $1
         GROUP BY year
         ON CONFLICT (year) DO UPDATE SET
           energy = EXCLUDED.energy,
           payment = EXCLUDED.payment,
           updated_at = NOW()`,
        [year]
      );
      
      // Fetch updated values for verification
      const dailyResult = await client.query(
        'SELECT energy, payment FROM daily_summaries WHERE date = $1',
        [TARGET_DATE]
      );
      
      if (dailyResult.rows.length > 0) {
        const { energy, payment } = dailyResult.rows[0];
        log(`[${TARGET_DATE}] Reprocessing complete: { energy: '${energy} MWh', payment: '£${payment}' }`, "success");
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
    
    // List of miner models to process
    const minerModels = ['S19J_PRO', 'S9', 'M20S'];
    
    // Current network difficulty (this would typically come from an API)
    // For this example, we're using a fixed value. In production, fetch from an API.
    const difficulty = 113757508810853; // Example difficulty value
    
    const client = await pool.connect();
    try {
      for (const minerModel of minerModels) {
        log(`Processing ${TARGET_DATE} with difficulty ${difficulty}`);
        
        // Get all settlement periods for the date
        const periodsResult = await client.query(
          'SELECT DISTINCT settlement_period FROM curtailment_records WHERE settlement_date = $1 ORDER BY settlement_period',
          [TARGET_DATE]
        );
        
        const periods = periodsResult.rows.map(r => r.settlement_period);
        
        // Count records for logging
        const countResult = await client.query(
          'SELECT COUNT(*) as count, COUNT(DISTINCT settlement_period) as period_count, COUNT(DISTINCT farm_id) as farm_count FROM curtailment_records WHERE settlement_date = $1',
          [TARGET_DATE]
        );
        
        log(`Found ${countResult.rows[0].count} curtailment records across ${countResult.rows[0].period_count} periods and ${countResult.rows[0].farm_count} farms`);
        
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
            // This is a simplified calculation - adjust as needed for your specific models
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
            
            // Insert the calculation
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
        
        log(`Inserted ${insertCount} records for ${TARGET_DATE} ${minerModel}`);
        log(`Processed periods: ${periods.join(', ')}`);
      }
      
      // Update monthly Bitcoin summaries
      const date = new Date(TARGET_DATE);
      const year = date.getUTCFullYear().toString();
      const month = (date.getUTCMonth() + 1).toString().padStart(2, '0');
      const yearMonth = `${year}-${month}`;
      
      log(`[${TARGET_DATE}] Updating monthly Bitcoin summary for ${yearMonth}...`);
      
      for (const minerModel of minerModels) {
        log(`Calculating monthly Bitcoin summary for ${yearMonth} with ${minerModel}`);
        
        // Sum up all Bitcoin mined for this month and miner model
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
        
        log(`Updated monthly summary for ${yearMonth}: ${totalBitcoin.toFixed(8)} BTC`);
      }
      
      // Update yearly Bitcoin summaries
      log(`[${TARGET_DATE}] Updating yearly Bitcoin summary for ${year}...`);
      log('=== Manual Yearly Bitcoin Summary Update ===');
      log(`Updating summaries for ${year}`);
      
      for (const minerModel of minerModels) {
        log(`- Processing ${minerModel}`);
        log(`Calculating yearly Bitcoin summary for ${year} with ${minerModel}`);
        
        // Get monthly summaries for this year and miner model
        const monthlyResult = await client.query(
          `SELECT bitcoin_mined FROM monthly_bitcoin_summaries 
           WHERE year_month LIKE $1 AND miner_model = $2 
           ORDER BY year_month`,
          [`${year}-%`, minerModel]
        );
        
        log(`Found ${monthlyResult.rows.length} monthly summaries for ${year}`);
        
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
        
        log(`Updated yearly summary for ${year}: ${yearlyTotal.toFixed(8)} BTC with ${minerModel}`);
      }
      
      // Verification logging
      const verificationResult = [];
      for (const minerModel of minerModels) {
        const yearlyResult = await client.query(
          `SELECT bitcoin_mined FROM yearly_bitcoin_summaries 
           WHERE year = $1 AND miner_model = $2`,
          [year, minerModel]
        );
        
        if (yearlyResult.rows.length > 0) {
          verificationResult.push(`- ${minerModel}: ${yearlyResult.rows[0].bitcoin_mined} BTC`);
        }
      }
      
      log('Verification Results for ' + year + ':');
      verificationResult.forEach(line => log(line));
      log('=== Yearly Summary Update Complete ===');
      
      log(`[${TARGET_DATE}] Bitcoin calculations updated for models: ${minerModels.join(', ')}`, "success");
    } finally {
      client.release();
    }
  } catch (error) {
    log(`Failed to update Bitcoin calculations: ${error}`, "error");
    throw error;
  }
}

/**
 * Main function to orchestrate the entire reingestion process
 */
async function main(): Promise<void> {
  const startTime = Date.now();
  
  try {
    // Validate target date
    if (!TARGET_DATE.match(/^\d{4}-\d{2}-\d{2}$/)) {
      log('Invalid date format. Please use YYYY-MM-DD format.', "error");
      return;
    }
    
    log(`Starting complete data reingest for ${TARGET_DATE}`);
    
    // Step 1: Load BMU mappings
    const { bmuMap, bmuLeadPartyMap } = await loadBmuMappings();
    
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
      log(`Processing batch ${i + 1} of ${batches.length} (periods ${batches[i][0]}-${batches[i][batches[i].length - 1]})`);
      
      const batchResult = await processBatch(batches[i], bmuMap, bmuLeadPartyMap);
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
    
    // Step 5: Update Bitcoin calculations
    await updateBitcoinCalculations();
    
    // Step 6: Final verification
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
    } finally {
      client.release();
    }
    
    const endTime = Date.now();
    const duration = ((endTime - startTime) / 1000).toFixed(1);
    
    log(`Update successful at ${new Date().toISOString()}`, "success");
    log(`=== Update Summary ===`);
    log(`Duration: ${duration}s`);
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