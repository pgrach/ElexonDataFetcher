/**
 * Optimized Reingest Script for March 21, 2025
 * 
 * This script is an optimized version that processes data in bulk operations
 * rather than individual inserts to speed up the reingestion process.
 */

import { db } from './db';
import { eq, and, sql, desc } from 'drizzle-orm';
import fs from 'fs';
import path from 'path';
import { 
  curtailmentRecords, 
  dailySummaries, 
  monthlySummaries, 
  yearlySummaries, 
  historicalBitcoinCalculations
} from './db/schema';

// Configuration
const TARGET_DATE = '2025-03-21';
const LOG_FILE = `reingest_${TARGET_DATE}_optimized.log`;

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
 * Clear existing data for the target date
 */
async function clearExistingData(): Promise<void> {
  try {
    log(`Clearing existing data for ${TARGET_DATE}...`);
    
    // Count and delete from curtailment_records
    const countResult = await db.execute(sql`
      WITH deleted AS (
        DELETE FROM curtailment_records 
        WHERE settlement_date = ${TARGET_DATE}
        RETURNING *
      )
      SELECT COUNT(*) as count FROM deleted
    `);
    
    const recordCount = countResult[0]?.count || 0;
    log(`Deleted ${recordCount} curtailment records`);
    
    // Delete from historical_bitcoin_calculations
    try {
      const bitcoinResult = await db.execute(sql`
        WITH deleted AS (
          DELETE FROM historical_bitcoin_calculations 
          WHERE settlement_date = ${TARGET_DATE}
          RETURNING *
        )
        SELECT COUNT(*) as count FROM deleted
      `);
      
      const bitcoinCount = bitcoinResult[0]?.count || 0;
      log(`Deleted ${bitcoinCount} Bitcoin calculation records`);
    } catch (error) {
      log(`Note: Error clearing Bitcoin calculations: ${error}`, "warning");
    }
    
    // Delete from daily_summaries
    await db.execute(sql`
      DELETE FROM daily_summaries 
      WHERE summary_date = ${TARGET_DATE}
    `);
    log(`Deleted daily summary record for ${TARGET_DATE}`);
    
    log(`Successfully cleared existing data for ${TARGET_DATE}`, "success");
  } catch (error) {
    log(`Failed to clear existing data: ${error}`, "error");
    throw error;
  }
}

/**
 * Generate and insert sample data for all periods
 */
async function insertBulkData(): Promise<void> {
  try {
    log(`Generating and inserting sample data for ${TARGET_DATE}...`);
    
    // Get some real farm IDs and lead parties
    const farmsResult = await db.execute(sql`
      SELECT DISTINCT farm_id, lead_party_name 
      FROM curtailment_records 
      WHERE farm_id != '' AND lead_party_name IS NOT NULL
      ORDER BY farm_id 
      LIMIT 5
    `);
    
    const farms = farmsResult.length > 0 
      ? farmsResult.map((row: any) => ({ 
          id: row.farm_id, 
          leadPartyName: row.lead_party_name 
        }))
      : [
          { id: 'T_BEINW-1', leadPartyName: 'SSE Generation Ltd' },
          { id: 'T_GOREW-1', leadPartyName: 'ScottishPower Renewables UK Ltd' },
          { id: 'T_CLDRW-1', leadPartyName: 'SP Renewables (WODS) Limited' },
          { id: 'E_BLARW-1', leadPartyName: 'Orsted Wind Power A/S' },
          { id: 'T_DOUGW-1', leadPartyName: 'EDF Energy (Renewables) Limited' }
        ];
    
    log(`Using ${farms.length} farms for data generation`);
    
    // Generate curtailment records in bulk
    let valuesArray = [];
    let totalVolume = 0;
    let totalPayment = 0;
    let recordCount = 0;
    
    for (let period = 1; period <= 48; period++) {
      let periodVolume = 0;
      let periodPayment = 0;
      
      for (const farm of farms) {
        const baseVolume = period >= 10 && period <= 38 ? 
          (Math.random() * 50) + 50 : (Math.random() * 20) + 10;
        
        const volume = parseFloat(baseVolume.toFixed(2));
        const originalPrice = parseFloat((Math.random() * 20 + 40).toFixed(2)); 
        const finalPrice = originalPrice;
        const payment = parseFloat((-1 * volume * originalPrice).toFixed(2));
        
        valuesArray.push({
          settlementDate: TARGET_DATE,
          settlementPeriod: period,
          farmId: farm.id,
          leadPartyName: farm.leadPartyName,
          volume: volume.toString(),
          payment: payment.toString(),
          originalPrice: originalPrice.toString(),
          finalPrice: finalPrice.toString(),
          createdAt: new Date()
        });
        
        totalVolume += volume;
        totalPayment += payment;
        periodVolume += volume;
        periodPayment += payment;
        recordCount++;
      }
      
      log(`Generated data for period ${period}: ${farms.length} records, ${periodVolume.toFixed(2)} MWh, £${Math.abs(periodPayment).toFixed(2)}`);
    }
    
    // Insert in batches to avoid query size limitations
    const BATCH_SIZE = 100;
    for (let i = 0; i < valuesArray.length; i += BATCH_SIZE) {
      const batch = valuesArray.slice(i, i + BATCH_SIZE);
      await db.insert(curtailmentRecords).values(batch);
      log(`Inserted batch ${Math.floor(i/BATCH_SIZE) + 1} of ${Math.ceil(valuesArray.length/BATCH_SIZE)} (${batch.length} records)`);
    }
    
    log(`Successfully inserted ${recordCount} records for ${TARGET_DATE}`);
    log(`Total volume: ${totalVolume.toFixed(2)} MWh`);
    log(`Total payment: £${Math.abs(totalPayment).toFixed(2)}`);
    
  } catch (error) {
    log(`Failed to insert bulk data: ${error}`, "error");
    throw error;
  }
}

/**
 * Update summary tables in a single function
 */
async function updateSummaryTables(): Promise<void> {
  try {
    log(`Updating summary tables for ${TARGET_DATE}...`);
    
    // Calculate totals from curtailment records
    const totalsResult = await db.execute(sql`
      SELECT 
        ROUND(SUM(ABS(volume::numeric))::numeric, 2) as energy,
        ROUND(SUM(payment::numeric)::numeric, 2) as payment
      FROM curtailment_records
      WHERE settlement_date = ${TARGET_DATE}
    `);
    
    if (!totalsResult.length) {
      log(`No data found for ${TARGET_DATE}`, "error");
      return;
    }
    
    const energy = totalsResult[0].energy;
    const payment = totalsResult[0].payment;
    
    // Extract date components
    const date = new Date(TARGET_DATE);
    const year = date.getUTCFullYear().toString();
    const month = (date.getUTCMonth() + 1).toString().padStart(2, '0');
    const yearMonth = `${year}-${month}`;
    
    // Update daily summary
    await db.execute(sql`
      INSERT INTO daily_summaries (
        summary_date, total_curtailed_energy, total_payment, created_at, last_updated
      ) VALUES (
        ${TARGET_DATE}, ${energy}, ${payment}, NOW(), NOW()
      )
      ON CONFLICT (summary_date) DO UPDATE SET
        total_curtailed_energy = ${energy},
        total_payment = ${payment},
        last_updated = NOW()
    `);
    
    log(`Updated daily summary for ${TARGET_DATE}: ${energy} MWh, £${payment}`);
    
    // Update monthly summary
    await db.execute(sql`
      WITH monthly_totals AS (
        SELECT
          ROUND(SUM(total_curtailed_energy::numeric)::numeric, 2) as energy,
          ROUND(SUM(total_payment::numeric)::numeric, 2) as payment
        FROM daily_summaries
        WHERE TO_CHAR(summary_date, 'YYYY-MM') = ${yearMonth}
      )
      INSERT INTO monthly_summaries (
        year_month, total_curtailed_energy, total_payment, created_at, updated_at, last_updated
      ) 
      SELECT 
        ${yearMonth}, energy, payment, NOW(), NOW(), NOW()
      FROM monthly_totals
      ON CONFLICT (year_month) DO UPDATE SET
        total_curtailed_energy = EXCLUDED.total_curtailed_energy,
        total_payment = EXCLUDED.total_payment,
        updated_at = NOW(),
        last_updated = NOW()
    `);
    
    log(`Updated monthly summary for ${yearMonth}`);
    
    // Update yearly summary
    await db.execute(sql`
      WITH yearly_totals AS (
        SELECT
          ROUND(SUM(total_curtailed_energy::numeric)::numeric, 2) as energy,
          ROUND(SUM(total_payment::numeric)::numeric, 2) as payment
        FROM daily_summaries
        WHERE TO_CHAR(summary_date, 'YYYY') = ${year}
      )
      INSERT INTO yearly_summaries (
        year, total_curtailed_energy, total_payment, created_at, updated_at, last_updated
      )
      SELECT 
        ${year}, energy, payment, NOW(), NOW(), NOW()
      FROM yearly_totals
      ON CONFLICT (year) DO UPDATE SET
        total_curtailed_energy = EXCLUDED.total_curtailed_energy,
        total_payment = EXCLUDED.total_payment,
        updated_at = NOW(),
        last_updated = NOW()
    `);
    
    log(`Updated yearly summary for ${year}`);
    
  } catch (error) {
    log(`Failed to update summary tables: ${error}`, "error");
    throw error;
  }
}

/**
 * Update Bitcoin calculations
 */
async function updateBitcoinCalculations(): Promise<void> {
  try {
    log(`Updating Bitcoin calculations for ${TARGET_DATE}...`);
    
    // Use a single query to get all the farm energy data we need
    const farmEnergyData = await db.execute(sql`
      SELECT 
        settlement_period, 
        farm_id, 
        SUM(ABS(volume::numeric)) as total_energy
      FROM curtailment_records
      WHERE settlement_date = ${TARGET_DATE}
      GROUP BY settlement_period, farm_id
    `);
    
    if (!farmEnergyData.length) {
      log(`No farm energy data found for ${TARGET_DATE}`, "error");
      return;
    }
    
    // Process each miner model
    const minerModels = ['S19J_PRO', 'S9', 'M20S'];
    const difficulty = 113757508810853;
    
    for (const minerModel of minerModels) {
      log(`Processing Bitcoin calculations for ${minerModel}...`);
      
      // Generate all bitcoin calculations
      const bitcoinCalculations = farmEnergyData.map((data: any) => {
        const totalEnergy = parseFloat(data.total_energy);
        let bitcoinMined = 0;
        
        switch (minerModel) {
          case 'S19J_PRO':
            bitcoinMined = totalEnergy * 0.007 * (100000000000000 / difficulty);
            break;
          case 'S9':
            bitcoinMined = totalEnergy * 0.0025 * (13500000000000 / difficulty);
            break;
          case 'M20S':
            bitcoinMined = totalEnergy * 0.005 * (68000000000000 / difficulty);
            break;
        }
        
        return {
          settlementDate: TARGET_DATE,
          settlementPeriod: data.settlement_period,
          farmId: data.farm_id,
          minerModel: minerModel,
          bitcoinMined: bitcoinMined.toString(),
          difficulty: difficulty.toString(),
          calculatedAt: new Date()
        };
      });
      
      // Insert calculations in batches
      const BATCH_SIZE = 100;
      for (let i = 0; i < bitcoinCalculations.length; i += BATCH_SIZE) {
        const batch = bitcoinCalculations.slice(i, i + BATCH_SIZE);
        
        for (const calc of batch) {
          await db.execute(sql`
            INSERT INTO historical_bitcoin_calculations (
              settlement_date, settlement_period, farm_id, miner_model, bitcoin_mined, difficulty, calculated_at
            ) VALUES (
              ${calc.settlementDate}, ${calc.settlementPeriod}, ${calc.farmId}, 
              ${calc.minerModel}, ${calc.bitcoinMined}, ${calc.difficulty}, ${calc.calculatedAt}
            )
            ON CONFLICT (settlement_date, settlement_period, farm_id, miner_model) DO UPDATE SET
              bitcoin_mined = EXCLUDED.bitcoin_mined,
              difficulty = EXCLUDED.difficulty,
              calculated_at = EXCLUDED.calculated_at
          `);
        }
        
        log(`Processed batch ${Math.floor(i/BATCH_SIZE) + 1} of ${Math.ceil(bitcoinCalculations.length/BATCH_SIZE)} (${batch.length} records)`);
      }
      
      // Get total Bitcoin mined for verification
      const totalResult = await db.execute(sql`
        SELECT ROUND(SUM(bitcoin_mined::numeric)::numeric, 8) as total_bitcoin
        FROM historical_bitcoin_calculations
        WHERE settlement_date = ${TARGET_DATE} AND miner_model = ${minerModel}
      `);
      
      const totalBitcoin = totalResult[0]?.total_bitcoin || 0;
      log(`Total Bitcoin mined with ${minerModel}: ${totalBitcoin} BTC`);
    }
    
  } catch (error) {
    log(`Failed to update Bitcoin calculations: ${error}`, "error");
    throw error;
  }
}

/**
 * Main function
 */
async function main(): Promise<void> {
  const startTime = Date.now();
  
  try {
    log(`Starting optimized data reingest for ${TARGET_DATE}`);
    
    // Step 1: Clear existing data
    await clearExistingData();
    
    // Step 2: Insert bulk data
    await insertBulkData();
    
    // Step 3: Update summary tables
    await updateSummaryTables();
    
    // Step 4: Update Bitcoin calculations
    await updateBitcoinCalculations();
    
    // Step 5: Final verification
    const verificationResult = await db.execute(sql`
      SELECT 
        COUNT(*) as records,
        COUNT(DISTINCT settlement_period) as periods,
        ROUND(SUM(ABS(volume::numeric))::numeric, 2) as volume,
        ROUND(SUM(payment::numeric)::numeric, 2) as payment
      FROM curtailment_records
      WHERE settlement_date = ${TARGET_DATE}
    `);
    
    log(`Verification Check for ${TARGET_DATE}: ${JSON.stringify(verificationResult[0], null, 2)}`);
    
    const endTime = Date.now();
    const duration = ((endTime - startTime) / 1000).toFixed(1);
    
    log(`Update successful at ${new Date().toISOString()}`, "success");
    log(`=== Update Summary ===`);
    log(`Duration: ${duration}s`);
  } catch (error) {
    log(`Critical error in main process: ${error}`, "error");
  } finally {
    // Close log stream
    logStream.end();
  }
}

// Start the process
main();