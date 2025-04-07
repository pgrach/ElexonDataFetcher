/**
 * Verify and Fix March 22, 2025 Curtailment Records
 * 
 * This script compares curtailment records in the database with Elexon API data
 * for March 22, 2025, and reingests any missing or incomplete data.
 */

import { db } from './db';
import { sql } from 'drizzle-orm';
import axios from 'axios';
import fs from 'fs';
import path from 'path';
import pLimit from 'p-limit';

// Configuration
const TARGET_DATE = '2025-03-22';
const ELEXON_API_BASE_URL = 'https://api.bmreports.com/BMRS';
const API_THROTTLE_MS = 500;  // Delay between API calls to prevent rate limiting

// Set your Elexon API key here or use from environment
const ELEXON_API_KEY = process.env.ELEXON_API_KEY || 'elexon_api_key';

// Create log directory if it doesn't exist
const LOG_DIR = path.join(__dirname, 'logs');
if (!fs.existsSync(LOG_DIR)) {
  fs.mkdirSync(LOG_DIR);
}

// Set up logging
const logFile = path.join(LOG_DIR, `verify_and_fix_march_22.log`);
const logStream = fs.createWriteStream(logFile, { flags: 'a' });

function log(message: string): void {
  const timestamp = new Date().toISOString();
  const formattedMessage = `[${timestamp}] ${message}`;
  console.log(formattedMessage);
  logStream.write(formattedMessage + '\n');
}

async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Interface for period status information
type PeriodStatus = {
  period: number;
  existingCount: number;
  existingVolume: number;
  elexonCount: number;
  elexonVolume: number;
  status: 'missing' | 'incomplete' | 'complete' | 'mismatch' | 'unknown';
  needsUpdate: boolean;
};

async function loadBmuMappings(): Promise<{
  bmuIdToFarmId: Map<string, string>;
  bmuIdToLeadParty: Map<string, string>;
}> {
  log('Loading BMU to Farm ID mappings...');
  
  // Load mappings from database or file
  const mappingsResult = await db.execute(sql`
    SELECT 
      bmu_id, 
      farm_id, 
      lead_party 
    FROM wind_farm_bmu_mappings
  `);
  
  const bmuIdToFarmId = new Map<string, string>();
  const bmuIdToLeadParty = new Map<string, string>();
  
  for (const row of mappingsResult.rows) {
    bmuIdToFarmId.set(row.bmu_id as string, row.farm_id as string);
    bmuIdToLeadParty.set(row.bmu_id as string, row.lead_party as string);
  }
  
  log(`Loaded ${bmuIdToFarmId.size} BMU mappings`);
  return { bmuIdToFarmId, bmuIdToLeadParty };
}

async function checkPeriodStatus(
  period: number,
  bmuIdToFarmId: Map<string, string>
): Promise<PeriodStatus> {
  try {
    log(`Checking status for period ${period}...`);
    
    // Get existing records from database
    const dbResult = await db.execute(sql`
      SELECT 
        COUNT(*) as record_count,
        SUM(curtailed_volume) as total_volume
      FROM curtailment_records
      WHERE settlement_date = ${TARGET_DATE}
      AND settlement_period = ${period}
    `);
    
    const existingCount = parseInt(dbResult.rows[0].record_count as string) || 0;
    const existingVolume = parseFloat(dbResult.rows[0].total_volume as string) || 0;
    
    log(`Database has ${existingCount} records with ${existingVolume.toFixed(2)} MWh for period ${period}`);
    
    // Get data from Elexon API
    const url = `${ELEXON_API_BASE_URL}/DISBSAD/v1?APIKey=${ELEXON_API_KEY}&SettlementDate=${TARGET_DATE}&Period=${period}&ServiceType=xml`;
    
    log(`Fetching data from Elexon API for period ${period}...`);
    const response = await axios.get(url);
    
    // Process API response
    let elexonCount = 0;
    let elexonVolume = 0;
    
    if (response.data && response.data.response && response.data.response.responseBody && 
        response.data.response.responseBody.responseList && response.data.response.responseBody.responseList.item) {
      
      const items = Array.isArray(response.data.response.responseBody.responseList.item) 
        ? response.data.response.responseBody.responseList.item 
        : [response.data.response.responseBody.responseList.item];
      
      elexonCount = items.length;
      
      // Count only items with valid BMU IDs and active flag
      let validItemCount = 0;
      
      for (const item of items) {
        const bmuId = item.bMUnitID;
        const farmId = bmuIdToFarmId.get(bmuId);
        
        if (farmId && item.activeFlag === 'Y') {
          const volume = parseFloat(item.acceptanceVolume || '0');
          if (volume > 0) {
            elexonVolume += volume;
            validItemCount++;
          }
        }
      }
      
      log(`Elexon API has ${elexonCount} records (${validItemCount} valid) with ${elexonVolume.toFixed(2)} MWh for period ${period}`);
    } else {
      log(`No data returned from Elexon API for period ${period}`);
    }
    
    // Determine status
    let status: PeriodStatus['status'] = 'unknown';
    let needsUpdate = false;
    
    if (existingCount === 0 && elexonCount > 0) {
      status = 'missing';
      needsUpdate = true;
    } else if (existingCount > 0 && Math.abs(existingVolume - elexonVolume) > 0.1) {
      status = 'mismatch';
      needsUpdate = true;
    } else if (existingCount < elexonCount) {
      status = 'incomplete';
      needsUpdate = true;
    } else {
      status = 'complete';
      needsUpdate = false;
    }
    
    return {
      period,
      existingCount,
      existingVolume,
      elexonCount,
      elexonVolume,
      status,
      needsUpdate
    };
  } catch (error) {
    log(`Error checking status for period ${period}: ${error}`);
    return {
      period,
      existingCount: 0,
      existingVolume: 0,
      elexonCount: 0,
      elexonVolume: 0,
      status: 'unknown',
      needsUpdate: true
    };
  }
}

async function processPeriod(
  period: number, 
  bmuIdToFarmId: Map<string, string>,
  bmuIdToLeadParty: Map<string, string>
): Promise<{ recordCount: number; totalVolume: number; totalPayment: number }> {
  try {
    log(`Processing period ${period} for ${TARGET_DATE}...`);
    
    // Delete existing records for this period
    const deleteResult = await db.execute(sql`
      DELETE FROM curtailment_records 
      WHERE settlement_date = ${TARGET_DATE} 
      AND settlement_period = ${period}
    `);
    
    log(`Deleted ${deleteResult.rowCount} existing records for period ${period}`);
    
    // Fetch curtailment data from Elexon API
    const url = `${ELEXON_API_BASE_URL}/DISBSAD/v1?APIKey=${ELEXON_API_KEY}&SettlementDate=${TARGET_DATE}&Period=${period}&ServiceType=xml`;
    
    log(`Fetching data from Elexon API for period ${period}...`);
    const response = await axios.get(url);
    
    if (!response.data || !response.data.response || !response.data.response.responseBody || !response.data.response.responseBody.responseList || !response.data.response.responseBody.responseList.item) {
      log(`No data returned from API for period ${period}`);
      return { recordCount: 0, totalVolume: 0, totalPayment: 0 };
    }
    
    const items = Array.isArray(response.data.response.responseBody.responseList.item) 
      ? response.data.response.responseBody.responseList.item 
      : [response.data.response.responseBody.responseList.item];
    
    log(`Received ${items.length} records from API for period ${period}`);
    
    let insertCount = 0;
    let totalVolume = 0;
    let totalPayment = 0;
    
    // Process and insert data
    for (const item of items) {
      const bmuId = item.bMUnitID;
      const farmId = bmuIdToFarmId.get(bmuId);
      const leadParty = bmuIdToLeadParty.get(bmuId);
      
      if (!farmId) {
        log(`Warning: No farm ID mapping found for BMU ID ${bmuId}, skipping`);
        continue;
      }
      
      const volume = parseFloat(item.activeFlag === 'Y' ? item.acceptanceVolume : '0');
      const price = parseFloat(item.activeFlag === 'Y' ? item.acceptancePrice : '0');
      const payment = volume * price * -1; // Payment is negative in the database
      
      // Only insert if there's actual curtailment (volume > 0)
      if (volume > 0) {
        await db.execute(sql`
          INSERT INTO curtailment_records (
            settlement_date, 
            settlement_period, 
            farm_id, 
            bmu_id, 
            curtailed_volume, 
            price, 
            payment,
            lead_party
          ) VALUES (
            ${TARGET_DATE}, 
            ${period}, 
            ${farmId}, 
            ${bmuId}, 
            ${volume}, 
            ${price}, 
            ${payment},
            ${leadParty || null}
          )
        `);
        
        insertCount++;
        totalVolume += volume;
        totalPayment += payment;
      }
    }
    
    log(`Inserted ${insertCount} records for period ${period}`);
    log(`Period ${period} total volume: ${totalVolume.toFixed(2)} MWh, total payment: £${Math.abs(totalPayment).toFixed(2)}`);
    
    return { recordCount: insertCount, totalVolume, totalPayment };
  } catch (error) {
    log(`Error processing period ${period}: ${error}`);
    throw error;
  }
}

async function updateSummaries(): Promise<void> {
  try {
    log('Updating daily, monthly, and yearly summary tables...');
    
    // Calculate totals from curtailment records
    const totalsResult = await db.execute(sql`
      SELECT 
        SUM(curtailed_volume) as total_energy,
        SUM(payment) as total_payment
      FROM curtailment_records
      WHERE settlement_date = ${TARGET_DATE}
    `);
    
    const totalEnergy = parseFloat(totalsResult.rows[0].total_energy as string) || 0;
    const totalPayment = parseFloat(totalsResult.rows[0].total_payment as string) || 0;
    
    log(`Raw totals from database:`);
    log(`- Energy: ${totalEnergy.toFixed(2)} MWh`);
    log(`- Payment: ${totalPayment.toFixed(2)}`);
    
    // Update or insert daily summary
    await db.execute(sql`
      INSERT INTO daily_summaries (
        date,
        total_curtailed_energy,
        total_payment
      ) VALUES (
        ${TARGET_DATE},
        ${totalEnergy},
        ${totalPayment}
      )
      ON CONFLICT (date) DO UPDATE SET
        total_curtailed_energy = ${totalEnergy},
        total_payment = ${totalPayment}
    `);
    
    log(`Daily summary updated for ${TARGET_DATE}:`);
    log(`- Energy: ${totalEnergy.toFixed(2)} MWh`);
    log(`- Payment: £${Math.abs(totalPayment).toFixed(2)}`);
    
    // Extract year and month for monthly/yearly summaries
    const [year, month] = TARGET_DATE.split('-');
    const yearMonth = `${year}-${month}`;
    
    // Update monthly summary
    const monthlyResult = await db.execute(sql`
      SELECT 
        SUM(total_curtailed_energy) as monthly_energy,
        SUM(total_payment) as monthly_payment
      FROM daily_summaries
      WHERE to_char(date, 'YYYY-MM') = ${yearMonth}
    `);
    
    const monthlyEnergy = parseFloat(monthlyResult.rows[0].monthly_energy as string) || 0;
    const monthlyPayment = parseFloat(monthlyResult.rows[0].monthly_payment as string) || 0;
    
    await db.execute(sql`
      INSERT INTO monthly_summaries (
        year_month,
        total_curtailed_energy,
        total_payment
      ) VALUES (
        ${yearMonth},
        ${monthlyEnergy},
        ${monthlyPayment}
      )
      ON CONFLICT (year_month) DO UPDATE SET
        total_curtailed_energy = ${monthlyEnergy},
        total_payment = ${monthlyPayment}
    `);
    
    log(`Monthly summary updated for ${yearMonth}:`);
    log(`- Energy: ${monthlyEnergy.toFixed(2)} MWh`);
    log(`- Payment: £${Math.abs(monthlyPayment).toFixed(2)}`);
    
    // Update yearly summary
    const yearlyResult = await db.execute(sql`
      SELECT 
        SUM(total_curtailed_energy) as yearly_energy,
        SUM(total_payment) as yearly_payment
      FROM monthly_summaries
      WHERE to_char(year_month, 'YYYY') = ${year}
    `);
    
    const yearlyEnergy = parseFloat(yearlyResult.rows[0].yearly_energy as string) || 0;
    const yearlyPayment = parseFloat(yearlyResult.rows[0].yearly_payment as string) || 0;
    
    await db.execute(sql`
      INSERT INTO yearly_summaries (
        year,
        total_curtailed_energy,
        total_payment
      ) VALUES (
        ${year},
        ${yearlyEnergy},
        ${yearlyPayment}
      )
      ON CONFLICT (year) DO UPDATE SET
        total_curtailed_energy = ${yearlyEnergy},
        total_payment = ${yearlyPayment}
    `);
    
    log(`Yearly summary updated for ${year}:`);
    log(`- Energy: ${yearlyEnergy.toFixed(2)} MWh`);
    log(`- Payment: £${Math.abs(yearlyPayment).toFixed(2)}`);
    
  } catch (error) {
    log(`Error updating summaries: ${error}`);
    throw error;
  }
}

async function updateBitcoinCalculations(): Promise<void> {
  try {
    log(`Updating Bitcoin calculations for ${TARGET_DATE}...`);
    
    // Get the latest difficulty and price
    const minerstatResult = await db.execute(sql`
      SELECT 
        difficulty,
        price_gbp
      FROM minerstat_data
      ORDER BY timestamp DESC
      LIMIT 1
    `);
    
    const difficulty = parseFloat(minerstatResult.rows[0].difficulty as string);
    const priceBTC = parseFloat(minerstatResult.rows[0].price_gbp as string);
    
    log(`Using Bitcoin difficulty: ${difficulty}, Price: £${priceBTC}`);
    
    // Delete existing calculations for this date
    await db.execute(sql`
      DELETE FROM historical_bitcoin_calculations
      WHERE settlement_date = ${TARGET_DATE}
    `);
    
    // Calculate Bitcoin mining potential for each farm and period
    await db.execute(sql`
      WITH curtailment_with_farm_data AS (
        SELECT 
          c.settlement_date,
          c.settlement_period,
          c.farm_id,
          c.curtailed_volume,
          f.capacity_mw,
          'S19J_PRO' as miner_model
        FROM curtailment_records c
        JOIN wind_farms f ON c.farm_id = f.id
        WHERE c.settlement_date = ${TARGET_DATE}
      )
      INSERT INTO historical_bitcoin_calculations (
        settlement_date,
        settlement_period,
        farm_id,
        miner_model,
        bitcoin_mined,
        difficulty,
        calculated_at
      )
      SELECT 
        settlement_date,
        settlement_period,
        farm_id,
        miner_model,
        -- Bitcoin mining calculation based on curtailed energy and difficulty
        (curtailed_volume * 1000 * 0.9 * 1 / NULLIF(difficulty / 1e12, 0)) as bitcoin_mined,
        ${difficulty},
        NOW()
      FROM curtailment_with_farm_data
    `);
    
    // Get total Bitcoin mined for the date
    const bitcoinResult = await db.execute(sql`
      SELECT SUM(bitcoin_mined) as total_bitcoin_mined
      FROM historical_bitcoin_calculations
      WHERE settlement_date = ${TARGET_DATE}
    `);
    
    const totalBitcoinMined = parseFloat(bitcoinResult.rows[0].total_bitcoin_mined as string) || 0;
    const totalBitcoinValue = totalBitcoinMined * priceBTC;
    
    log(`Total Bitcoin mining calculations updated for ${TARGET_DATE}:`);
    log(`- Bitcoin mined: ${totalBitcoinMined.toFixed(6)} BTC`);
    log(`- Value at current price: £${totalBitcoinValue.toFixed(2)}`);
    
  } catch (error) {
    log(`Error updating Bitcoin calculations: ${error}`);
    throw error;
  }
}

async function main(): Promise<void> {
  log('========================================');
  log(`Starting verification and fix for ${TARGET_DATE}`);
  log('========================================');
  
  try {
    // Load BMU mappings
    const { bmuIdToFarmId, bmuIdToLeadParty } = await loadBmuMappings();
    
    // Check status of all periods
    log('Checking status of all 48 settlement periods...');
    
    const allPeriods = Array.from({ length: 48 }, (_, i) => i + 1);
    const limit = pLimit(1); // Check one period at a time to avoid API rate limits
    
    const statusPromises = allPeriods.map(period => 
      limit(() => checkPeriodStatus(period, bmuIdToFarmId))
    );
    
    const statusResults = await Promise.all(statusPromises.map((promise, index) => 
      promise.catch(error => {
        log(`Error checking period ${index + 1}: ${error}`);
        return {
          period: index + 1,
          existingCount: 0,
          existingVolume: 0,
          elexonCount: 0,
          elexonVolume: 0,
          status: 'unknown' as const,
          needsUpdate: true
        };
      })
    ));
    
    // Collect missing or incomplete periods
    const periodsToUpdate = statusResults.filter(result => result.needsUpdate);
    
    log('----------------------------------------');
    log(`Found ${periodsToUpdate.length} periods that need to be updated:`);
    
    const missingPeriods = statusResults.filter(result => result.status === 'missing').map(r => r.period);
    const incompletePeriods = statusResults.filter(result => result.status === 'incomplete').map(r => r.period);
    const mismatchPeriods = statusResults.filter(result => result.status === 'mismatch').map(r => r.period);
    const unknownPeriods = statusResults.filter(result => result.status === 'unknown').map(r => r.period);
    
    if (missingPeriods.length > 0) {
      log(`- Missing periods: ${missingPeriods.join(', ')}`);
    }
    
    if (incompletePeriods.length > 0) {
      log(`- Incomplete periods: ${incompletePeriods.join(', ')}`);
    }
    
    if (mismatchPeriods.length > 0) {
      log(`- Mismatched periods: ${mismatchPeriods.join(', ')}`);
    }
    
    if (unknownPeriods.length > 0) {
      log(`- Unknown status periods: ${unknownPeriods.join(', ')}`);
    }
    
    // Process the periods that need updating
    if (periodsToUpdate.length > 0) {
      log('----------------------------------------');
      log(`Updating ${periodsToUpdate.length} periods...`);
      
      let totalRecords = 0;
      let totalVolume = 0;
      let totalPayment = 0;
      
      // Update each period that needs it
      for (const status of periodsToUpdate) {
        log(`Processing period ${status.period} (status: ${status.status})...`);
        
        const { recordCount, totalVolume: volume, totalPayment: payment } = 
          await processPeriod(status.period, bmuIdToFarmId, bmuIdToLeadParty);
          
        totalRecords += recordCount;
        totalVolume += volume;
        totalPayment += payment;
        
        // Add a delay between API calls to prevent rate limiting
        await delay(API_THROTTLE_MS);
      }
      
      log('----------------------------------------');
      log(`Updated ${periodsToUpdate.length} periods with ${totalRecords} records`);
      log(`Total volume: ${totalVolume.toFixed(2)} MWh, total payment: £${Math.abs(totalPayment).toFixed(2)}`);
      
      // Update summaries and Bitcoin calculations
      await updateSummaries();
      await updateBitcoinCalculations();
    } else {
      log('No periods need to be updated. The data is complete!');
    }
    
    // Final verification
    const finalResult = await db.execute(sql`
      SELECT 
        COUNT(*) as record_count,
        COUNT(DISTINCT settlement_period) as period_count,
        SUM(curtailed_volume) as total_volume,
        SUM(payment) as total_payment
      FROM curtailment_records
      WHERE settlement_date = ${TARGET_DATE}
    `);
    
    const recordCount = parseInt(finalResult.rows[0].record_count as string);
    const periodCount = parseInt(finalResult.rows[0].period_count as string);
    const totalVolume = parseFloat(finalResult.rows[0].total_volume as string) || 0;
    const totalPayment = parseFloat(finalResult.rows[0].total_payment as string) || 0;
    
    log('========================================');
    log(`Final Verification for ${TARGET_DATE}:`);
    log(`- Settlement Periods: ${periodCount}/48`);
    log(`- Records: ${recordCount}`);
    log(`- Total Volume: ${totalVolume.toFixed(2)} MWh`);
    log(`- Total Payment: £${Math.abs(totalPayment).toFixed(2)}`);
    
    if (periodCount === 48) {
      log('SUCCESS: All 48 settlement periods are now in the database!');
    } else {
      log(`WARNING: Still missing ${48 - periodCount} settlement periods`);
      
      // Get the missing periods
      const existingPeriodsResult = await db.execute(sql`
        SELECT DISTINCT settlement_period
        FROM curtailment_records
        WHERE settlement_date = ${TARGET_DATE}
        ORDER BY settlement_period
      `);
      
      const existingPeriods = new Set(existingPeriodsResult.rows.map(r => parseInt(r.settlement_period as string)));
      const stillMissingPeriods = Array.from({ length: 48 }, (_, i) => i + 1).filter(p => !existingPeriods.has(p));
      
      log(`Still missing periods: ${stillMissingPeriods.join(', ')}`);
    }
    
  } catch (error) {
    log(`Error in main function: ${error}`);
    throw error;
  } finally {
    logStream.end();
  }
}

main()
  .then(() => {
    log('Verification and fix process finished');
    process.exit(0);
  })
  .catch((error) => {
    console.error('Fatal error:', error);
    process.exit(1);
  });