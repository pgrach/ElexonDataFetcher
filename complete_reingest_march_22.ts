/**
 * Complete Reingest for March 22, 2025
 * 
 * This script completely removes all settlement period data for March 22, 2025
 * and then reingests all 48 settlement periods from the Elexon API.
 */

import { db } from './db';
import { sql } from 'drizzle-orm';
import axios from 'axios';
import fs from 'fs';
import path from 'path';
import pLimit from 'p-limit';

// Set expected total payment for verification
export const EXPECTED_TOTAL_PAYMENT = 880000.00; // Placeholder value, will be updated with actual expected amount

// Configuration
const TARGET_DATE = '2025-03-22';
const ELEXON_API_BASE_URL = 'https://api.bmreports.com/BMRS';
const API_THROTTLE_MS = 500;  // Delay between API calls to prevent rate limiting
const BATCH_SIZE = 4;         // Process periods in batches of 4 to prevent timeouts

// Set your Elexon API key here or use from environment
const ELEXON_API_KEY = process.env.ELEXON_API_KEY || 'elexon_api_key';

// Create log directory if it doesn't exist
const LOG_DIR = path.join(__dirname, 'logs');
if (!fs.existsSync(LOG_DIR)) {
  fs.mkdirSync(LOG_DIR);
}

// Set up logging
const logFile = path.join(LOG_DIR, `complete_reingest_march_22.log`);
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

async function clearExistingData(): Promise<void> {
  try {
    log(`Clearing ALL existing data for ${TARGET_DATE}...`);
    
    // Delete curtailment records
    const curtailmentResult = await db.execute(sql`
      DELETE FROM curtailment_records 
      WHERE settlement_date = ${TARGET_DATE}
    `);
    log(`Deleted ${curtailmentResult.rowCount} existing curtailment records`);
    
    // Delete Bitcoin calculations
    const bitcoinResult = await db.execute(sql`
      DELETE FROM historical_bitcoin_calculations 
      WHERE settlement_date = ${TARGET_DATE}
    `);
    log(`Deleted ${bitcoinResult.rowCount} existing Bitcoin calculation records`);
    
    // Delete daily summary
    const summaryResult = await db.execute(sql`
      DELETE FROM daily_summaries 
      WHERE date = ${TARGET_DATE}
    `);
    log(`Deleted ${summaryResult.rowCount} daily summary records`);
    
  } catch (error) {
    log(`Error clearing existing data: ${error}`);
    throw error;
  }
}

async function processPeriod(
  period: number, 
  bmuIdToFarmId: Map<string, string>,
  bmuIdToLeadParty: Map<string, string>
): Promise<{ recordCount: number; totalVolume: number; totalPayment: number }> {
  try {
    log(`Processing period ${period} for ${TARGET_DATE}...`);
    
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

async function processBatch(
  periods: number[],
  bmuIdToFarmId: Map<string, string>,
  bmuIdToLeadParty: Map<string, string>
): Promise<{ totalRecords: number; totalVolume: number; totalPayment: number }> {
  
  log(`Processing batch with periods: ${periods.join(', ')}`);
  
  const limit = pLimit(1); // Process one period at a time to avoid API rate limits
  
  const results = await Promise.all(
    periods.map(period => limit(() => processPeriod(period, bmuIdToFarmId, bmuIdToLeadParty)))
  );
  
  const batchTotals = results.reduce(
    (acc, { recordCount, totalVolume, totalPayment }) => {
      return {
        totalRecords: acc.totalRecords + recordCount,
        totalVolume: acc.totalVolume + totalVolume,
        totalPayment: acc.totalPayment + totalPayment
      };
    },
    { totalRecords: 0, totalVolume: 0, totalPayment: 0 }
  );
  
  log(`Batch total records: ${batchTotals.totalRecords}`);
  log(`Batch total volume: ${batchTotals.totalVolume.toFixed(2)} MWh`);
  log(`Batch total payment: £${Math.abs(batchTotals.totalPayment).toFixed(2)}`);
  
  return batchTotals;
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
    
    const yearlyEnergy = parseFloat(yearlyResult.rows[0].yearly_energy) || 0;
    const yearlyPayment = parseFloat(yearlyResult.rows[0].yearly_payment) || 0;
    
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
    
    const difficulty = parseFloat(minerstatResult.rows[0].difficulty);
    const priceBTC = parseFloat(minerstatResult.rows[0].price_gbp);
    
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
    
    const totalBitcoinMined = parseFloat(bitcoinResult.rows[0].total_bitcoin_mined) || 0;
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
  log(`Starting complete reingestion for ${TARGET_DATE}`);
  log('========================================');
  
  try {
    // Load BMU mappings
    const { bmuIdToFarmId, bmuIdToLeadParty } = await loadBmuMappings();
    
    // Clear existing data for the target date
    await clearExistingData();
    
    // Divide periods into manageable batches
    const allPeriods = Array.from({ length: 48 }, (_, i) => i + 1);
    const batches: number[][] = [];
    
    for (let i = 0; i < allPeriods.length; i += BATCH_SIZE) {
      batches.push(allPeriods.slice(i, i + BATCH_SIZE));
    }
    
    log(`Divided 48 periods into ${batches.length} batches of ${BATCH_SIZE} periods each`);
    
    // Process each batch
    let overallTotalRecords = 0;
    let overallTotalVolume = 0;
    let overallTotalPayment = 0;
    
    for (let i = 0; i < batches.length; i++) {
      log(`Processing batch ${i + 1} of ${batches.length}...`);
      
      const { totalRecords, totalVolume, totalPayment } = await processBatch(
        batches[i],
        bmuIdToFarmId,
        bmuIdToLeadParty
      );
      
      overallTotalRecords += totalRecords;
      overallTotalVolume += totalVolume;
      overallTotalPayment += totalPayment;
      
      log(`Overall progress: ${((i + 1) / batches.length * 100).toFixed(1)}% complete`);
      log(`Running totals: ${overallTotalRecords} records, ${overallTotalVolume.toFixed(2)} MWh, £${Math.abs(overallTotalPayment).toFixed(2)}`);
      
      // Add a delay between batches to prevent API rate limiting
      if (i < batches.length - 1) {
        log(`Waiting for ${API_THROTTLE_MS * 2}ms before next batch...`);
        await delay(API_THROTTLE_MS * 2);
      }
    }
    
    // Update summary tables
    await updateSummaries();
    
    // Update Bitcoin calculations
    await updateBitcoinCalculations();
    
    // Verify final results
    const finalResult = await db.execute(sql`
      SELECT 
        COUNT(*) as record_count,
        COUNT(DISTINCT settlement_period) as period_count,
        SUM(curtailed_volume) as total_volume,
        SUM(payment) as total_payment
      FROM curtailment_records
      WHERE settlement_date = ${TARGET_DATE}
    `);
    
    const recordCount = parseInt(finalResult.rows[0].record_count);
    const periodCount = parseInt(finalResult.rows[0].period_count);
    const totalVolume = parseFloat(finalResult.rows[0].total_volume) || 0;
    const totalPayment = parseFloat(finalResult.rows[0].total_payment) || 0;
    
    log('========================================');
    log(`Final Results for ${TARGET_DATE}:`);
    log(`- Settlement Periods: ${periodCount}/48`);
    log(`- Records: ${recordCount}`);
    log(`- Total Volume: ${totalVolume.toFixed(2)} MWh`);
    log(`- Total Payment: £${Math.abs(totalPayment).toFixed(2)}`);
    
    if (periodCount === 48) {
      log('SUCCESS: All 48 settlement periods successfully processed');
    } else {
      log(`WARNING: Only ${periodCount} out of 48 settlement periods were processed`);
      
      // Get missing periods
      const periodsResult = await db.execute(sql`
        SELECT settlement_period
        FROM curtailment_records
        WHERE settlement_date = ${TARGET_DATE}
        GROUP BY settlement_period
        ORDER BY settlement_period
      `);
      
      const existingPeriods = new Set(periodsResult.rows.map(r => parseInt(r.settlement_period)));
      const missingPeriods = Array.from({ length: 48 }, (_, i) => i + 1).filter(p => !existingPeriods.has(p));
      
      log(`Missing periods: ${missingPeriods.join(', ')}`);
    }
    
    // Check if payment is close to expected
    const paymentDifference = Math.abs(Math.abs(totalPayment) - EXPECTED_TOTAL_PAYMENT);
    const percentageDifference = (paymentDifference / EXPECTED_TOTAL_PAYMENT) * 100;
    
    log(`Payment verification:`);
    log(`- Expected: £${EXPECTED_TOTAL_PAYMENT.toFixed(2)}`);
    log(`- Actual: £${Math.abs(totalPayment).toFixed(2)}`);
    log(`- Difference: £${paymentDifference.toFixed(2)} (${percentageDifference.toFixed(2)}%)`);
    
    if (percentageDifference < 0.1 || paymentDifference < 100) {
      log('SUCCESS: Final payment matches expected total (within £100 or 0.1% margin)');
    } else {
      log('WARNING: Final payment does not match expected total');
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
    log('Complete reingestion process finished');
    process.exit(0);
  })
  .catch((error) => {
    console.error('Fatal error:', error);
    process.exit(1);
  });