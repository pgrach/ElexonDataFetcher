/**
 * Process a Single Period For March 27
 * 
 * This is a simplified script that directly processes a specific period
 * for the date 2025-03-27 without using the command-line interface.
 */

import { fetchBidsOffers } from './server/services/elexon';
import { db } from './db';
import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';

// Get the directory name
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Configuration
const TARGET_DATE = '2025-03-27';
const TARGET_PERIOD = 30;  // Just process one period at a time
// const START_PERIOD = 17;
// const END_PERIOD = 24;
const DELAY_BETWEEN_PERIODS = 5000; // ms
const BMU_MAPPING_PATH = path.join(__dirname, "data/bmu_mapping.json");

// Sleep utility
function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Process a single period
async function processPeriod(period: number): Promise<{records: number, volume: number, payment: number}> {
  console.log(`\nProcessing period ${period} for ${TARGET_DATE}...`);
  
  try {
    // Load BMU mapping
    const mappingContent = await fs.readFile(BMU_MAPPING_PATH, 'utf8');
    const bmuMapping = JSON.parse(mappingContent);
    
    // Create a set of valid wind farm BMU IDs for faster lookups
    const validWindFarmIds = new Set(
      bmuMapping
        .filter((bmu: any) => bmu.fuelType === "WIND")
        .map((bmu: any) => bmu.elexonBmUnit)
    );
    
    // Create a map of BMU IDs to lead party names
    const bmuLeadPartyMap = new Map(
      bmuMapping
        .filter((bmu: any) => bmu.fuelType === "WIND")
        .map((bmu: any) => [bmu.elexonBmUnit, bmu.leadPartyName || 'Unknown'])
    );
    
    console.log(`Found ${validWindFarmIds.size} valid wind farm BMUs`);
    
    // Fetch data from Elexon API
    const records = await fetchBidsOffers(TARGET_DATE, period);
    const validRecords = records.filter(record => 
      record.volume < 0 && 
      (record.soFlag || record.cadlFlag) && 
      validWindFarmIds.has(record.id)
    );
    
    console.log(`Found ${validRecords.length} valid curtailment records for period ${period}`);
    
    // Delete any existing records for this date and period
    const deleteQuery = `
      DELETE FROM curtailment_records 
      WHERE settlement_date = '${TARGET_DATE}' AND settlement_period = ${period}
    `;
    await db.execute(deleteQuery);
    
    // Insert the records into the database
    let totalVolume = 0;
    let totalPayment = 0;
    
    for (const record of validRecords) {
      const volume = Math.abs(record.volume);
      const payment = volume * record.originalPrice;
      
      const insertQuery = `
        INSERT INTO curtailment_records 
        (settlement_date, settlement_period, farm_id, lead_party_name, 
         volume, payment, original_price, final_price, so_flag, cadl_flag)
        VALUES (
          '${TARGET_DATE}', 
          ${period}, 
          '${record.id}', 
          '${(bmuLeadPartyMap.get(record.id) || 'Unknown').replace(/'/g, "''")}',
          '${record.volume.toString()}', 
          '${payment.toString()}',
          '${record.originalPrice.toString()}',
          '${record.finalPrice.toString()}',
          ${record.soFlag},
          ${record.cadlFlag}
        )
      `;
      await db.execute(insertQuery);
      
      totalVolume += volume;
      totalPayment += payment;
      
      console.log(`Added record for ${record.id}: ${volume.toFixed(2)} MWh, £${payment.toFixed(2)}`);
    }
    
    console.log(`\nProcessed ${validRecords.length} records for period ${period}`);
    console.log(`Total volume: ${totalVolume.toFixed(2)} MWh`);
    console.log(`Total payment: £${totalPayment.toFixed(2)}`);
    
    return {
      records: validRecords.length,
      volume: totalVolume,
      payment: totalPayment
    };
  } catch (error) {
    console.error(`Error processing period ${period}:`, error);
    return { records: 0, volume: 0, payment: 0 };
  }
}

// Process all periods
async function processAllPeriods(): Promise<void> {
  console.log(`\n===== Starting to process periods ${START_PERIOD} to ${END_PERIOD} for ${TARGET_DATE} =====\n`);
  
  let totalRecords = 0;
  let totalVolume = 0;
  let totalPayment = 0;
  
  // Process all periods in order
  for (let period = START_PERIOD; period <= END_PERIOD; period++) {
    const result = await processPeriod(period);
    
    totalRecords += result.records;
    totalVolume += result.volume;
    totalPayment += result.payment;
    
    // Wait between periods to avoid API rate limits
    if (period < END_PERIOD) {
      console.log(`Waiting ${DELAY_BETWEEN_PERIODS}ms before next period...`);
      await sleep(DELAY_BETWEEN_PERIODS);
    }
  }
  
  // Print overall summary
  console.log(`\n===== Processing complete for ${TARGET_DATE} (periods ${START_PERIOD}-${END_PERIOD}) =====`);
  console.log(`Total records added: ${totalRecords}`);
  console.log(`Total volume: ${totalVolume.toFixed(2)} MWh`);
  console.log(`Total payment: ${totalPayment.toFixed(2) > 0 ? '£' : '-£'}${Math.abs(totalPayment).toFixed(2)}`);
  
  // Update daily summary
  await updateDailySummary(totalVolume, totalPayment);
}

// Update the daily summary
async function updateDailySummary(newVolume: number, newPayment: number): Promise<void> {
  try {
    console.log(`\nUpdating daily summary for ${TARGET_DATE}...`);
    
    // Get current daily summary values
    const query = `
      SELECT 
        total_curtailed_energy::numeric as total_volume,
        total_payment::numeric as total_payment
      FROM daily_summaries
      WHERE summary_date = '${TARGET_DATE}'
    `;
    
    const result = await db.execute(query);
    let totalVolume = 0;
    let totalPayment = 0;
    
    if (result.rows.length > 0) {
      // Add to existing values
      totalVolume = parseFloat(result.rows[0].total_volume) || 0;
      totalPayment = parseFloat(result.rows[0].total_payment) || 0;
      console.log(`Existing summary: ${totalVolume.toFixed(2)} MWh, ${totalPayment.toFixed(2) > 0 ? '£' : '-£'}${Math.abs(totalPayment).toFixed(2)}`);
      
      // Add new values
      totalVolume += newVolume;
      totalPayment += newPayment;
      
      // Update the daily summary
      const updateQuery = `
        UPDATE daily_summaries
        SET 
          total_curtailed_energy = '${totalVolume.toString()}',
          total_payment = '${totalPayment.toString()}',
          last_updated = NOW()
        WHERE summary_date = '${TARGET_DATE}'
      `;
      await db.execute(updateQuery);
    } else {
      // Create new daily summary
      totalVolume = newVolume;
      totalPayment = newPayment;
      
      const insertQuery = `
        INSERT INTO daily_summaries
        (summary_date, total_curtailed_energy, total_payment, created_at, last_updated)
        VALUES ('${TARGET_DATE}', '${totalVolume.toString()}', '${totalPayment.toString()}', NOW(), NOW())
      `;
      await db.execute(insertQuery);
    }
    
    console.log(`Daily summary updated: ${totalVolume.toFixed(2)} MWh, ${totalPayment.toFixed(2) > 0 ? '£' : '-£'}${Math.abs(totalPayment).toFixed(2)}`);
  } catch (error) {
    console.error('Error updating daily summary:', error);
  }
}

// Process a single period and update the daily summary
async function processSinglePeriod(): Promise<void> {
  console.log(`\n===== Processing period ${TARGET_PERIOD} for ${TARGET_DATE} =====\n`);
  
  // Process the period
  const result = await processPeriod(TARGET_PERIOD);
  
  // Update the daily summary
  await updateDailySummary(result.volume, result.payment);
  
  console.log(`\n===== Processing complete for period ${TARGET_PERIOD} =====`);
  console.log(`Records added: ${result.records}`);
  console.log(`Volume: ${result.volume.toFixed(2)} MWh`);
  console.log(`Payment: ${result.payment < 0 ? '-£' : '£'}${Math.abs(result.payment).toFixed(2)}`);
}

// Run the script
processSinglePeriod().then(() => {
  console.log('\nProcessing complete');
  process.exit(0);
}).catch(error => {
  console.error('Unhandled error:', error);
  process.exit(1);
});