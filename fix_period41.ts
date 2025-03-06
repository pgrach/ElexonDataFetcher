/**
 * Fix Period 41 Missing Records - March 5th, 2025
 * 
 * This script specifically addresses the missing records for period 41
 * by fetching data from the Elexon API and inserting it into our database.
 */

import axios from 'axios';
import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';
import { db } from './db';
import { curtailmentRecords, dailySummaries } from './db/schema';
import { eq } from 'drizzle-orm';
import { format } from 'date-fns';

const TARGET_DATE = '2025-03-05';
const TARGET_PERIOD = 41;

// Initialize paths directly
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const BMU_MAPPING_PATH = path.join(__dirname, "server/data/bmuMapping.json");

interface WindFarmMapping {
  elexonBmUnit: string;
  leadPartyName: string;
}

async function loadWindFarmIds(): Promise<Map<string, string>> {
  try {
    console.log('Loading BMU mapping from:', BMU_MAPPING_PATH);
    const mappingContent = await fs.readFile(BMU_MAPPING_PATH, 'utf8');
    const bmuMapping = JSON.parse(mappingContent) as WindFarmMapping[];
    
    // Create a map of BMU ID to lead party name
    const farmMap = new Map<string, string>();
    for (const mapping of bmuMapping) {
      farmMap.set(mapping.elexonBmUnit, mapping.leadPartyName);
    }
    
    console.log(`Loaded ${farmMap.size} wind farm BMU IDs with lead party mappings`);
    return farmMap;
  } catch (error) {
    console.error('Error loading BMU mapping:', error);
    throw error;
  }
}

async function getDirectApiData(): Promise<any[]> {
  console.log(`\nFetching data from Elexon API for ${TARGET_DATE} period ${TARGET_PERIOD}...`);
  
  try {
    // Get the correct API URLs
    const baseUrl = 'https://data.elexon.co.uk/bmrs/api/v1';
    const bidsUrl = `${baseUrl}/balancing/settlement/stack/all/bid/${TARGET_DATE}/${TARGET_PERIOD}`;
    const offersUrl = `${baseUrl}/balancing/settlement/stack/all/offer/${TARGET_DATE}/${TARGET_PERIOD}`;
    
    console.log(`Bid URL: ${bidsUrl}`);
    console.log(`Offer URL: ${offersUrl}`);
    
    // Load wind farm IDs with lead party mappings
    const farmMap = await loadWindFarmIds();
    const windFarmIds = new Set(farmMap.keys());
    
    // Make separate API calls to avoid timeouts
    console.log('Fetching bid data...');
    let validBids: any[] = [];
    try {
      const bidsResponse = await axios.get(bidsUrl, { 
        headers: { 'Accept': 'application/json' },
        timeout: 30000 // 30 second timeout
      });
      
      if (bidsResponse.data?.data && Array.isArray(bidsResponse.data.data)) {
        validBids = bidsResponse.data.data.filter((record: any) => 
          record.volume < 0 && record.soFlag && windFarmIds.has(record.id)
        );
        console.log(`Valid bid records: ${validBids.length}`);
      }
    } catch (error) {
      console.error('Error fetching bid data:', error.message);
    }
    
    console.log('Fetching offer data...');
    let validOffers: any[] = [];
    try {
      const offersResponse = await axios.get(offersUrl, { 
        headers: { 'Accept': 'application/json' },
        timeout: 30000 // 30 second timeout
      });
      
      if (offersResponse.data?.data && Array.isArray(offersResponse.data.data)) {
        validOffers = offersResponse.data.data.filter((record: any) => 
          record.volume < 0 && record.soFlag && windFarmIds.has(record.id)
        );
        console.log(`Valid offer records: ${validOffers.length}`);
      }
    } catch (error) {
      console.error('Error fetching offer data:', error.message);
    }
    
    const allRecords = [...validBids, ...validOffers].map(record => {
      // Add lead party name from our mapping
      const leadPartyName = farmMap.get(record.id) || 'Unknown';
      return {
        ...record,
        leadPartyName
      };
    });
    
    console.log(`\nTotal valid records from API: ${allRecords.length}`);
    
    if (allRecords.length > 0) {
      const periodVolume = allRecords.reduce((sum: number, r: any) => sum + Math.abs(r.volume), 0);
      const periodPayment = allRecords.reduce((sum: number, r: any) => 
        sum + (Math.abs(r.volume) * r.originalPrice * -1), 0
      );
      
      console.log(`API Period volume: ${periodVolume.toFixed(2)} MWh`);
      console.log(`API Period payment: £${periodPayment.toFixed(2)}`);
    }
    
    return allRecords;
  } catch (error) {
    console.error('Error in API data retrieval:', error);
    return [];
  }
}

async function insertRecordsIntoDb(records: any[]): Promise<void> {
  console.log(`\nInserting ${records.length} records into database...`);
  
  let insertedCount = 0;
  let totalVolume = 0;
  let totalPayment = 0;
  
  try {
    for (const record of records) {
      const volume = Math.abs(record.volume);
      const payment = volume * record.originalPrice * -1;
      
      // Ensure all required table fields are properly mapped
      await db.insert(curtailmentRecords).values({
        farm_id: record.id,
        settlement_date: TARGET_DATE,
        settlement_period: TARGET_PERIOD,
        volume: volume.toString(),
        payment: payment.toString(),
        cadl_flag: record.cadlFlag || false,
        so_flag: record.soFlag,
        original_price: record.originalPrice.toString(),
        final_price: record.finalPrice.toString(),
        lead_party_name: record.leadPartyName || null,
        created_at: new Date(),
        updated_at: new Date()
      });
      
      insertedCount++;
      totalVolume += volume;
      totalPayment += payment;
      
      if (insertedCount % 10 === 0) {
        console.log(`Inserted ${insertedCount}/${records.length} records...`);
      }
    }
    
    console.log(`Successfully inserted ${insertedCount} records!`);
    console.log(`Total volume: ${totalVolume.toFixed(2)} MWh`);
    console.log(`Total payment: £${totalPayment.toFixed(2)}`);
    
    return;
  } catch (error) {
    console.error('Error inserting records:', error);
    throw error;
  }
}

async function updateDailySummary(): Promise<void> {
  console.log('\nUpdating daily summary for', TARGET_DATE);
  
  try {
    // Import SQL here to ensure it's available
    const { sql } = await import('drizzle-orm');
    
    // Calculate updated totals from all records for the day
    const result = await db.execute(sql`
      SELECT 
        SUM(volume) as total_volume, 
        SUM(payment) as total_payment
      FROM curtailment_records 
      WHERE settlement_date = ${TARGET_DATE}
    `);
    
    const resultArray = result as any[];
    const totalVolume = parseFloat(resultArray[0].total_volume);
    const totalPayment = parseFloat(resultArray[0].total_payment);
    
    console.log(`Updated totals: ${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)}`);
    
    // Check if summary exists
    const existingSummary = await db.query.dailySummaries.findFirst({
      where: eq(dailySummaries.summaryDate, TARGET_DATE)
    });
    
    if (existingSummary) {
      // Update existing summary
      await db.update(dailySummaries)
        .set({
          totalCurtailedEnergy: totalVolume.toString(),
          totalPayment: totalPayment.toString(),
          updatedAt: new Date()
        })
        .where(eq(dailySummaries.summaryDate, TARGET_DATE));
      
      console.log('Daily summary updated successfully');
    } else {
      // Create new summary
      await db.insert(dailySummaries).values({
        summaryDate: TARGET_DATE,
        totalCurtailedEnergy: totalVolume.toString(),
        totalPayment: totalPayment.toString(),
        createdAt: new Date(),
        updatedAt: new Date()
      });
      
      console.log('Daily summary created successfully');
    }
  } catch (error) {
    console.error('Error updating daily summary:', error);
    throw error;
  }
}

async function triggerBitcoinCalculationUpdates(): Promise<void> {
  console.log('\nTriggerring Bitcoin calculation updates for', TARGET_DATE);
  
  try {
    // Import the service dynamically to avoid circular dependencies
    const { processHistoricalCalculations } = await import('./server/services/bitcoinService');
    
    // Process both standard models
    await processHistoricalCalculations(TARGET_DATE, 'S19J_PRO');
    await processHistoricalCalculations(TARGET_DATE, 'S9');
    await processHistoricalCalculations(TARGET_DATE, 'M20S');
    
    console.log(`[${TARGET_DATE}] Bitcoin calculations updated for models: S19J_PRO, S9, M20S`);
  } catch (error) {
    console.error('Error updating Bitcoin calculations:', error);
    throw error;
  }
}

async function verifyFixes(): Promise<void> {
  console.log('\nVerifying fixes for', TARGET_DATE);
  
  try {
    // Check total records, periods and volumes
    const { sql } = await import('drizzle-orm');
    
    const recordsResult = await db.execute(sql`
      SELECT COUNT(*) as record_count
      FROM curtailment_records
      WHERE settlement_date = ${TARGET_DATE}
    `);
    
    const periodsResult = await db.execute(sql`
      SELECT COUNT(DISTINCT settlement_period) as period_count
      FROM curtailment_records
      WHERE settlement_date = ${TARGET_DATE}
    `);
    
    const totalsResult = await db.execute(sql`
      SELECT 
        SUM(volume) as total_volume, 
        SUM(payment) as total_payment
      FROM curtailment_records
      WHERE settlement_date = ${TARGET_DATE}
    `);
    
    const recordCount = parseInt(recordsResult[0].record_count as string);
    const periodCount = parseInt(periodsResult[0].period_count as string);
    const totalVolume = parseFloat(totalsResult[0].total_volume as string);
    const totalPayment = parseFloat(totalsResult[0].total_payment as string);
    
    console.log(`Verification Check for ${TARGET_DATE}: {
  records: '${recordCount}',
  periods: '${periodCount}',
  volume: '${totalVolume.toFixed(2)}',
  payment: '${totalPayment.toFixed(2)}'
}`);
    
    // Check specifically period 41
    const period41Result = await db.execute(sql`
      SELECT COUNT(*) as record_count, SUM(volume) as total_volume, SUM(payment) as total_payment
      FROM curtailment_records
      WHERE settlement_date = ${TARGET_DATE} AND settlement_period = ${TARGET_PERIOD}
    `);
    
    const period41RecordCount = parseInt(period41Result[0].record_count as string);
    const period41Volume = parseFloat(period41Result[0].total_volume as string);
    const period41Payment = parseFloat(period41Result[0].total_payment as string);
    
    console.log(`Period ${TARGET_PERIOD} Check: {
  records: '${period41RecordCount}',
  volume: '${period41Volume.toFixed(2)}',
  payment: '${period41Payment.toFixed(2)}'
}`);
    
    // Check if we've reached the expected total
    const expectedTotalVolume = 105247.85; // from previous analysis
    const expectedTotalPayment = 3390364.09;
    
    const volumePercentage = (totalVolume / expectedTotalVolume) * 100;
    const paymentPercentage = (totalPayment / expectedTotalPayment) * 100;
    
    console.log(`Completion Status:
  Volume: ${totalVolume.toFixed(2)}/${expectedTotalVolume} MWh (${volumePercentage.toFixed(2)}%)
  Payment: £${totalPayment.toFixed(2)}/£${expectedTotalPayment} (${paymentPercentage.toFixed(2)}%)
`);
  } catch (error) {
    console.error('Error verifying fixes:', error);
  }
}

async function main() {
  console.log(`=== Fixing Period ${TARGET_PERIOD} for ${TARGET_DATE} ===\n`);
  
  try {
    const startTime = new Date();
    
    // Check if we already have records for this period
    const { count } = await db.select({ count: db.fn.count() })
      .from(curtailmentRecords)
      .where(sql`
        settlement_date = ${TARGET_DATE} AND settlement_period = ${TARGET_PERIOD}
      `);
    
    const recordCount = Number(count);
    
    if (recordCount > 0) {
      console.log(`Period ${TARGET_PERIOD} already has ${recordCount} records. Skipping insertion.`);
    } else {
      // Fetch API data
      const apiRecords = await getDirectApiData();
      
      if (apiRecords.length === 0) {
        console.log('No valid records found in API. Aborting.');
        return;
      }
      
      // Insert records
      await insertRecordsIntoDb(apiRecords);
      
      // Update daily summary
      await updateDailySummary();
      
      // Update Bitcoin calculations
      await triggerBitcoinCalculationUpdates();
    }
    
    // Verify fixes
    await verifyFixes();
    
    const endTime = new Date();
    const duration = (endTime.getTime() - startTime.getTime()) / 1000;
    
    console.log(`\nFix completed in ${duration.toFixed(1)}s at ${format(endTime, 'yyyy-MM-dd HH:mm:ss')}`);
  } catch (error) {
    console.error('Error in main function:', error);
  }
}

main().catch(console.error);