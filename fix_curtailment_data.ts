/**
 * Fix Curtailment Data Script
 * 
 * This script corrects the curtailment data for 2025-03-31 by:
 * 1. Deleting existing records for that date
 * 2. Fetching correct records from Elexon API
 * 3. Inserting the correct records into the database
 * 4. Updating dependent tables (Bitcoin calculations, summaries)
 */

import { db } from './db';
import { curtailmentRecords } from './db/schema';
import { eq, sql } from 'drizzle-orm';
import axios from 'axios';
import { processFullCascade } from './process_bitcoin_optimized';

// Elexon API base URL
const ELEXON_BASE_URL = 'https://data.elexon.co.uk/bmrs/api/v1';

// BMU IDs from our database
const TARGET_BMU_IDS = [
  'T_SGRWO-1', 'T_SGRWO-2', 'T_SGRWO-3', 'T_SGRWO-4', 'T_SGRWO-5',
  'T_VKNGW-1', 'T_VKNGW-2', 'T_VKNGW-3', 'T_VKNGW-4'
];

// Date to fix
const TARGET_DATE = '2025-03-31';

// Periods with curtailment based on our API check
const TARGET_PERIODS = [22, 23, 24, 25, 26, 27];

// BMU mapping - needed to get lead party names
async function loadBmuMapping(): Promise<Record<string, { name: string, leadParty: string }>> {
  try {
    console.log('Loading BMU mapping data...');
    
    // Generate mapping data directly
    const bmuMapping: Record<string, { name: string, leadParty: string }> = {};
    
    for (const bmuId of TARGET_BMU_IDS) {
      // Use naming patterns to determine lead party
      let leadParty = 'Unknown';
      
      if (bmuId.startsWith('T_SGRWO')) {
        leadParty = 'Scottish Power Renewables';
      } else if (bmuId.startsWith('T_VKNGW')) {
        leadParty = 'Vattenfall';
      }
      
      bmuMapping[bmuId] = {
        name: bmuId,
        leadParty
      };
    }
    
    console.log(`Created mapping for ${Object.keys(bmuMapping).length} BMUs`);
    return bmuMapping;
  } catch (error) {
    console.error('Error in loadBmuMapping:', error);
    throw error;
  }
}

/**
 * Make a request to the Elexon API
 */
async function makeElexonRequest(url: string): Promise<any> {
  try {
    console.log(`Making request to: ${url}`);
    
    const response = await axios.get(url, {
      headers: { 'Accept': 'application/json' },
      timeout: 30000 // 30 second timeout
    });
    
    return response.data;
  } catch (error: any) {
    if (axios.isAxiosError(error)) {
      if (error.response) {
        console.error(`API Error: ${error.response.status} - ${error.response.statusText}`);
        console.error('Response data:', error.response.data);
      } else {
        console.error(`API Error: ${error.message}`);
      }
    } else {
      console.error('Error making request:', error);
    }
    
    throw error;
  }
}

/**
 * Get curtailment records for a specific period
 */
async function getCurtailmentRecords(date: string, period: number): Promise<any[]> {
  try {
    // Try both bids and offers
    const bidUrl = `${ELEXON_BASE_URL}/balancing/settlement/stack/all/bid/${date}/${period}`;
    const offerUrl = `${ELEXON_BASE_URL}/balancing/settlement/stack/all/offer/${date}/${period}`;
    
    // Make the requests
    const [bidsResponse, offersResponse] = await Promise.all([
      makeElexonRequest(bidUrl),
      makeElexonRequest(offerUrl)
    ]);
    
    // Process the responses
    const bids = bidsResponse.data || [];
    const offers = offersResponse.data || [];
    
    // Filter to our BMUs of interest
    const relevantBids = bids.filter((bid: any) => 
      TARGET_BMU_IDS.includes(bid.id) && bid.volume < 0 && bid.soFlag);
    
    const relevantOffers = offers.filter((offer: any) => 
      TARGET_BMU_IDS.includes(offer.id) && offer.volume < 0 && offer.soFlag);
    
    // Combine all records
    return [...relevantBids, ...relevantOffers];
  } catch (error) {
    console.error(`Error getting curtailment records for period ${period}:`, error);
    return [];
  }
}

/**
 * Delete existing records for a date
 */
async function deleteExistingRecords(date: string): Promise<number> {
  try {
    console.log(`Deleting existing records for ${date}...`);
    
    const result = await db.delete(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date))
      .returning({ id: curtailmentRecords.id });
    
    console.log(`Deleted ${result.length} records`);
    return result.length;
  } catch (error) {
    console.error('Error deleting records:', error);
    throw error;
  }
}

/**
 * Insert new records from API data
 */
async function insertNewRecords(date: string, bmuMapping: Record<string, { name: string, leadParty: string }>): Promise<number> {
  try {
    console.log(`Inserting new records for ${date}...`);
    
    let totalInserted = 0;
    
    // Process each period
    for (const period of TARGET_PERIODS) {
      console.log(`Processing period ${period}...`);
      
      const records = await getCurtailmentRecords(date, period);
      
      if (records.length === 0) {
        console.log(`No records found for period ${period}`);
        continue;
      }
      
      console.log(`Found ${records.length} records for period ${period}`);
      
      // Insert each record
      for (const record of records) {
        const volume = record.volume; // Keep negative to indicate curtailment
        
        // Calculate payment - need to store as NEGATIVE value since curtailment involves payments TO generators
        const paymentAmount = Math.abs(record.volume) * record.originalPrice;
        const payment = -1 * paymentAmount; // Force this to be negative
        
        console.log(`Inserting record: BMU=${record.id}, Volume=${volume}, Price=${record.originalPrice}, Payment=${payment}`);
        
        await db.insert(curtailmentRecords).values({
          settlementDate: date,
          settlementPeriod: period,
          farmId: record.id,
          leadPartyName: bmuMapping[record.id]?.leadParty || 'Unknown',
          volume: volume.toString(),
          payment: payment.toString(),
          originalPrice: record.originalPrice.toString(),
          finalPrice: record.finalPrice.toString(),
          soFlag: record.soFlag,
          cadlFlag: record.cadlFlag || false
        });
        
        totalInserted++;
      }
    }
    
    console.log(`Inserted ${totalInserted} records`);
    return totalInserted;
  } catch (error) {
    console.error('Error inserting records:', error);
    throw error;
  }
}

/**
 * Update Bitcoin calculations and summaries
 */
async function updateBitcoinCalculations(date: string): Promise<void> {
  try {
    console.log(`Updating Bitcoin calculations for ${date}...`);
    
    // Use the optimized process for Bitcoin calculations
    await processFullCascade(date);
    
    console.log('Bitcoin calculations updated successfully');
  } catch (error) {
    console.error('Error updating Bitcoin calculations:', error);
    throw error;
  }
}

/**
 * Main function to fix the data
 */
async function main() {
  try {
    console.log(`\n=== Starting Data Fix for ${TARGET_DATE} ===\n`);
    
    // Step 1: Load BMU mapping
    console.log('Step 1: Loading BMU mapping...');
    const bmuMapping = await loadBmuMapping();
    
    // Step 2: Delete existing records
    console.log('\nStep 2: Deleting existing records...');
    const deletedCount = await deleteExistingRecords(TARGET_DATE);
    
    // Step 3: Insert new records from API data
    console.log('\nStep 3: Inserting new records...');
    const insertedCount = await insertNewRecords(TARGET_DATE, bmuMapping);
    
    if (insertedCount === 0) {
      console.error('No records were inserted. Something went wrong.');
      return;
    }
    
    // Step 4: Update Bitcoin calculations and summaries
    console.log('\nStep 4: Updating Bitcoin calculations...');
    await updateBitcoinCalculations(TARGET_DATE);
    
    console.log(`\n=== Data Fix Complete for ${TARGET_DATE} ===\n`);
    console.log('Summary:');
    console.log(`- Deleted Records: ${deletedCount}`);
    console.log(`- Inserted Records: ${insertedCount}`);
    console.log(`- Periods Fixed: ${TARGET_PERIODS.join(', ')}`);
    
    console.log('\nData has been successfully corrected!');
  } catch (error) {
    console.error('Error in main process:', error);
  }
}

// Run the main function
main();