/**
 * Direct TypeScript script to process missing periods for 2025-03-27
 * This uses the correct Elexon API endpoints as defined in the elexon.ts service
 */

import { db } from './db';
import { and, eq, sql } from 'drizzle-orm';
import { curtailmentRecords } from './db/schema';
import axios from 'axios';
import * as fsPromises from 'fs/promises';
import * as fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

// Configure the script
const SETTLEMENT_DATE = '2025-03-27';
const START_PERIOD = 35;
const END_PERIOD = 48;

// Constants from elexon.ts service
const ELEXON_BASE_URL = 'https://data.elexon.co.uk/bmrs/api/v1';
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Try both possible locations for the BMU mapping file
const BMU_MAPPING_PATH_ROOT = path.join(__dirname, 'data', 'bmu_mapping.json');
const BMU_MAPPING_PATH_SERVER = path.join(__dirname, 'server', 'data', 'bmuMapping.json');
const BMU_MAPPING_PATH = fs.existsSync(BMU_MAPPING_PATH_ROOT) 
  ? BMU_MAPPING_PATH_ROOT 
  : BMU_MAPPING_PATH_SERVER;

// Type definitions for curtailment records
interface WindFarmBMU {
  elexonBmUnit: string;
  leadPartyName: string;
  fuelType: string;
}

interface ElexonBidOffer {
  id: string;
  volume: number;
  soFlag: boolean;
  cadlFlag: boolean | null;
  originalPrice: number;
  finalPrice: number;
}

// Helper functions
async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function loadBmuMappings(): Promise<{
  windFarmIds: Set<string>;
  bmuLeadPartyMap: Map<string, string>;
}> {
  try {
    console.log('Loading BMU mapping from:', BMU_MAPPING_PATH);
    const mappingContent = await fsPromises.readFile(BMU_MAPPING_PATH, 'utf8');
    const bmuMapping: WindFarmBMU[] = JSON.parse(mappingContent);
    
    const windFarmIds = new Set<string>();
    const bmuLeadPartyMap = new Map<string, string>();
    
    for (const bmu of bmuMapping) {
      windFarmIds.add(bmu.elexonBmUnit);
      bmuLeadPartyMap.set(bmu.elexonBmUnit, bmu.leadPartyName);
    }
    
    console.log(`Loaded ${windFarmIds.size} wind farm BMUs`);
    return { windFarmIds, bmuLeadPartyMap };
  } catch (error) {
    console.error('Error loading BMU mapping:', error);
    throw error;
  }
}

async function processOnePeriod(period: number, date: string, mappings: {
  windFarmIds: Set<string>;
  bmuLeadPartyMap: Map<string, string>;
}): Promise<boolean> {
  try {
    console.log(`\n=== Processing period ${period} for ${date} ===`);
    
    // First, clear any existing records for this period to avoid duplicates
    await db.delete(curtailmentRecords)
      .where(
        and(
          eq(curtailmentRecords.settlementDate, date),
          eq(curtailmentRecords.settlementPeriod, period)
        )
      );
    
    console.log(`Cleared any existing records for period ${period}`);
    
    // Make API requests to both bid and offer endpoints in parallel
    try {
      console.log(`Fetching data from Elexon API for period ${period}...`);
      
      const [bidsResponse, offersResponse] = await Promise.all([
        axios.get(`${ELEXON_BASE_URL}/balancing/settlement/stack/all/bid/${date}/${period}`, {
          headers: { 'Accept': 'application/json' },
          timeout: 30000
        }).catch(e => {
          console.log(`Bid endpoint returned error: ${e.message}`);
          return { data: { data: [] } };
        }),
        
        axios.get(`${ELEXON_BASE_URL}/balancing/settlement/stack/all/offer/${date}/${period}`, {
          headers: { 'Accept': 'application/json' },
          timeout: 30000
        }).catch(e => {
          console.log(`Offer endpoint returned error: ${e.message}`);
          return { data: { data: [] } };
        })
      ]);
      
      // Extract and filter the data for valid wind farm curtailment records
      const validBids = (bidsResponse.data?.data || []).filter((record: ElexonBidOffer) => 
        record.volume < 0 && record.soFlag && mappings.windFarmIds.has(record.id)
      );
      
      const validOffers = (offersResponse.data?.data || []).filter((record: ElexonBidOffer) => 
        record.volume < 0 && record.soFlag && mappings.windFarmIds.has(record.id)
      );
      
      // Combine all records
      const allRecords = [...validBids, ...validOffers];
      
      console.log(`Found ${allRecords.length} valid records (bids: ${validBids.length}, offers: ${validOffers.length})`);
      
      if (allRecords.length > 0) {
        // Prepare records for insertion
        const recordsToInsert = allRecords.map(record => {
          const volume = record.volume; // Keep negative for curtailment
          const payment = Math.abs(volume) * record.originalPrice * -1;
          
          return {
            settlementDate: date,
            settlementPeriod: period,
            farmId: record.id,
            leadPartyName: mappings.bmuLeadPartyMap.get(record.id) || 'Unknown',
            volume: volume.toString(),
            payment: payment.toString(),
            originalPrice: record.originalPrice.toString(),
            finalPrice: record.finalPrice.toString(),
            soFlag: record.soFlag,
            cadlFlag: record.cadlFlag
          };
        });
        
        // Insert records
        await db.insert(curtailmentRecords).values(recordsToInsert);
        console.log(`Successfully inserted ${recordsToInsert.length} records for period ${period}`);
        
        // Calculate totals for reporting
        const totalVolume = allRecords.reduce((sum, record) => sum + Math.abs(record.volume), 0);
        const totalPayment = allRecords.reduce((sum, record) => sum + (Math.abs(record.volume) * record.originalPrice * -1), 0);
        
        console.log(`Period ${period} totals: Volume = ${totalVolume.toFixed(2)} MWh, Payment = £${totalPayment.toFixed(2)}`);
        return true;
      } else {
        console.log(`No valid records found for period ${period}`);
        return true; // Consider this a successful processing, just with zero records
      }
    } catch (error) {
      console.error(`API error for period ${period}:`, error);
      return false;
    }
  } catch (error) {
    console.error(`Error processing period ${period}:`, error);
    return false;
  }
}

async function generateSummary(date: string): Promise<void> {
  try {
    console.log(`\n=== Database Summary for ${date} ===`);
    
    const result = await db.select({
      count: sql<number>`COUNT(*)`,
      minPeriod: sql<number>`MIN(settlement_period)`,
      maxPeriod: sql<number>`MAX(settlement_period)`,
      distinctPeriods: sql<number>`COUNT(DISTINCT settlement_period)`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date));
    
    console.log(`Total records: ${result[0].count}`);
    console.log(`Period range: ${result[0].minPeriod}-${result[0].maxPeriod}`);
    console.log(`Distinct periods: ${result[0].distinctPeriods} (of 48 total)`);
    
    // Get counts by period
    const periodCounts = await db.execute(sql`
      SELECT 
        settlement_period, 
        COUNT(*) as record_count
      FROM curtailment_records
      WHERE settlement_date = ${date}
      GROUP BY settlement_period
      ORDER BY settlement_period
    `);
    
    console.log('\nRecords per period:');
    for (const row of periodCounts.rows) {
      console.log(`Period ${row.settlement_period}: ${row.record_count} records`);
    }
  } catch (error) {
    console.error('Error generating summary:', error);
  }
}

async function main() {
  try {
    console.log(`Starting processing of missing periods (${START_PERIOD}-${END_PERIOD}) for ${SETTLEMENT_DATE}`);
    
    // Load BMU mappings once for all periods
    const mappings = await loadBmuMappings();
    
    // Process each period sequentially
    for (let period = START_PERIOD; period <= END_PERIOD; period++) {
      const success = await processOnePeriod(period, SETTLEMENT_DATE, mappings);
      
      if (success) {
        console.log(`✅ Successfully processed period ${period}`);
      } else {
        console.log(`❌ Failed to process period ${period}`);
      }
      
      // Add a small delay between periods
      if (period < END_PERIOD) {
        console.log(`Waiting 3 seconds before processing next period...`);
        await delay(3000);
      }
    }
    
    console.log(`\n=== Processing complete ===`);
    console.log(`Processed periods ${START_PERIOD}-${END_PERIOD} for ${SETTLEMENT_DATE}`);
    
    // Generate summary
    await generateSummary(SETTLEMENT_DATE);
    
  } catch (error) {
    console.error('Error in main process:', error);
  }
}

// Run the script
main().catch(console.error);