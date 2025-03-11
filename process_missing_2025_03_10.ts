/**
 * Script to process missing periods 47-48 for 2025-03-10
 */

import { db } from './db';
import { and, eq } from 'drizzle-orm';
import { curtailmentRecords } from './db/schema';
import axios from 'axios';
import { spawn } from 'child_process';
import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';

// Handle ESM module dirname
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Configuration
const API_BASE_URL = 'https://data.elexon.co.uk/bmrs/api/v1';
const BMU_MAPPING_PATH = path.join(__dirname, 'server', 'data', 'bmuMapping.json');
const date = '2025-03-10';
const PERIODS_TO_PROCESS = [47, 48];

console.log(`Processing missing periods ${PERIODS_TO_PROCESS.join(', ')} for ${date}`);

// Load BMU mappings once
async function loadBmuMappings() {
  try {
    console.log(`Loading BMU mapping from: ${BMU_MAPPING_PATH}`);
    const data = await fs.readFile(BMU_MAPPING_PATH, 'utf8');
    const bmuMapping = JSON.parse(data);
    
    const windFarmIds = new Set<string>();
    const bmuLeadPartyMap = new Map<string, string>();
    
    for (const bmu of bmuMapping) {
      windFarmIds.add(bmu.id);
      bmuLeadPartyMap.set(bmu.id, bmu.leadPartyName);
    }
    
    console.log(`Found ${windFarmIds.size} wind farm BMUs`);
    return { windFarmIds, bmuLeadPartyMap };
  } catch (error) {
    console.error(`Error loading BMU mapping:`, error);
    return { windFarmIds: new Set<string>(), bmuLeadPartyMap: new Map<string, string>() };
  }
}

// Process a single period
async function processPeriod(period: number, windFarmIds: Set<string>, bmuLeadPartyMap: Map<string, string>) {
  console.log(`Processing period ${period}...`);
  
  try {
    // Fetch data from the Elexon API with correct endpoint format
    // Make parallel requests for bids and offers as in the elexon.ts service
    const [bidsResponse, offersResponse] = await Promise.all([
      axios.get(`${API_BASE_URL}/balancing/settlement/stack/all/bid/${date}/${period}`),
      axios.get(`${API_BASE_URL}/balancing/settlement/stack/all/offer/${date}/${period}`)
    ]).catch(error => {
      console.error(`[${date} P${period}] Error fetching data:`, error.message);
      return [{ data: { data: [] } }, { data: { data: [] } }];
    });
    
    if (!bidsResponse.data?.data || !offersResponse.data?.data) {
      console.error(`[${date} P${period}] Invalid API response format`);
      return { records: 0, volume: 0, payment: 0 };
    }
    
    const validBids = bidsResponse.data.data.filter((record: any) => 
      record.volume < 0 && record.soFlag && windFarmIds.has(record.id)
    );
    
    const validOffers = offersResponse.data.data.filter((record: any) => 
      record.volume < 0 && record.soFlag && windFarmIds.has(record.id)
    );
    
    const validRecords = [...validBids, ...validOffers];
    
    const totalVolume = validRecords.reduce((sum: number, record: any) => sum + Math.abs(record.volume), 0);
    const totalPayment = validRecords.reduce((sum: number, record: any) => sum + (Math.abs(record.volume) * record.originalPrice), 0);
    
    console.log(`Period ${period}: Found ${validRecords.length} records (${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)})`);
    
    // Clear any existing records for this period to avoid duplicates
    await db.delete(curtailmentRecords)
      .where(
        and(
          eq(curtailmentRecords.settlementDate, date),
          eq(curtailmentRecords.settlementPeriod, period)
        )
      );
    
    console.log(`Cleared any existing records for period ${period}`);
    
    // Insert new records if any
    if (validRecords.length > 0) {
      const recordsToInsert = validRecords.map((record: any) => {
        const volume = Math.abs(record.volume);
        const payment = volume * record.originalPrice;
        
        return {
          settlementDate: date,
          settlementPeriod: period,
          farmId: record.id,
          leadPartyName: bmuLeadPartyMap.get(record.id) || 'Unknown',
          volume: record.volume.toString(), // Keep negative value
          payment: payment.toString(),
          originalPrice: record.originalPrice.toString(),
          finalPrice: record.finalPrice.toString(),
          soFlag: record.soFlag,
          cadlFlag: record.cadlFlag
        };
      });
      
      await db.insert(curtailmentRecords).values(recordsToInsert);
      
      console.log(`Period ${period}: Added ${recordsToInsert.length} records`);
      
      // Log individual records
      for (const record of validRecords) {
        const volume = Math.abs(record.volume);
        const payment = volume * record.originalPrice;
        console.log(`- Added ${record.id}: ${volume.toFixed(2)} MWh, £${payment.toFixed(2)}`);
      }
    } else {
      console.log(`No valid records found for period ${period}`);
    }
    
    return {
      records: validRecords.length,
      volume: totalVolume,
      payment: totalPayment
    };
  } catch (error) {
    console.error(`Error processing period ${period}:`, error);
    return {
      records: 0,
      volume: 0,
      payment: 0
    };
  }
}

// Run reconciliation to update Bitcoin calculations
async function runReconciliation() {
  return new Promise((resolve) => {
    console.log(`Running reconciliation for ${date}...`);
    
    const reconciliation = spawn('npx', ['tsx', 'unified_reconciliation.ts', 'date', date]);
    
    reconciliation.stdout.on('data', (data) => {
      console.log(`${data}`);
    });
    
    reconciliation.stderr.on('data', (data) => {
      console.error(`${data}`);
    });
    
    reconciliation.on('close', (code) => {
      if (code === 0) {
        console.log(`Reconciliation completed successfully for ${date}`);
        resolve(true);
      } else {
        console.log(`Reconciliation failed with code ${code}`);
        resolve(false);
      }
    });
    
    // Add timeout to prevent hanging
    setTimeout(() => {
      console.log(`Reconciliation timed out after 60 seconds, continuing anyway`);
      resolve(false);
    }, 60000);
  });
}

async function checkFinalStats() {
  try {
    // Get all records for this date to analyze
    const records = await db
      .select()
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date));
    
    // Calculate statistics from the raw records
    const totalRecords = records.length;
    
    // Group by period to get unique periods
    const periodGroups: Record<number, boolean> = {};
    records.forEach(record => {
      periodGroups[record.settlementPeriod] = true;
    });
    
    const existingPeriods = Object.keys(periodGroups).map(Number);
    const totalPeriods = existingPeriods.length;
    
    console.log(`\n=== Final statistics for ${date} ===`);
    console.log(`- Total records: ${totalRecords}`);
    console.log(`- Total periods: ${totalPeriods}`);
    
    // Check if we have all 48 periods
    if (totalPeriods === 48) {
      console.log('✅ All 48 periods are now present');
    } else {
      console.log(`⚠️ Only ${totalPeriods}/48 periods found`);
      
      // Find missing periods
      const missing = [];
      for (let p = 1; p <= 48; p++) {
        if (!existingPeriods.includes(p)) {
          missing.push(p);
        }
      }
      
      if (missing.length > 0) {
        console.log(`Missing periods: ${missing.join(', ')}`);
      }
    }
  } catch (error) {
    console.error('Error checking final stats:', error);
  }
}

async function main() {
  try {
    // Load BMU mappings
    const { windFarmIds, bmuLeadPartyMap } = await loadBmuMappings();
    
    // Process each missing period
    let totalRecords = 0;
    let totalVolume = 0;
    let totalPayment = 0;
    
    for (const period of PERIODS_TO_PROCESS) {
      const result = await processPeriod(period, windFarmIds, bmuLeadPartyMap);
      totalRecords += result.records;
      totalVolume += result.volume;
      totalPayment += result.payment;
      
      // Add a short delay to avoid rate limiting
      await new Promise(resolve => setTimeout(resolve, 1500));
    }
    
    console.log(`\n=== Processing Summary ===`);
    console.log(`- Processed periods: ${PERIODS_TO_PROCESS.join(', ')}`);
    console.log(`- Total records added: ${totalRecords}`);
    console.log(`- Total volume: ${totalVolume.toFixed(2)} MWh`);
    console.log(`- Total payment: £${totalPayment.toFixed(2)}`);
    
    // Run reconciliation to update Bitcoin calculations if needed
    if (totalRecords > 0) {
      await runReconciliation();
    } else {
      console.log('No records added, skipping reconciliation');
    }
    
    // Check final stats
    await checkFinalStats();
    
    console.log('\n=== Process completed ===');
  } catch (error) {
    console.error('Fatal error during processing:', error);
  }
}

// Run the script
main().catch(console.error);