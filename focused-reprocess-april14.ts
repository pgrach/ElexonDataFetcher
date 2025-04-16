/**
 * Focused Data Reprocessing Script for 2025-04-14
 * 
 * This script targets specific settlement periods that are known to have data
 * for more efficient processing of April 14, 2025.
 * 
 * Run with: npx tsx focused-reprocess-april14.ts
 */

import axios from 'axios';
import { db } from './db';
import { curtailmentRecords, dailySummaries, monthlySummaries } from './db/schema';
import { eq, sql } from 'drizzle-orm';
import fs from 'fs';
import path from 'path';

// Constants
const TARGET_DATE = '2025-04-14';
const API_KEY = process.env.ELEXON_API_KEY;
const LOG_DIR = './logs';
const LOG_FILE = path.join(LOG_DIR, `focused_reprocess_${TARGET_DATE.replace(/-/g, '')}_${new Date().toISOString().replace(/:/g, '-').replace(/\./g, '-')}.log`);

// Periods with highest likelihood of having curtailment data on April 14
// Based on historical patterns and previous analysis
const PRIORITY_PERIODS = [3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24];

// Set up logging
if (!fs.existsSync(LOG_DIR)) {
  fs.mkdirSync(LOG_DIR, { recursive: true });
}
fs.writeFileSync(LOG_FILE, `=== Focused Reprocessing Log for ${TARGET_DATE} ===\n`);

const log = (message: string) => {
  const timestamp = new Date().toISOString();
  const formattedMessage = `[${timestamp}] ${message}`;
  console.log(formattedMessage);
  fs.appendFileSync(LOG_FILE, formattedMessage + '\n');
};

// Fetch data from Elexon API with focused approach
async function fetchElexonData(date: string, period: number): Promise<any[]> {
  try {
    log(`Fetching data for ${date} period ${period} from Elexon...`);
    const url = `https://api.bmreports.com/BMRS/B1610/v2?APIKey=${API_KEY}&SettlementDate=${date}&Period=${period}&ServiceType=xml`;
    
    const response = await axios.get(url, { timeout: 5000 });
    
    if (response.status !== 200) {
      throw new Error(`API returned status code ${response.status}`);
    }
    
    const xmlData = response.data;
    
    // Extract all B1610 items
    const itemRegex = /<item>[\s\S]*?<\/item>/g;
    const items = xmlData.match(itemRegex) || [];
    
    const records = [];
    
    for (const item of items) {
      // Extract fields
      const bmuIdMatch = item.match(/<bm_unit_id>(.*?)<\/bm_unit_id>/);
      const volumeMatch = item.match(/<volume>(.*?)<\/volume>/);
      const priceMatch = item.match(/<price>(.*?)<\/price>/);
      const soFlagMatch = item.match(/<so_flag>(.*?)<\/so_flag>/);
      const cadlFlagMatch = item.match(/<cadl_flag>(.*?)<\/cadl_flag>/);
      const leadPartyMatch = item.match(/<lead_party>(.*?)<\/lead_party>/);
      
      if (bmuIdMatch && volumeMatch && priceMatch) {
        const farmId = bmuIdMatch[1];
        const volume = parseFloat(volumeMatch[1]);
        const price = parseFloat(priceMatch[1]);
        const soFlag = soFlagMatch ? soFlagMatch[1] === 'Y' : false;
        const cadlFlag = cadlFlagMatch ? cadlFlagMatch[1] === 'Y' : false;
        const leadPartyName = leadPartyMatch ? leadPartyMatch[1] : 'Unknown';
        
        // Only include curtailment records (negative volume with flags)
        if (volume < 0 && (soFlag || cadlFlag)) {
          records.push({
            farmId,
            volume,
            price,
            soFlag,
            cadlFlag,
            leadPartyName
          });
        }
      }
    }
    
    // Check if we found any records
    log(`Retrieved ${records.length} curtailment records for period ${period}`);
    return records;
  } catch (error: any) {
    log(`Error fetching data for period ${period}: ${error.message}`);
    return [];
  }
}

async function reprocessData() {
  log(`Starting focused reprocessing for ${TARGET_DATE}`);
  
  try {
    // Step 1: Clear existing data for the target date
    log(`Removing existing curtailment records for ${TARGET_DATE}...`);
    await db.delete(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    // Step 2: Process priority settlement periods first
    let totalRecords = 0;
    let totalVolume = 0;
    let totalPayment = 0;
    
    for (const period of PRIORITY_PERIODS) {
      const records = await fetchElexonData(TARGET_DATE, period);
      
      if (records.length > 0) {
        log(`Processing ${records.length} curtailment records for period ${period}`);
        
        // Insert each record into the database
        for (const record of records) {
          const absVolume = Math.abs(record.volume);
          const payment = absVolume * record.price;
          
          totalVolume += absVolume;
          totalPayment += payment;
          
          await db.insert(curtailmentRecords).values({
            settlementDate: TARGET_DATE,
            settlementPeriod: period,
            farmId: record.farmId,
            leadPartyName: record.leadPartyName,
            volume: record.volume.toString(),
            payment: payment.toString(),
            originalPrice: record.price.toString(),
            finalPrice: record.price.toString(),
            soFlag: record.soFlag,
            cadlFlag: record.cadlFlag
          });
          
          totalRecords++;
        }
        
        log(`Stored ${records.length} records for period ${period}`);
      }
      
      // Short delay to avoid overwhelming the API
      await new Promise(resolve => setTimeout(resolve, 500));
    }
    
    log(`\nProcessed ${totalRecords} total curtailment records`);
    log(`Total volume: ${totalVolume.toFixed(2)} MWh`);
    log(`Total payment: £${totalPayment.toFixed(2)}`);
    
    if (totalRecords === 0) {
      log('No curtailment records found, skipping summary updates.');
      return;
    }
    
    // Step 3: Verify database totals
    const dbTotals = await db
      .select({
        recordCount: sql<string>`COUNT(*)`,
        periodCount: sql<string>`COUNT(DISTINCT ${curtailmentRecords.settlementPeriod})`,
        totalVolume: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
        totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    log(`\nVerified database totals:`);
    log(`Total Records: ${dbTotals[0]?.recordCount || '0'}`);
    log(`Settlement Periods: ${dbTotals[0]?.periodCount || '0'}`);
    log(`Total Volume: ${Number(dbTotals[0]?.totalVolume || 0).toFixed(2)} MWh`);
    log(`Total Payment: £${Number(dbTotals[0]?.totalPayment || 0).toFixed(2)}`);
    
    // Step 4: Update daily summary with accurate totals from database
    log(`\nUpdating daily summary for ${TARGET_DATE}...`);
    
    const dbEnergy = Number(dbTotals[0]?.totalVolume || 0);
    const dbPayment = Number(dbTotals[0]?.totalPayment || 0);
    
    await db.insert(dailySummaries).values({
      summaryDate: TARGET_DATE,
      totalCurtailedEnergy: dbEnergy.toString(),
      totalPayment: (-dbPayment).toString(), // Payment is stored as negative in daily summaries
      lastUpdated: new Date()
    }).onConflictDoUpdate({
      target: [dailySummaries.summaryDate],
      set: {
        totalCurtailedEnergy: dbEnergy.toString(),
        totalPayment: (-dbPayment).toString(),
        lastUpdated: new Date()
      }
    });
    
    // Step 5: Update monthly summary
    const yearMonth = TARGET_DATE.substring(0, 7);
    log(`\nUpdating monthly summary for ${yearMonth}...`);
    
    // Calculate total from all daily summaries in this month
    const monthlyTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(${dailySummaries.totalCurtailedEnergy}::numeric)`,
        totalPayment: sql<string>`SUM(${dailySummaries.totalPayment}::numeric)`
      })
      .from(dailySummaries)
      .where(sql`date_trunc('month', ${dailySummaries.summaryDate}::date) = date_trunc('month', ${TARGET_DATE}::date)`);
    
    if (monthlyTotals[0].totalCurtailedEnergy) {
      await db.insert(monthlySummaries).values({
        yearMonth,
        totalCurtailedEnergy: monthlyTotals[0].totalCurtailedEnergy,
        totalPayment: monthlyTotals[0].totalPayment,
        updatedAt: new Date()
      }).onConflictDoUpdate({
        target: [monthlySummaries.yearMonth],
        set: {
          totalCurtailedEnergy: monthlyTotals[0].totalCurtailedEnergy,
          totalPayment: monthlyTotals[0].totalPayment,
          updatedAt: new Date()
        }
      });
    }
    
    log('\nReprocessing successful');
    
  } catch (error: any) {
    log(`Fatal error during reprocessing: ${error.message}\n${error.stack}`);
    process.exit(1);
  }
}

// Run the reprocessing script
reprocessData().then(() => {
  log('Script execution completed');
}).catch(error => {
  log(`Script execution error: ${error}`);
  process.exit(1);
});