/**
 * Direct April 14 Elexon Reprocessing Script
 * 
 * This script focuses specifically on April 14, 2025 to ensure
 * we get the most complete data from Elexon API.
 * 
 * Run with: npx tsx april14-reprocess.ts
 */

import axios from 'axios';
import { db } from './db';
import { curtailmentRecords, dailySummaries, monthlySummaries } from './db/schema';
import { eq, sql, and } from 'drizzle-orm';
import fs from 'fs';
import path from 'path';

// Constants
const TARGET_DATE = '2025-04-14';
const API_KEY = process.env.ELEXON_API_KEY || '';
const LOG_DIR = './logs';
const LOG_FILE = path.join(LOG_DIR, `elexon_reprocess_${TARGET_DATE.replace(/-/g, '')}_${new Date().toISOString().replace(/:/g, '-').replace(/\./g, '-')}.log`);

// Set up logging
if (!fs.existsSync(LOG_DIR)) {
  fs.mkdirSync(LOG_DIR, { recursive: true });
}
fs.writeFileSync(LOG_FILE, `=== Elexon Reprocessing Log for ${TARGET_DATE} ===\n`);

const log = (message: string) => {
  const timestamp = new Date().toISOString();
  const formattedMessage = `[${timestamp}] ${message}`;
  console.log(formattedMessage);
  fs.appendFileSync(LOG_FILE, formattedMessage + '\n');
};

// Fetch data from Elexon API
async function fetchElexonData(date: string, period: number, retries = 3): Promise<any[]> {
  try {
    log(`Fetching data for ${date} period ${period} from Elexon...`);
    const url = `https://api.bmreports.com/BMRS/B1610/v2?APIKey=${API_KEY}&SettlementDate=${date}&Period=${period}&ServiceType=xml`;
    
    const response = await axios.get(url, { timeout: 10000 });
    
    if (response.status !== 200) {
      throw new Error(`API returned status code ${response.status}`);
    }
    
    const xmlData = response.data;
    const records = parseElexonXML(xmlData);
    
    // Check if we found any records
    log(`Retrieved ${records.length} records for period ${period}`);
    return records;
  } catch (error: any) {
    if (retries > 0) {
      log(`Error fetching data for period ${period} (${retries} retries left): ${error.message}`);
      await new Promise(resolve => setTimeout(resolve, 2000)); // Wait before retrying
      return fetchElexonData(date, period, retries - 1);
    }
    log(`Failed to fetch data for period ${period} after retries: ${error.message}`);
    return [];
  }
}

// Parse Elexon XML data
function parseElexonXML(xmlData: string): any[] {
  const records: any[] = [];
  
  try {
    // Extract all items using regex (simple approach for reliability)
    const itemRegex = /<item>[\s\S]*?<\/item>/g;
    const items = xmlData.match(itemRegex) || [];
    
    for (const item of items) {
      try {
        // Extract required fields
        const bmuIdMatch = item.match(/<bm_unit_id>(.*?)<\/bm_unit_id>/);
        const volumeMatch = item.match(/<volume>(.*?)<\/volume>/);
        const priceMatch = item.match(/<price>(.*?)<\/price>/);
        const soFlagMatch = item.match(/<so_flag>(.*?)<\/so_flag>/);
        const cadlFlagMatch = item.match(/<cadl_flag>(.*?)<\/cadl_flag>/);
        const leadPartyMatch = item.match(/<lead_party>(.*?)<\/lead_party>/);
        
        if (bmuIdMatch && volumeMatch && priceMatch) {
          const record = {
            id: bmuIdMatch[1],
            volume: parseFloat(volumeMatch[1]),
            originalPrice: parseFloat(priceMatch[1]),
            finalPrice: parseFloat(priceMatch[1]), // Same as original for simplicity
            soFlag: soFlagMatch ? soFlagMatch[1] === 'Y' : false,
            cadlFlag: cadlFlagMatch ? cadlFlagMatch[1] === 'Y' : false,
            leadPartyName: leadPartyMatch ? leadPartyMatch[1] : 'Unknown'
          };
          
          // Only include curtailment records (negative volume with flags)
          if (record.volume < 0 && (record.soFlag || record.cadlFlag)) {
            records.push(record);
          }
        }
      } catch (err: any) {
        log(`Error parsing item: ${err.message}`);
      }
    }
  } catch (err: any) {
    log(`Error parsing XML: ${err.message}`);
  }
  
  return records;
}

// Main function
async function reprocessData() {
  log(`Starting reprocessing for ${TARGET_DATE}`);
  
  try {
    // Step 1: Clear existing data for the target date
    log(`Removing existing curtailment records for ${TARGET_DATE}...`);
    await db.delete(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    // Step 2: Process ALL settlement periods with a focus on thoroughness
    const allPeriods = Array.from({ length: 48 }, (_, i) => i + 1);
    
    // Process in batches of 5 for efficiency while avoiding rate limits
    const BATCH_SIZE = 5;
    let totalRecords = 0;
    let totalVolume = 0;
    let totalPayment = 0;
    let periodsWithData = 0;
    
    for (let i = 0; i < allPeriods.length; i += BATCH_SIZE) {
      const batch = allPeriods.slice(i, i + BATCH_SIZE);
      const batchResults = await Promise.all(
        batch.map(period => fetchElexonData(TARGET_DATE, period))
      );
      
      // Process each period's results
      for (let j = 0; j < batch.length; j++) {
        const period = batch[j];
        const records = batchResults[j];
        
        if (records.length > 0) {
          periodsWithData++;
          log(`Processing ${records.length} curtailment records for period ${period}`);
          
          const recordsToInsert = records.map(record => {
            const absVolume = Math.abs(record.volume);
            const payment = absVolume * record.originalPrice;
            
            totalVolume += absVolume;
            totalPayment += payment;
            
            return {
              settlementDate: TARGET_DATE,
              settlementPeriod: period,
              farmId: record.id,
              leadPartyName: record.leadPartyName,
              volume: record.volume.toString(),
              payment: payment.toString(),
              originalPrice: record.originalPrice.toString(),
              finalPrice: record.finalPrice.toString(),
              soFlag: record.soFlag,
              cadlFlag: record.cadlFlag || null
            };
          });
          
          if (recordsToInsert.length > 0) {
            await db.insert(curtailmentRecords).values(recordsToInsert);
            totalRecords += recordsToInsert.length;
            
            log(`Stored ${recordsToInsert.length} records for period ${period}`);
          }
        } else {
          log(`No curtailment records found for period ${period}`);
        }
      }
      
      // Add a short delay between batches to avoid overloading the API
      if (i + BATCH_SIZE < allPeriods.length) {
        await new Promise(resolve => setTimeout(resolve, 1000));
      }
    }
    
    log(`\nProcessed ${totalRecords} total curtailment records across ${periodsWithData} periods`);
    log(`Total volume: ${totalVolume.toFixed(2)} MWh`);
    log(`Total payment: £${totalPayment.toFixed(2)}`);
    
    if (totalRecords === 0) {
      log('No curtailment records found, skipping summary updates.');
      return;
    }
    
    // Step 3: Verify database totals
    const dbTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
        totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    const dbEnergy = Number(dbTotals[0]?.totalCurtailedEnergy || 0);
    const dbPayment = Number(dbTotals[0]?.totalPayment || 0);
    
    log(`\nVerified database totals:`);
    log(`Total curtailed energy: ${dbEnergy.toFixed(2)} MWh`);
    log(`Total payment: £${dbPayment.toFixed(2)}`);
    
    // Step 4: Update daily summary with accurate totals from database
    log(`\nUpdating daily summary for ${TARGET_DATE}...`);
    await db.insert(dailySummaries).values({
      summaryDate: TARGET_DATE,
      totalCurtailedEnergy: dbEnergy.toString(),
      totalPayment: dbPayment.toString(),
      lastUpdated: new Date()
    }).onConflictDoUpdate({
      target: [dailySummaries.summaryDate],
      set: {
        totalCurtailedEnergy: dbEnergy.toString(),
        totalPayment: dbPayment.toString(),
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

    if (monthlyTotals[0].totalCurtailedEnergy && monthlyTotals[0].totalPayment) {
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
    
    // Final verification
    const verification = await db.select({
      recordCount: sql<string>`COUNT(*)`,
      periodCount: sql<string>`COUNT(DISTINCT ${curtailmentRecords.settlementPeriod})`,
      totalVolume: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
      totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    log(`\nFinal Verification:`);
    log(`Total Records: ${verification[0]?.recordCount || '0'}`);
    log(`Settlement Periods: ${verification[0]?.periodCount || '0'}`);
    log(`Total Volume: ${Number(verification[0]?.totalVolume || 0).toFixed(2)} MWh`);
    log(`Total Payment: £${Number(verification[0]?.totalPayment || 0).toFixed(2)}`);
    
    log('\nReprocessing successful');
    
  } catch (error: any) {
    log(`Fatal error during reprocessing: ${error.message}\n${error.stack}`);
    process.exit(1);
  }
}

// Run the reprocessing script
reprocessData().then(() => {
  log('Script execution completed');
  process.exit(0);
}).catch(error => {
  log(`Script execution error: ${error}`);
  process.exit(1);
});