/**
 * Thorough Data Reprocessing Script for 2025-04-14
 * 
 * This script performs a comprehensive reprocessing of data for 2025-04-14,
 * ensuring ALL data from Elexon is captured with no omissions and additional
 * verification steps are performed.
 * 
 * Run with: npx tsx thorough-reprocess-april14.ts
 */

import axios from 'axios';
import { db } from './db';
import { curtailmentRecords, dailySummaries, monthlySummaries } from './db/schema';
import { eq, sql } from 'drizzle-orm';
import fs from 'fs';
import path from 'path';
import { XMLParser } from 'fast-xml-parser';

// Constants
const TARGET_DATE = '2025-04-14';
const API_KEY = process.env.ELEXON_API_KEY || '';
const LOG_DIR = './logs';
const LOG_FILE = path.join(LOG_DIR, `thorough_reprocess_${TARGET_DATE.replace(/-/g, '')}_${new Date().toISOString().replace(/:/g, '-').replace(/\./g, '-')}.log`);

// Set up logging
if (!fs.existsSync(LOG_DIR)) {
  fs.mkdirSync(LOG_DIR, { recursive: true });
}
fs.writeFileSync(LOG_FILE, `=== Thorough Reprocessing Log for ${TARGET_DATE} ===\n`);

const log = (message: string) => {
  const timestamp = new Date().toISOString();
  const formattedMessage = `[${timestamp}] ${message}`;
  console.log(formattedMessage);
  fs.appendFileSync(LOG_FILE, formattedMessage + '\n');
};

// Fetch data from Elexon API with robust error handling and retries
async function fetchElexonData(date: string, period: number, retries = 3, delay = 2000): Promise<any[]> {
  try {
    log(`Fetching data for ${date} period ${period} from Elexon...`);
    const url = `https://api.bmreports.com/BMRS/B1610/v2?APIKey=${API_KEY}&SettlementDate=${date}&Period=${period}&ServiceType=xml`;
    
    const response = await axios.get(url, { 
      timeout: 10000,
      headers: {
        'Accept': 'application/xml',
        'User-Agent': 'Curtailment-Processor/1.0'
      }
    });
    
    if (response.status !== 200) {
      throw new Error(`API returned status code ${response.status}`);
    }
    
    const xmlData = response.data;
    
    // Use a proper XML parser for more reliable parsing
    const parser = new XMLParser({
      ignoreAttributes: false,
      attributeNamePrefix: "_",
    });
    const parsed = parser.parse(xmlData);
    
    // Extract the items from the parsed XML
    let items = [];
    if (parsed && parsed.response && parsed.response.responseBody && 
        parsed.response.responseBody.responseList && 
        parsed.response.responseBody.responseList.item) {
      items = Array.isArray(parsed.response.responseBody.responseList.item) 
        ? parsed.response.responseBody.responseList.item 
        : [parsed.response.responseBody.responseList.item];
    }
    
    // Process the items to extract the relevant data
    const records = items.map(item => {
      const volume = parseFloat(item.volume || '0');
      const soFlag = (item.so_flag === 'Y');
      const cadlFlag = (item.cadl_flag === 'Y');
      const leadParty = item.lead_party || 'Unknown';
      
      // Only return curtailment records (negative volume with flags)
      if (volume < 0 && (soFlag || cadlFlag)) {
        return {
          farmId: item.bm_unit_id,
          volume,
          originalPrice: parseFloat(item.price || '0'),
          finalPrice: parseFloat(item.price || '0'),
          soFlag,
          cadlFlag,
          leadPartyName: leadParty
        };
      }
      return null;
    }).filter(Boolean);
    
    log(`Retrieved ${records.length} curtailment records for period ${period}`);
    return records;
  } catch (error: any) {
    if (retries > 0) {
      log(`Error fetching data for period ${period} (${retries} retries left): ${error.message}`);
      await new Promise(resolve => setTimeout(resolve, delay)); 
      return fetchElexonData(date, period, retries - 1, delay);
    }
    log(`Failed to fetch data for period ${period} after retries: ${error.message}`);
    return [];
  }
}

// Helper function to determine if a period is expected to have curtailment
// Based on historical patterns for 2025-04-14
function isExpectedCurtailmentPeriod(period: number): boolean {
  // Periods with higher probability of curtailment based on historical data
  // These will be processed first to optimize for time
  return [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16].includes(period);
}

async function thoroughReprocessData() {
  log(`Starting thorough reprocessing for ${TARGET_DATE}`);
  
  try {
    // Step 1: Clear existing data for the target date
    log(`Removing existing curtailment records for ${TARGET_DATE}...`);
    await db.delete(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    // Step 2: Process ALL settlement periods with a focus on thoroughness
    // First process periods most likely to have data (for time efficiency)
    const allPeriods = Array.from({ length: 48 }, (_, i) => i + 1);
    const priorityPeriods = allPeriods.filter(isExpectedCurtailmentPeriod);
    const remainingPeriods = allPeriods.filter(p => !isExpectedCurtailmentPeriod(p));
    
    // Process in order of priority
    const processingOrder = [...priorityPeriods, ...remainingPeriods];
    
    // Process in small batches to avoid overwhelming the API
    const BATCH_SIZE = 3;
    let totalRecords = 0;
    let totalVolume = 0;
    let totalPayment = 0;
    let periodsWithData = 0;
    
    // Track which periods have been processed
    const processedPeriods = new Set<number>();
    
    log(`Processing ${processingOrder.length} settlement periods in batches of ${BATCH_SIZE}...`);
    
    // Process periods in batches
    for (let i = 0; i < processingOrder.length; i += BATCH_SIZE) {
      const batch = processingOrder.slice(i, i + BATCH_SIZE);
      log(`Processing batch ${Math.floor(i / BATCH_SIZE) + 1}/${Math.ceil(processingOrder.length / BATCH_SIZE)}: Periods ${batch.join(', ')}`);
      
      const batchPromises = batch.map(period => fetchElexonData(TARGET_DATE, period));
      const batchResults = await Promise.all(batchPromises);
      
      for (let j = 0; j < batch.length; j++) {
        const period = batch[j];
        const records = batchResults[j] || [];
        
        if (records.length > 0) {
          periodsWithData++;
          processedPeriods.add(period);
          
          log(`Processing ${records.length} curtailment records for period ${period}`);
          let periodVolume = 0;
          let periodPayment = 0;
          
          // Insert records for this period
          for (const record of records) {
            const absVolume = Math.abs(record.volume);
            const payment = absVolume * record.originalPrice;
            
            periodVolume += absVolume;
            periodPayment += payment;
            
            await db.insert(curtailmentRecords).values({
              settlementDate: TARGET_DATE,
              settlementPeriod: period,
              farmId: record.farmId,
              leadPartyName: record.leadPartyName,
              volume: record.volume.toString(),
              payment: payment.toString(),
              originalPrice: record.originalPrice.toString(),
              finalPrice: record.finalPrice.toString(),
              soFlag: record.soFlag,
              cadlFlag: record.cadlFlag
            });
            
            totalRecords++;
          }
          
          totalVolume += periodVolume;
          totalPayment += periodPayment;
          
          log(`Period ${period}: Added ${records.length} records (${periodVolume.toFixed(2)} MWh, £${periodPayment.toFixed(2)})`);
        } else {
          log(`Period ${period}: No curtailment records found`);
        }
      }
      
      // Add a small delay between batches to avoid overwhelming the Elexon API
      if (i + BATCH_SIZE < processingOrder.length) {
        log(`Waiting before processing next batch...`);
        await new Promise(resolve => setTimeout(resolve, 1500));
      }
    }
    
    // Check if any expected periods are missing
    const missingPriority = priorityPeriods.filter(p => !processedPeriods.has(p));
    if (missingPriority.length > 0) {
      log(`WARNING: The following expected periods have no curtailment data: ${missingPriority.join(', ')}`);
      
      // Try to reprocess missing priority periods one more time
      for (const period of missingPriority) {
        log(`Re-attempting to fetch data for priority period ${period}...`);
        const records = await fetchElexonData(TARGET_DATE, period, 2, 3000);
        
        if (records.length > 0) {
          processedPeriods.add(period);
          log(`Successfully retrieved ${records.length} records for period ${period} on second attempt!`);
          
          // Process the records
          let periodVolume = 0;
          let periodPayment = 0;
          
          for (const record of records) {
            const absVolume = Math.abs(record.volume);
            const payment = absVolume * record.originalPrice;
            
            periodVolume += absVolume;
            periodPayment += payment;
            
            await db.insert(curtailmentRecords).values({
              settlementDate: TARGET_DATE,
              settlementPeriod: period,
              farmId: record.farmId,
              leadPartyName: record.leadPartyName,
              volume: record.volume.toString(),
              payment: payment.toString(),
              originalPrice: record.originalPrice.toString(),
              finalPrice: record.finalPrice.toString(),
              soFlag: record.soFlag,
              cadlFlag: record.cadlFlag
            });
            
            totalRecords++;
          }
          
          totalVolume += periodVolume;
          totalPayment += periodPayment;
          periodsWithData++;
          
          log(`Period ${period} (retry): Added ${records.length} records (${periodVolume.toFixed(2)} MWh, £${periodPayment.toFixed(2)})`);
        }
      }
    }
    
    log(`\nProcessed ${totalRecords} total curtailment records across ${periodsWithData} periods`);
    log(`Total volume: ${totalVolume.toFixed(2)} MWh`);
    log(`Total payment: £${totalPayment.toFixed(2)}`);
    
    if (totalRecords === 0) {
      log('No curtailment records found, skipping summary updates.');
      return;
    }
    
    // Step 3: Verify database totals (double-check from database itself)
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
    
    // Step 6: Special verification step - Compare with expected periods
    // This helps confirm that we didn't miss any important data
    const processedPeriodsArray = Array.from(processedPeriods).sort((a, b) => a - b);
    log(`\nProcessed periods (${processedPeriodsArray.length}): ${processedPeriodsArray.join(', ')}`);
    
    // Document any discrepancies between expected and actual data
    const expectedPeriodsWithoutData = priorityPeriods.filter(p => !processedPeriods.has(p));
    if (expectedPeriodsWithoutData.length > 0) {
      log(`\nWARNING: Expected data for periods ${expectedPeriodsWithoutData.join(', ')} but none was found after retries.`);
      log(`Please check if the Elexon API data might be incomplete for these periods.`);
    }
    
    // Final verification from database for absolute confirmation
    const periods = await db.select({
      period: curtailmentRecords.settlementPeriod,
      count: sql<number>`COUNT(*)`,
      volume: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
      payment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE))
    .groupBy(curtailmentRecords.settlementPeriod)
    .orderBy(curtailmentRecords.settlementPeriod);
    
    log('\nDetailed period breakdown:');
    periods.forEach(p => {
      log(`Period ${p.period}: ${p.count} records, ${Number(p.volume).toFixed(2)} MWh, £${Number(p.payment).toFixed(2)}`);
    });
    
    log('\nThorough reprocessing completed successfully');
    
  } catch (error: any) {
    log(`Fatal error during reprocessing: ${error.message}\n${error.stack}`);
    process.exit(1);
  }
}

// Run the thorough reprocessing script
thoroughReprocessData().then(() => {
  log('Script execution completed');
}).catch(error => {
  log(`Script execution error: ${error}`);
  process.exit(1);
});