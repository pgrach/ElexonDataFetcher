/**
 * Simplified Data Reprocessing Script for 2025-04-14
 * 
 * This script reprocesses a set of periods for 2025-04-14.
 * It can be run multiple times with different period ranges.
 * 
 * Run with: npx tsx reprocess-april14-simple.ts START_PERIOD END_PERIOD
 * Example: npx tsx reprocess-april14-simple.ts 1 10
 */

import { db } from './db';
import { curtailmentRecords } from './db/schema';
import { eq, and } from 'drizzle-orm';
import { format } from 'date-fns';
import { fetchBidsOffers } from './server/services/elexon';

const TARGET_DATE = '2025-04-14';

// Get period range from command line
const startPeriodArg = process.argv[2];
const endPeriodArg = process.argv[3];

const startPeriod = startPeriodArg ? parseInt(startPeriodArg, 10) : 1;
const endPeriod = endPeriodArg ? parseInt(endPeriodArg, 10) : 5;

if (isNaN(startPeriod) || isNaN(endPeriod) || startPeriod < 1 || endPeriod > 48 || startPeriod > endPeriod) {
  console.error('Invalid period range. Please specify START_PERIOD and END_PERIOD between 1 and 48.');
  process.exit(1);
}

// Define the periods to process
const PERIODS_TO_PROCESS = Array.from({ length: endPeriod - startPeriod + 1 }, (_, i) => i + startPeriod);

// Process a single settlement period
async function processSettlementPeriod(date: string, period: number): Promise<{ records: number; volume: number; payment: number }> {
  console.log(`[${date} P${period}] Processing settlement period...`);
  
  let records = 0;
  let volume = 0;
  let payment = 0;
  
  try {
    // Fetch data from Elexon API
    const elexonRecords = await fetchBidsOffers(date, period);
    console.log(`[${date} P${period}] Retrieved ${elexonRecords.length} records from Elexon`);
    
    // First delete any existing records for this period
    await db.delete(curtailmentRecords)
      .where(and(
        eq(curtailmentRecords.settlementDate, date),
        eq(curtailmentRecords.settlementPeriod, period)
      ));
    
    // Filter for curtailment records (negative volume with flags)
    const curtailmentRecordsToInsert = elexonRecords
      .filter(record => record.volume < 0 && (record.soFlag || record.cadlFlag))
      .map(record => {
        const absVolume = Math.abs(record.volume);
        const paymentAmount = absVolume * record.originalPrice;
        
        records++;
        volume += absVolume;
        payment += paymentAmount;
        
        return {
          settlementDate: date,
          settlementPeriod: period,
          farmId: record.id,
          leadPartyName: record.leadPartyName || 'Unknown',
          volume: record.volume.toString(), // Keep negative
          payment: paymentAmount.toString(),
          originalPrice: record.originalPrice.toString(),
          finalPrice: record.finalPrice.toString(),
          soFlag: record.soFlag,
          cadlFlag: record.cadlFlag
        };
      });
    
    // Insert all valid records for this period
    if (curtailmentRecordsToInsert.length > 0) {
      await db.insert(curtailmentRecords).values(curtailmentRecordsToInsert);
      console.log(`[${date} P${period}] Inserted ${curtailmentRecordsToInsert.length} curtailment records`);
      console.log(`[${date} P${period}] Volume: ${volume.toFixed(2)} MWh, Payment: £${payment.toFixed(2)}`);
    } else {
      console.log(`[${date} P${period}] No valid curtailment records found`);
    }
    
    return { records, volume, payment };
  } catch (error) {
    console.error(`[${date} P${period}] Error processing period:`, error);
    return { records: 0, volume: 0, payment: 0 };
  }
}

async function processRangeOfPeriods() {
  console.log(`\n=== Processing Periods ${startPeriod}-${endPeriod} for ${TARGET_DATE} ===`);
  console.log(`Start Time: ${format(new Date(), "yyyy-MM-dd HH:mm:ss")}`);
  
  let totalRecords = 0;
  let totalVolume = 0;
  let totalPayment = 0;
  const periodsWithData = new Set<number>();
  
  for (const period of PERIODS_TO_PROCESS) {
    try {
      const result = await processSettlementPeriod(TARGET_DATE, period);
      
      totalRecords += result.records;
      totalVolume += result.volume;
      totalPayment += result.payment;
      
      if (result.records > 0) {
        periodsWithData.add(period);
      }
      
      // Short delay between periods to avoid rate limiting
      await new Promise(resolve => setTimeout(resolve, 100));
    } catch (error) {
      console.error(`Error processing period ${period}:`, error);
    }
  }
  
  console.log(`\n=== Processing Complete ===`);
  console.log(`Processed periods ${startPeriod}-${endPeriod}`);
  console.log(`Periods with data: ${Array.from(periodsWithData).join(', ')}`);
  console.log(`Total records: ${totalRecords}`);
  console.log(`Total volume: ${totalVolume.toFixed(2)} MWh`);
  console.log(`Total payment: £${totalPayment.toFixed(2)}`);
  console.log(`End Time: ${format(new Date(), "yyyy-MM-dd HH:mm:ss")}`);
}

// Run the processing script
processRangeOfPeriods().catch(error => {
  console.error("Script execution error:", error);
  process.exit(1);
});