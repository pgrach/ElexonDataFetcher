/**
 * Validate Elexon API Data - Batch 1 (Periods 1-16)
 * 
 * This script fetches data directly from the Elexon API for 2025-04-01
 * and compares it with the data stored in the curtailment_records table.
 * 
 * This file processes periods 1-16 to avoid timeouts.
 */

import { fetchBidsOffers } from "./elexon_validation";
import { curtailmentRecords } from "./db/schema";
import { db } from "./db";
import { eq } from "drizzle-orm";

const TARGET_DATE = '2025-04-01';
// Check periods 1-16 in this batch
const PERIODS_TO_CHECK = Array.from({ length: 16 }, (_, i) => i + 1);

// Create a class to accumulate totals
class VolumeAccumulator {
  totalVolume: number = 0;
  totalPayment: number = 0;
  recordCount: number = 0;
  periodCount: number = 0;
  periodCounts: Record<number, number> = {};
  
  addPeriodData(period: number, volume: number, payment: number, count: number) {
    if (count > 0) {
      this.totalVolume += volume;
      this.totalPayment += payment;
      this.recordCount += count;
      this.periodCount++;
      this.periodCounts[period] = count;
    }
  }
  
  printSummary(source: string) {
    console.log(`\n=== ${source} Summary (Batch 1: Periods 1-16) ===`);
    console.log(`Records: ${this.recordCount}`);
    console.log(`Periods with data: ${this.periodCount}`);
    console.log(`Total volume: ${this.totalVolume.toFixed(2)} MWh`);
    console.log(`Total payment: £${this.totalPayment.toFixed(2)}`);
  }
}

async function validateElexonData(): Promise<void> {
  try {
    console.log(`\n=== Validating Elexon API Data for ${TARGET_DATE} (Batch 1: Periods 1-16) ===\n`);
    
    // Fetch data from database
    const dbRecords = await db
      .select()
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log(`Found ${dbRecords.length} records in database for ${TARGET_DATE}`);
    
    // Create accumulators for API and DB data
    const apiData = new VolumeAccumulator();
    const dbData = new VolumeAccumulator();
    
    // Process DB data
    const dbByPeriod: Record<number, {volume: number, payment: number, count: number}> = {};
    
    for (const record of dbRecords) {
      const period = record.settlementPeriod;
      const volume = Math.abs(parseFloat(record.volume));
      const payment = parseFloat(record.payment);
      
      if (!dbByPeriod[period]) {
        dbByPeriod[period] = {volume: 0, payment: 0, count: 0};
      }
      
      dbByPeriod[period].volume += volume;
      dbByPeriod[period].payment += payment;
      dbByPeriod[period].count++;
    }
    
    // Populate DB accumulator (only for periods in this batch)
    for (const period in dbByPeriod) {
      const periodNum = parseInt(period);
      if (PERIODS_TO_CHECK.includes(periodNum)) {
        const data = dbByPeriod[periodNum];
        dbData.addPeriodData(periodNum, data.volume, data.payment, data.count);
      }
    }
    
    // Fetch data from Elexon API for periods in this batch
    console.log(`\nFetching data from Elexon API for periods 1-16...`);
    
    // Process each period one at a time
    for (const period of PERIODS_TO_CHECK) {
      try {
        console.log(`\nFetching data for period ${period}...`);
        const apiRecords = await fetchBidsOffers(TARGET_DATE, period);
        
        if (apiRecords.length > 0) {
          const periodVolume = apiRecords.reduce((sum, record) => sum + Math.abs(record.volume), 0);
          const periodPayment = apiRecords.reduce((sum, record) => sum + (Math.abs(record.volume) * record.originalPrice * -1), 0);
          
          apiData.addPeriodData(period, periodVolume, periodPayment, apiRecords.length);
          
          console.log(`Period ${period}: ${apiRecords.length} records, ${periodVolume.toFixed(2)} MWh, £${periodPayment.toFixed(2)}`);
        } else {
          console.log(`Period ${period}: No records found`);
        }
        
        // Add a small delay between periods to avoid rate limiting
        if (period !== PERIODS_TO_CHECK[PERIODS_TO_CHECK.length - 1]) {
          console.log("Waiting 1 second before next period...");
          await new Promise(resolve => setTimeout(resolve, 1000));
        }
      } catch (error) {
        console.error(`Error fetching data for period ${period}:`, error);
      }
    }
    
    // Print summaries
    apiData.printSummary("Elexon API");
    dbData.printSummary("Database");
    
    // Compare totals between API and DB for this batch
    console.log("\n=== Batch 1 Comparison (Periods 1-16) ===");
    const volumeDiff = Math.abs(apiData.totalVolume - dbData.totalVolume);
    const paymentDiff = Math.abs(apiData.totalPayment - Math.abs(dbData.totalPayment));
    const recordDiff = Math.abs(apiData.recordCount - dbData.recordCount);
    
    console.log(`Volume difference: ${volumeDiff.toFixed(2)} MWh`);
    console.log(`Volume in API: ${apiData.totalVolume.toFixed(2)} MWh`);
    console.log(`Volume in DB: ${dbData.totalVolume.toFixed(2)} MWh`);
    console.log(`Payment difference: £${paymentDiff.toFixed(2)}`);
    console.log(`Payment in API: £${apiData.totalPayment.toFixed(2)}`);
    console.log(`Payment in DB: £${dbData.totalPayment.toFixed(2)}`);
    console.log(`Record count difference: ${recordDiff}`);
    console.log(`Records in API: ${apiData.recordCount}`);
    console.log(`Records in DB: ${dbData.recordCount}`);
    
    // Calculate missing periods in this batch
    const apiPeriods = new Set(Object.keys(apiData.periodCounts).map(p => parseInt(p)));
    const dbPeriodsInBatch = new Set(PERIODS_TO_CHECK.filter(p => dbByPeriod[p]));
    
    const missingInDb = [...apiPeriods].filter(p => !dbPeriodsInBatch.has(p));
    const missingInApi = [...dbPeriodsInBatch].filter(p => !apiPeriods.has(p));
    
    if (missingInDb.length > 0) {
      console.log(`\nPeriods in API but missing in DB: ${missingInDb.join(', ')}`);
    }
    
    if (missingInApi.length > 0) {
      console.log(`\nPeriods in DB but missing in API: ${missingInApi.join(', ')}`);
    }
    
    // Conclusion for this batch
    const threshold = 0.01; // 1% threshold for discrepancy
    let volumePercentDiff = 0;
    
    if (apiData.totalVolume > 0) {
      volumePercentDiff = (volumeDiff / apiData.totalVolume) * 100;
      
      if (volumePercentDiff > threshold) {
        console.log(`\n⚠️ Data discrepancy detected: ${volumePercentDiff.toFixed(2)}% difference in volume for batch 1`);
      } else {
        console.log(`\n✓ Batch 1 validation passed: ${volumePercentDiff.toFixed(2)}% difference is within threshold`);
      }
    } else {
      console.log(`\nNo volume data from API for comparison in batch 1`);
    }
    
    // Create a results file for batch 1
    await writeResults('batch1_results.json', {
      apiData: {
        totalVolume: apiData.totalVolume,
        totalPayment: apiData.totalPayment,
        recordCount: apiData.recordCount,
        periodCount: apiData.periodCount,
        periods: apiData.periodCounts
      },
      dbData: {
        totalVolume: dbData.totalVolume,
        totalPayment: dbData.totalPayment,
        recordCount: dbData.recordCount,
        periodCount: dbData.periodCount
      },
      differences: {
        volumeDiff,
        paymentDiff,
        recordDiff,
        volumePercentDiff,
        missingInDb,
        missingInApi
      }
    });
    
  } catch (error) {
    console.error(`Error validating Elexon data:`, error);
  }
}

async function writeResults(fileName: string, data: any): Promise<void> {
  try {
    const fs = require('fs');
    await fs.promises.writeFile(fileName, JSON.stringify(data, null, 2));
    console.log(`Results written to ${fileName}`);
  } catch (error) {
    console.error(`Error writing results:`, error);
  }
}

async function main(): Promise<void> {
  try {
    await validateElexonData();
    process.exit(0);
  } catch (error) {
    console.error('Script failed:', error);
    process.exit(1);
  }
}

// Execute main function
main();