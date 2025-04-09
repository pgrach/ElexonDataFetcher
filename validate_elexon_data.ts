/**
 * Validate Elexon API Data for 2025-04-01
 * 
 * This script fetches data directly from the Elexon API for 2025-04-01
 * and compares it with the data stored in the curtailment_records table.
 */

import { fetchBidsOffers } from "./server/services/elexon";
import { db } from "./db";
import { curtailmentRecords } from "./db/schema";
import { eq } from "drizzle-orm";

const TARGET_DATE = '2025-04-01';
// Check specific periods instead of all 48 to make the script faster
const PERIODS_TO_CHECK = [18, 24, 29, 36, 41, 44];

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
    console.log(`\n=== ${source} Summary ===`);
    console.log(`Records: ${this.recordCount}`);
    console.log(`Periods with data: ${this.periodCount}`);
    console.log(`Total volume: ${this.totalVolume.toFixed(2)} MWh`);
    console.log(`Total payment: £${this.totalPayment.toFixed(2)}`);
  }
}

async function validateElexonData(): Promise<void> {
  try {
    console.log(`\n=== Validating Elexon API Data for ${TARGET_DATE} ===\n`);
    
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
    
    // Populate DB accumulator
    for (const period in dbByPeriod) {
      const data = dbByPeriod[parseInt(period)];
      dbData.addPeriodData(parseInt(period), data.volume, data.payment, data.count);
    }
    
    // Fetch data from Elexon API for selected periods
    console.log("\nFetching data from Elexon API for sample periods...");
    
    for (const period of PERIODS_TO_CHECK) {
      try {
        const apiRecords = await fetchBidsOffers(TARGET_DATE, period);
        
        if (apiRecords.length > 0) {
          const periodVolume = apiRecords.reduce((sum, record) => sum + Math.abs(record.volume), 0);
          const periodPayment = apiRecords.reduce((sum, record) => sum + (Math.abs(record.volume) * record.originalPrice * -1), 0);
          
          apiData.addPeriodData(period, periodVolume, periodPayment, apiRecords.length);
          
          console.log(`Period ${period}: ${apiRecords.length} records, ${periodVolume.toFixed(2)} MWh, £${periodPayment.toFixed(2)}`);
        }
      } catch (error) {
        console.error(`Error fetching data for period ${period}:`, error);
      }
    }
    
    // Print summaries
    apiData.printSummary("Elexon API");
    dbData.printSummary("Database");
    
    // Filter DB data to only include the periods we checked
    const dbDataForSample = new VolumeAccumulator();
    for (const period of PERIODS_TO_CHECK) {
      if (dbByPeriod[period]) {
        const data = dbByPeriod[period];
        dbDataForSample.addPeriodData(period, data.volume, data.payment, data.count);
      }
    }
    
    // Print filtered DB data summary
    dbDataForSample.printSummary("Database (Sample Periods Only)");
    
    // Compare totals
    console.log("\n=== Comparison ===");
    const volumeDiff = Math.abs(apiData.totalVolume - dbDataForSample.totalVolume);
    const paymentDiff = Math.abs(apiData.totalPayment - dbDataForSample.totalPayment);
    const recordDiff = Math.abs(apiData.recordCount - dbDataForSample.recordCount);
    
    console.log(`Volume difference for sample periods: ${volumeDiff.toFixed(2)} MWh`);
    console.log(`Payment difference for sample periods: £${paymentDiff.toFixed(2)}`);
    console.log(`Record count difference for sample periods: ${recordDiff}`);
    
    // Show totals across all periods in DB for reference
    console.log(`\nTotal in database across all periods: ${dbData.totalVolume.toFixed(2)} MWh, £${dbData.totalPayment.toFixed(2)}, ${dbData.recordCount} records`);
    
    // Calculate missing periods in the sample we checked
    const apiPeriods = new Set(Object.keys(apiData.periodCounts).map(p => parseInt(p)));
    const dbPeriodsWeChecked = new Set(PERIODS_TO_CHECK.filter(p => dbByPeriod[p]));
    
    const missingInDb = [...apiPeriods].filter(p => !dbPeriodsWeChecked.has(p));
    const missingInApi = [...dbPeriodsWeChecked].filter(p => !apiPeriods.has(p));
    
    if (missingInDb.length > 0) {
      console.log(`\nPeriods in API but missing in DB: ${missingInDb.join(', ')}`);
    }
    
    if (missingInApi.length > 0) {
      console.log(`\nPeriods in DB but missing in API: ${missingInApi.join(', ')}`);
    }
    
    // Conclusion
    const threshold = 0.01; // 1% threshold for discrepancy
    const volumePercentDiff = (volumeDiff / apiData.totalVolume) * 100;
    
    if (volumePercentDiff > threshold) {
      console.log(`\n⚠️ Data discrepancy detected: ${volumePercentDiff.toFixed(2)}% difference in volume`);
      console.log(`Consider reingesting data from the Elexon API for ${TARGET_DATE}`);
    } else {
      console.log(`\n✓ Data validation passed: ${volumePercentDiff.toFixed(2)}% difference is within threshold`);
    }
    
  } catch (error) {
    console.error(`Error validating Elexon data:`, error);
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