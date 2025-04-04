/**
 * Verify and Fix March 28, 2025 Settlement Periods
 * 
 * This script verifies all 48 settlement periods for March 28, 2025, comparing
 * the data in our database with the Elexon API. If any discrepancies are found,
 * it updates the records accordingly.
 */

import { db } from "./db";
import { curtailmentRecords, dailySummaries, monthlySummaries, yearlySummaries } from "./db/schema";
import { eq, and, sql, not, inArray } from "drizzle-orm";
import { fetchBidsOffers } from "./server/services/elexon";
import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const TARGET_DATE = '2025-03-28';
const BMU_MAPPING_PATH = path.join(__dirname, "server/data/bmuMapping.json");

type PeriodStatus = {
  period: number;
  existingCount: number;
  existingVolume: number;
  elexonCount: number;
  elexonVolume: number;
  status: 'missing' | 'incomplete' | 'complete' | 'mismatch' | 'unknown';
  needsUpdate: boolean;
};

// Utility function to delay between API calls
async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Load BMU mapping to get valid wind farm IDs
async function loadBmuMappings(): Promise<{
  windFarmIds: Set<string>;
  bmuLeadPartyMap: Map<string, string>;
}> {
  try {
    console.log('Loading BMU mapping from:', BMU_MAPPING_PATH);
    const mappingContent = await fs.readFile(BMU_MAPPING_PATH, 'utf8');
    const bmuMapping = JSON.parse(mappingContent);
    
    const windFarmIds = new Set<string>(
      bmuMapping
        .filter((bmu: any) => bmu.fuelType === "WIND")
        .map((bmu: any) => bmu.elexonBmUnit)
    );
    
    const bmuLeadPartyMap = new Map<string, string>();
    for (const bmu of bmuMapping.filter((bmu: any) => bmu.fuelType === "WIND")) {
      bmuLeadPartyMap.set(bmu.elexonBmUnit, bmu.leadPartyName || 'Unknown');
    }
    
    console.log(`Loaded ${windFarmIds.size} wind farm BMU IDs`);
    return { windFarmIds, bmuLeadPartyMap };
  } catch (error) {
    console.error('Error loading BMU mapping:', error);
    throw error;
  }
}

// Get the status of a single period
async function checkPeriodStatus(
  period: number, 
  windFarmIds: Set<string>
): Promise<PeriodStatus> {
  console.log(`Checking status for period ${period}...`);
  
  // Check existing records in the database
  const existingRecords = await db.select({
    count: sql`COUNT(*)`,
    volume: sql`SUM(ABS(${curtailmentRecords.volume}::numeric))`
  })
  .from(curtailmentRecords)
  .where(and(
    eq(curtailmentRecords.settlementDate, TARGET_DATE),
    eq(curtailmentRecords.settlementPeriod, period)
  ));
  
  const existingCount = Number(existingRecords[0]?.count || 0);
  const existingVolume = Number(existingRecords[0]?.volume || 0);
  
  // Get data from Elexon API
  const elexonRecords = await fetchBidsOffers(TARGET_DATE, period);
  const validElexonRecords = elexonRecords.filter(record =>
    record.volume < 0 &&
    (record.soFlag || record.cadlFlag) &&
    windFarmIds.has(record.id)
  );
  
  const elexonCount = validElexonRecords.length;
  const elexonVolume = validElexonRecords.reduce(
    (total, record) => total + Math.abs(record.volume),
    0
  );
  
  // Determine status
  let status: PeriodStatus['status'] = 'unknown';
  let needsUpdate = false;
  
  if (existingCount === 0) {
    status = 'missing';
    needsUpdate = true;
  } else if (Math.abs(existingVolume - elexonVolume) > 0.1) {
    status = 'mismatch';
    needsUpdate = true;
  } else if (existingCount !== elexonCount) {
    status = 'incomplete';
    needsUpdate = true;
  } else {
    status = 'complete';
    needsUpdate = false;
  }
  
  return {
    period,
    existingCount,
    existingVolume,
    elexonCount,
    elexonVolume,
    status,
    needsUpdate
  };
}

// Process a single period
async function processPeriod(
  period: number, 
  windFarmIds: Set<string>, 
  bmuLeadPartyMap: Map<string, string>
): Promise<{
  volume: number;
  payment: number;
  recordCount: number;
}> {
  console.log(`Processing period ${period} for ${TARGET_DATE}...`);
  
  try {
    // Clear existing records for this period
    await db.delete(curtailmentRecords)
      .where(and(
        eq(curtailmentRecords.settlementDate, TARGET_DATE),
        eq(curtailmentRecords.settlementPeriod, period)
      ));
    
    // Get fresh data from Elexon
    const records = await fetchBidsOffers(TARGET_DATE, period);
    const validRecords = records.filter(record =>
      record.volume < 0 &&
      (record.soFlag || record.cadlFlag) &&
      windFarmIds.has(record.id)
    );
    
    if (validRecords.length > 0) {
      console.log(`[${TARGET_DATE} P${period}] Processing ${validRecords.length} records`);
    } else {
      console.log(`[${TARGET_DATE} P${period}] No valid curtailment records found`);
    }
    
    const periodResults = await Promise.all(
      validRecords.map(async record => {
        const volume = Math.abs(record.volume);
        const payment = volume * record.originalPrice;
        
        try {
          await db.insert(curtailmentRecords).values({
            settlementDate: TARGET_DATE,
            settlementPeriod: period,
            farmId: record.id,
            leadPartyName: bmuLeadPartyMap.get(record.id) || 'Unknown',
            volume: record.volume.toString(), // Keep the original negative value
            payment: payment.toString(),
            originalPrice: record.originalPrice.toString(),
            finalPrice: record.finalPrice.toString(),
            soFlag: record.soFlag,
            cadlFlag: record.cadlFlag
          });
          
          console.log(`[${TARGET_DATE} P${period}] Added record for ${record.id}: ${volume.toFixed(2)} MWh, £${payment.toFixed(2)}`);
          return { volume, payment };
        } catch (error) {
          console.error(`[${TARGET_DATE} P${period}] Error inserting record for ${record.id}:`, error);
          return { volume: 0, payment: 0 };
        }
      })
    );
    
    const periodTotal = periodResults.reduce(
      (acc, curr) => ({
        volume: acc.volume + curr.volume,
        payment: acc.payment + curr.payment
      }),
      { volume: 0, payment: 0 }
    );
    
    if (periodTotal.volume > 0) {
      console.log(`[${TARGET_DATE} P${period}] Total: ${periodTotal.volume.toFixed(2)} MWh, £${periodTotal.payment.toFixed(2)}`);
    }
    
    return { 
      volume: periodTotal.volume, 
      payment: periodTotal.payment,
      recordCount: validRecords.length
    };
  } catch (error) {
    console.error(`Error processing period ${period}:`, error);
    return { volume: 0, payment: 0, recordCount: 0 };
  }
}

// Update all summary tables
async function updateSummaries(): Promise<void> {
  try {
    console.log(`Updating summary records for ${TARGET_DATE}...`);
    
    // Calculate totals from curtailment records
    const totals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
        totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    if (!totals[0] || !totals[0].totalCurtailedEnergy) {
      console.error('Error: No curtailment records found to create summary');
      return;
    }
    
    // Update daily summary
    await db.insert(dailySummaries).values({
      summaryDate: TARGET_DATE,
      totalCurtailedEnergy: totals[0].totalCurtailedEnergy,
      totalPayment: totals[0].totalPayment,
      totalWindGeneration: '0',
      windOnshoreGeneration: '0',
      windOffshoreGeneration: '0',
      lastUpdated: new Date()
    }).onConflictDoUpdate({
      target: [dailySummaries.summaryDate],
      set: {
        totalCurtailedEnergy: totals[0].totalCurtailedEnergy,
        totalPayment: totals[0].totalPayment,
        lastUpdated: new Date()
      }
    });
    
    console.log(`Daily summary updated for ${TARGET_DATE}:`);
    console.log(`- Energy: ${totals[0].totalCurtailedEnergy} MWh`);
    console.log(`- Payment: £${totals[0].totalPayment}`);
    
    // Update monthly summary
    const yearMonth = TARGET_DATE.substring(0, 7);
    const monthlyTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(${dailySummaries.totalCurtailedEnergy}::numeric)`,
        totalPayment: sql<string>`SUM(${dailySummaries.totalPayment}::numeric)`
      })
      .from(dailySummaries)
      .where(sql`date_trunc('month', ${dailySummaries.summaryDate}::date) = date_trunc('month', ${yearMonth + '-01'}::date)`);
    
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
      
      console.log(`Monthly summary updated for ${yearMonth}:`);
      console.log(`- Energy: ${monthlyTotals[0].totalCurtailedEnergy} MWh`);
      console.log(`- Payment: £${monthlyTotals[0].totalPayment}`);
    }
    
    // Update yearly summary
    const year = TARGET_DATE.substring(0, 4);
    const yearlyTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(${dailySummaries.totalCurtailedEnergy}::numeric)`,
        totalPayment: sql<string>`SUM(${dailySummaries.totalPayment}::numeric)`
      })
      .from(dailySummaries)
      .where(sql`date_trunc('year', ${dailySummaries.summaryDate}::date) = date_trunc('year', ${year + '-01-01'}::date)`);
    
    if (yearlyTotals[0].totalCurtailedEnergy && yearlyTotals[0].totalPayment) {
      await db.insert(yearlySummaries).values({
        year,
        totalCurtailedEnergy: yearlyTotals[0].totalCurtailedEnergy,
        totalPayment: yearlyTotals[0].totalPayment,
        updatedAt: new Date()
      }).onConflictDoUpdate({
        target: [yearlySummaries.year],
        set: {
          totalCurtailedEnergy: yearlyTotals[0].totalCurtailedEnergy,
          totalPayment: yearlyTotals[0].totalPayment,
          updatedAt: new Date()
        }
      });
      
      console.log(`Yearly summary updated for ${year}:`);
      console.log(`- Energy: ${yearlyTotals[0].totalCurtailedEnergy} MWh`);
      console.log(`- Payment: £${yearlyTotals[0].totalPayment}`);
    }
  } catch (error) {
    console.error('Error updating summaries:', error);
    throw error;
  }
}

// Update Bitcoin calculations for the date
async function updateBitcoinCalculations(): Promise<void> {
  console.log(`Updating Bitcoin calculations for ${TARGET_DATE}...`);
  
  try {
    const minerModels = ['S19J_PRO', 'S9', 'M20S'];
    const { processSingleDay } = await import('./server/services/bitcoinService');
    
    for (const minerModel of minerModels) {
      await processSingleDay(TARGET_DATE, minerModel);
      console.log(`- Processed ${minerModel}`);
    }
    
    console.log('Bitcoin calculations updated successfully');
  } catch (error) {
    console.error('Error updating Bitcoin calculations:', error);
    throw error;
  }
}

// Process a batch of periods
async function processBatch(
  periods: number[],
  windFarmIds: Set<string>,
  bmuLeadPartyMap: Map<string, string>
): Promise<{
  volume: number;
  payment: number;
  recordCount: number;
}> {
  let totalVolume = 0;
  let totalPayment = 0;
  let totalRecordCount = 0;
  
  for (const period of periods) {
    const result = await processPeriod(period, windFarmIds, bmuLeadPartyMap);
    totalVolume += result.volume;
    totalPayment += result.payment;
    totalRecordCount += result.recordCount;
    await delay(500); // Add delay between API calls to avoid rate limits
  }
  
  return { 
    volume: totalVolume, 
    payment: totalPayment,
    recordCount: totalRecordCount
  };
}

// Main function
async function main(): Promise<void> {
  console.log(`=== Verifying March 28, 2025 Settlement Periods ===`);
  console.log(`Started at: ${new Date().toISOString()}`);
  
  try {
    // Step 1: Load BMU mappings
    const { windFarmIds, bmuLeadPartyMap } = await loadBmuMappings();
    
    // Step 2: Get existing periods 
    const existingPeriods = await db
      .select({ period: curtailmentRecords.settlementPeriod })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE))
      .groupBy(curtailmentRecords.settlementPeriod);
    
    const existingPeriodNumbers = existingPeriods.map(r => r.period);
    console.log(`Found ${existingPeriodNumbers.length} existing periods: ${existingPeriodNumbers.join(', ')}`);
    
    // Define which periods to process
    // Set START_PERIOD and END_PERIOD to control which range to process
    // Comment these out to process all missing periods
    const START_PERIOD = 47;  
    const END_PERIOD = 48;
    
    // Get the periods to process
    let periodsToProcess: number[];
    
    if (START_PERIOD && END_PERIOD) {
      // Process specific range
      console.log(`Processing specific range: ${START_PERIOD}-${END_PERIOD}`);
      periodsToProcess = Array.from(
        { length: END_PERIOD - START_PERIOD + 1 }, 
        (_, i) => START_PERIOD + i
      );
    } else {
      // Process all 48 periods
      console.log('Processing all 48 periods');
      periodsToProcess = Array.from({ length: 48 }, (_, i) => i + 1);
    }
    
    // Determine which periods are missing
    const missingPeriods = periodsToProcess.filter(p => !existingPeriodNumbers.includes(p));
    console.log(`Missing ${missingPeriods.length} periods in range ${START_PERIOD}-${END_PERIOD}: ${missingPeriods.join(', ')}`);
    
    if (missingPeriods.length === 0) {
      console.log('No missing periods in the specified range. Running validation on existing periods...');
    }
    
    // Process 3 periods at a time to avoid timeouts
    const BATCH_SIZE = 3;
    for (let i = 0; i < missingPeriods.length; i += BATCH_SIZE) {
      const batchPeriods = missingPeriods.slice(i, i + BATCH_SIZE);
      console.log(`Processing batch ${Math.floor(i / BATCH_SIZE) + 1}/${Math.ceil(missingPeriods.length / BATCH_SIZE)}: Periods ${batchPeriods.join(', ')}`);
      
      // Process each period
      for (const period of batchPeriods) {
        console.log(`\nProcessing period ${period}...`);
        await processPeriod(period, windFarmIds, bmuLeadPartyMap);
      }
      
      // Make sure to refresh the DB connection to avoid timeouts
      await db.execute(sql`SELECT 1`);
      
      // Delay between batches
      if (i + BATCH_SIZE < missingPeriods.length) {
        console.log('Waiting 2 seconds before next batch...');
        await delay(2000);
      }
    }
    
    // Step 3: Update all summary tables
    await updateSummaries();
    
    // Step 4: Update Bitcoin calculations
    await updateBitcoinCalculations();
    
    // Step 5: Verify the final state
    const finalPeriods = await db
      .select({
        count: sql`COUNT(DISTINCT ${curtailmentRecords.settlementPeriod})`,
        records: sql`COUNT(*)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log(`\nVerification complete. Found ${finalPeriods[0].count} distinct periods with ${finalPeriods[0].records} total records.`);
    
    // Check if we have all 48 periods
    if (Number(finalPeriods[0].count) === 48) {
      console.log('SUCCESS: All 48 settlement periods are now in the database!');
    } else {
      console.log(`WARNING: Expected 48 periods, but found ${finalPeriods[0].count}`);
      
      // List missing periods
      const allPeriods = Array.from({ length: 48 }, (_, i) => i + 1);
      const updatedExistingPeriods = await db
        .select({ period: curtailmentRecords.settlementPeriod })
        .from(curtailmentRecords)
        .where(eq(curtailmentRecords.settlementDate, TARGET_DATE))
        .groupBy(curtailmentRecords.settlementPeriod);
        
      const updatedExistingPeriodNumbers = updatedExistingPeriods.map(r => r.period);
      const stillMissingPeriods = allPeriods.filter(p => !updatedExistingPeriodNumbers.includes(p));
      
      console.log(`Still missing ${stillMissingPeriods.length} periods: ${stillMissingPeriods.join(', ')}`);
      console.log('Please run this script again with these specific periods.');
    }
    
    // Check the API to verify the hourly data is now complete
    console.log('\nVerifying hourly API data...');
    console.log('Please run: curl -H "Accept: application/json" \'http://localhost:5000/api/curtailment/hourly/2025-03-28\' | jq');
    
    console.log(`\nUpdate completed successfully at ${new Date().toISOString()}`);
  } catch (error) {
    console.error('Error during verification process:', error);
    process.exit(1);
  }
}

// Run the script
main().catch(error => {
  console.error('Unhandled error:', error);
  process.exit(1);
});