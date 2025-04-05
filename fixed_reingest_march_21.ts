/**
 * Complete Reingest for March 21, 2025
 * 
 * This script completely removes all settlement period data for March 21, 2025
 * and then reingests all 48 settlement periods from the Elexon API.
 * 
 * The goal is to ensure the total payment matches the expected amount of £1,240,439.58.
 */

import { db } from "./db";
import { curtailmentRecords, dailySummaries, monthlySummaries, yearlySummaries } from "./db/schema";
import { eq, and, sql } from "drizzle-orm";
import axios from "axios";
import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const TARGET_DATE = '2025-03-21';
const EXPECTED_TOTAL_PAYMENT = 1240439.58; // Expected total in GBP
const EXPECTED_TOTAL_ENERGY = 50518.72; // Expected total in MWh
const BMU_MAPPING_PATH = path.join(__dirname, "server/data/bmuMapping.json");
const ELEXON_API_URL = "https://api.bmreports.com/BMRS/BOALF/V1";
const LOG_DIR = path.join(__dirname, "logs");
const LOG_FILE_PATH = path.join(LOG_DIR, `reingest_${TARGET_DATE}_${Date.now()}.log`);

// Ensure log directory exists
async function setupLogging() {
  try {
    await fs.mkdir(LOG_DIR, { recursive: true });
  } catch (error) {
    console.error("Error creating log directory:", error);
  }
}

// Function to log to both console and file
async function log(message: string) {
  console.log(message);
  try {
    await fs.appendFile(LOG_FILE_PATH, message + "\n");
  } catch (error) {
    console.error("Error writing to log file:", error);
  }
}

// Utility function to delay between API calls
async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Load BMU mapping to get valid wind farm IDs
async function loadBmuMappings(): Promise<{
  bmuIds: Set<string>;
  bmuLeadPartyMap: Map<string, string>;
}> {
  try {
    await log('Loading BMU mapping from: ' + BMU_MAPPING_PATH);
    const mappingContent = await fs.readFile(BMU_MAPPING_PATH, 'utf8');
    const bmuMapping = JSON.parse(mappingContent);
    
    const bmuIds = new Set<string>();
    const bmuLeadPartyMap = new Map<string, string>();
    
    for (const mapping of bmuMapping) {
      if (mapping.nationalGridBmUnit) {
        bmuIds.add(mapping.nationalGridBmUnit);
        bmuLeadPartyMap.set(mapping.nationalGridBmUnit, mapping.leadPartyName || 'Unknown');
      }
      if (mapping.elexonBmUnit) {
        bmuIds.add(mapping.elexonBmUnit);
        bmuLeadPartyMap.set(mapping.elexonBmUnit, mapping.leadPartyName || 'Unknown');
      }
    }
    
    await log(`Loaded ${bmuIds.size} wind farm BMU IDs`);
    return { bmuIds, bmuLeadPartyMap };
  } catch (error) {
    console.error('Error loading BMU mapping:', error);
    throw error;
  }
}

// First clear the existing data for the target date
async function clearExistingData(): Promise<void> {
  await log(`Clearing all existing data for ${TARGET_DATE}...`);
  
  // First, delete from curtailment_records
  const deleteResult = await db.delete(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
  
  await log(`Deleted ${deleteResult.rowCount} curtailment records for ${TARGET_DATE}`);
  
  // Then delete from daily_summaries
  const summaryDeleteResult = await db.delete(dailySummaries)
    .where(eq(dailySummaries.summaryDate, TARGET_DATE));
  
  await log(`Deleted ${summaryDeleteResult.rowCount} daily summary records for ${TARGET_DATE}`);
  
  return;
}

// Process a single settlement period
async function processPeriod(
  period: number, 
  bmuIds: Set<string>, 
  bmuLeadPartyMap: Map<string, string>
): Promise<{
  volume: number;
  payment: number;
  recordCount: number;
}> {
  await log(`Processing period ${period} for ${TARGET_DATE}...`);
  
  try {
    // Format the date as required by Elexon API (DD-MM-YYYY)
    const formattedDate = TARGET_DATE.split('-').reverse().join('-');
    
    // Fetch data from Elexon API
    const response = await axios.get(ELEXON_API_URL, {
      params: {
        APIKey: "l2dmkqoqrk0vwky",
        ServiceType: "xml",
        Period: period,
        SettlementDate: formattedDate
      }
    });
    
    const xmlData = response.data;
    
    // Extract the relevant data points using regex
    const bmuRecords = xmlData.match(/<BMUData>([\s\S]*?)<\/BMUData>/g) || [];
    
    let recordCount = 0;
    let totalVolume = 0;
    let totalPayment = 0;
    
    for (const bmuRecord of bmuRecords) {
      const bmuId = bmuRecord.match(/<NGC_BMU_ID>(.*?)<\/NGC_BMU_ID>/)?.[1];
      
      // Only process wind farms in our mapping
      if (!bmuId || !bmuIds.has(bmuId)) continue;
      
      const originalLevel = parseFloat(bmuRecord.match(/<OriginalLevel>(.*?)<\/OriginalLevel>/)?.[1] || "0");
      const finalLevel = parseFloat(bmuRecord.match(/<FinalLevel>(.*?)<\/FinalLevel>/)?.[1] || "0");
      const cadlFlag = bmuRecord.match(/<CADL_Flag>(.*?)<\/CADL_Flag>/)?.[1] === "Y";
      const soFlag = bmuRecord.match(/<SO_Flag>(.*?)<\/SO_Flag>/)?.[1] === "Y";
      
      // Calculate volume (negative means curtailment)
      const volume = originalLevel - finalLevel;
      
      // Only process records with curtailment
      if (volume <= 0) continue;
      
      // Get the original and final prices
      const originalPrice = parseFloat(bmuRecord.match(/<OriginalPrice>(.*?)<\/OriginalPrice>/)?.[1] || "0");
      const finalPrice = parseFloat(bmuRecord.match(/<FinalPrice>(.*?)<\/FinalPrice>/)?.[1] || "0");
      
      // Calculate payment (note: using originalPrice directly)
      const payment = volume * originalPrice;
      
      // Get farm ID from BMU ID (use first part of BMU ID)
      const farmId = bmuId;
      
      // Get lead party name from our mapping
      const leadPartyName = bmuLeadPartyMap.get(bmuId) || "Unknown";
      
      try {
        await db.insert(curtailmentRecords).values({
          settlementDate: TARGET_DATE,
          settlementPeriod: period,
          farmId,
          leadPartyName,
          volume: volume * -1, // Store as negative value for curtailment
          payment,
          originalPrice,
          finalPrice,
          soFlag,
          cadlFlag
        });
        
        // Add to totals (using absolute value for volume)
        totalVolume += volume;
        totalPayment += payment;
        recordCount++;
        
        await log(`[${TARGET_DATE} P${period}] Added record for ${farmId}: ${volume.toFixed(2)} MWh, £${payment.toFixed(2)}`);
      } catch (error) {
        console.error(`Error inserting record for ${bmuId}:`, error);
      }
    }
    
    await log(`[${TARGET_DATE} P${period}] Total: ${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)}`);
    
    return { 
      volume: totalVolume, 
      payment: totalPayment,
      recordCount
    };
  } catch (error) {
    console.error(`Error processing period ${period}:`, error);
    await log(`ERROR processing period ${period}: ${(error as Error).message}`);
    return { volume: 0, payment: 0, recordCount: 0 };
  }
}

// Process a batch of periods
async function processBatch(
  periods: number[],
  bmuIds: Set<string>,
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
    const result = await processPeriod(period, bmuIds, bmuLeadPartyMap);
    totalVolume += result.volume;
    totalPayment += result.payment;
    totalRecordCount += result.recordCount;
    
    // Add delay between periods to avoid API rate limits
    await delay(500); 
  }
  
  return { 
    volume: totalVolume, 
    payment: totalPayment,
    recordCount: totalRecordCount
  };
}

// Update all summary tables
async function updateSummaries(): Promise<void> {
  try {
    await log(`Updating summary records for ${TARGET_DATE}...`);
    
    // Calculate totals from curtailment records
    const totals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
        totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    if (!totals[0] || !totals[0].totalCurtailedEnergy) {
      await log('Error: No curtailment records found to create summary');
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
    
    await log(`Daily summary updated for ${TARGET_DATE}:`);
    await log(`- Energy: ${totals[0].totalCurtailedEnergy} MWh`);
    await log(`- Payment: £${totals[0].totalPayment}`);
    
    // If the payment amount doesn't match expected total, log a warning
    const paymentTotal = parseFloat(totals[0].totalPayment);
    const energyTotal = parseFloat(totals[0].totalCurtailedEnergy);
    
    if (Math.abs(paymentTotal - EXPECTED_TOTAL_PAYMENT) > 100) {
      await log(`WARNING: Total payment £${paymentTotal.toFixed(2)} differs from expected £${EXPECTED_TOTAL_PAYMENT.toFixed(2)}`);
      await log(`Difference: £${Math.abs(paymentTotal - EXPECTED_TOTAL_PAYMENT).toFixed(2)}`);
    } else {
      await log(`SUCCESS: Payment total £${paymentTotal.toFixed(2)} matches expected total (within £100 margin)`);
    }
    
    if (Math.abs(energyTotal - EXPECTED_TOTAL_ENERGY) > 100) {
      await log(`WARNING: Total energy ${energyTotal.toFixed(2)} MWh differs from expected ${EXPECTED_TOTAL_ENERGY.toFixed(2)} MWh`);
      await log(`Difference: ${Math.abs(energyTotal - EXPECTED_TOTAL_ENERGY).toFixed(2)} MWh`);
    } else {
      await log(`SUCCESS: Energy total ${energyTotal.toFixed(2)} MWh matches expected total (within 100 MWh margin)`);
    }
    
    // Update monthly summary
    const yearMonth = TARGET_DATE.substring(0, 7);
    const monthlyTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(${dailySummaries.totalCurtailedEnergy}::numeric)`,
        totalPayment: sql<string>`SUM(${dailySummaries.totalPayment}::numeric)`,
        totalWindGeneration: sql<string>`SUM(${dailySummaries.totalWindGeneration}::numeric)`,
        windOnshoreGeneration: sql<string>`SUM(${dailySummaries.windOnshoreGeneration}::numeric)`,
        windOffshoreGeneration: sql<string>`SUM(${dailySummaries.windOffshoreGeneration}::numeric)`
      })
      .from(dailySummaries)
      .where(sql`date_trunc('month', ${dailySummaries.summaryDate}::date) = date_trunc('month', ${yearMonth + '-01'}::date)`);
    
    if (monthlyTotals[0].totalCurtailedEnergy && monthlyTotals[0].totalPayment) {
      await db.insert(monthlySummaries).values({
        yearMonth,
        totalCurtailedEnergy: monthlyTotals[0].totalCurtailedEnergy,
        totalPayment: monthlyTotals[0].totalPayment,
        totalWindGeneration: monthlyTotals[0].totalWindGeneration || '0',
        windOnshoreGeneration: monthlyTotals[0].windOnshoreGeneration || '0',
        windOffshoreGeneration: monthlyTotals[0].windOffshoreGeneration || '0',
        createdAt: new Date(),
        updatedAt: new Date(),
        lastUpdated: new Date()
      }).onConflictDoUpdate({
        target: [monthlySummaries.yearMonth],
        set: {
          totalCurtailedEnergy: monthlyTotals[0].totalCurtailedEnergy,
          totalPayment: monthlyTotals[0].totalPayment,
          totalWindGeneration: monthlyTotals[0].totalWindGeneration || '0',
          windOnshoreGeneration: monthlyTotals[0].windOnshoreGeneration || '0',
          windOffshoreGeneration: monthlyTotals[0].windOffshoreGeneration || '0',
          updatedAt: new Date(),
          lastUpdated: new Date()
        }
      });
      
      await log(`Monthly summary updated for ${yearMonth}:`);
      await log(`- Energy: ${monthlyTotals[0].totalCurtailedEnergy} MWh`);
      await log(`- Payment: £${monthlyTotals[0].totalPayment}`);
    }
    
    // Update yearly summary
    const year = TARGET_DATE.substring(0, 4);
    const yearlyTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(${dailySummaries.totalCurtailedEnergy}::numeric)`,
        totalPayment: sql<string>`SUM(${dailySummaries.totalPayment}::numeric)`,
        totalWindGeneration: sql<string>`SUM(${dailySummaries.totalWindGeneration}::numeric)`,
        windOnshoreGeneration: sql<string>`SUM(${dailySummaries.windOnshoreGeneration}::numeric)`,
        windOffshoreGeneration: sql<string>`SUM(${dailySummaries.windOffshoreGeneration}::numeric)`
      })
      .from(dailySummaries)
      .where(sql`date_trunc('year', ${dailySummaries.summaryDate}::date) = date_trunc('year', ${year + '-01-01'}::date)`);
    
    if (yearlyTotals[0].totalCurtailedEnergy && yearlyTotals[0].totalPayment) {
      await db.insert(yearlySummaries).values({
        year,
        totalCurtailedEnergy: yearlyTotals[0].totalCurtailedEnergy,
        totalPayment: yearlyTotals[0].totalPayment,
        totalWindGeneration: yearlyTotals[0].totalWindGeneration || '0',
        windOnshoreGeneration: yearlyTotals[0].windOnshoreGeneration || '0',
        windOffshoreGeneration: yearlyTotals[0].windOffshoreGeneration || '0',
        createdAt: new Date(),
        updatedAt: new Date(),
        lastUpdated: new Date()
      }).onConflictDoUpdate({
        target: [yearlySummaries.year],
        set: {
          totalCurtailedEnergy: yearlyTotals[0].totalCurtailedEnergy,
          totalPayment: yearlyTotals[0].totalPayment,
          totalWindGeneration: yearlyTotals[0].totalWindGeneration || '0',
          windOnshoreGeneration: yearlyTotals[0].windOnshoreGeneration || '0',
          windOffshoreGeneration: yearlyTotals[0].windOffshoreGeneration || '0',
          updatedAt: new Date(),
          lastUpdated: new Date()
        }
      });
      
      await log(`Yearly summary updated for ${year}:`);
      await log(`- Energy: ${yearlyTotals[0].totalCurtailedEnergy} MWh`);
      await log(`- Payment: £${yearlyTotals[0].totalPayment}`);
    }
  } catch (error) {
    console.error('Error updating summaries:', error);
    await log(`ERROR updating summaries: ${(error as Error).message}`);
    throw error;
  }
}

// Update Bitcoin calculations
async function updateBitcoinCalculations(): Promise<void> {
  await log(`Updating Bitcoin calculations for ${TARGET_DATE}...`);
  
  try {
    const minerModels = ['S19J_PRO', 'S9', 'M20S'];
    const { processSingleDay } = await import('./server/services/bitcoinService');
    
    for (const minerModel of minerModels) {
      await processSingleDay(TARGET_DATE, minerModel);
      await log(`- Processed ${minerModel}`);
    }
    
    await log('Bitcoin calculations updated successfully');
  } catch (error) {
    console.error('Error updating Bitcoin calculations:', error);
    await log(`ERROR updating Bitcoin calculations: ${(error as Error).message}`);
  }
}

// Main function
async function main(): Promise<void> {
  const startTime = Date.now();
  
  await setupLogging();
  
  await log(`=== Complete Reingest for March 21, 2025 ===`);
  await log(`Started at: ${new Date().toISOString()}`);
  await log(`Expected total payment: £${EXPECTED_TOTAL_PAYMENT.toFixed(2)}`);
  await log(`Expected total energy: ${EXPECTED_TOTAL_ENERGY.toFixed(2)} MWh`);
  await log(`Target date: ${TARGET_DATE}`);
  
  try {
    // Step 1: Load BMU mappings
    const { bmuIds, bmuLeadPartyMap } = await loadBmuMappings();
    
    // Step 2: Clear existing data
    await clearExistingData();
    
    // Step 3: Process all 48 periods
    await log('Processing all 48 settlement periods...');
    const allPeriods = Array.from({ length: 48 }, (_, i) => i + 1);
    
    // Group periods into batches to avoid timeouts
    const BATCH_SIZE = 4;
    const batches = [];
    for (let i = 0; i < allPeriods.length; i += BATCH_SIZE) {
      batches.push(allPeriods.slice(i, i + BATCH_SIZE));
    }
    
    await log(`Split into ${batches.length} batches of ${BATCH_SIZE} periods each`);
    
    let totalVolume = 0;
    let totalPayment = 0;
    let totalRecords = 0;
    
    for (let i = 0; i < batches.length; i++) {
      const batch = batches[i];
      await log(`Processing batch ${i+1}/${batches.length}: Periods ${batch.join(', ')}`);
      
      const batchResult = await processBatch(batch, bmuIds, bmuLeadPartyMap);
      totalVolume += batchResult.volume;
      totalPayment += batchResult.payment;
      totalRecords += batchResult.recordCount;
      
      await log(`Batch ${i+1} completed: ${batchResult.recordCount} records, ${batchResult.volume.toFixed(2)} MWh, £${batchResult.payment.toFixed(2)}`);
      await log(`Running total: ${totalRecords} records, ${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)}`);
      
      // Make sure to refresh the DB connection to avoid timeouts
      await db.execute(sql`SELECT 1`);
      
      // Delay between batches
      if (i < batches.length - 1) {
        const delayTime = 1000;
        await log(`Waiting ${delayTime/1000} second before next batch...`);
        await delay(delayTime);
      }
    }
    
    // Step 4: Update all summary tables
    await updateSummaries();
    
    // Step 5: Update Bitcoin calculations
    await updateBitcoinCalculations();
    
    // Step 6: Verify the final state
    const finalStatus = await db
      .select({
        periodCount: sql`COUNT(DISTINCT ${curtailmentRecords.settlementPeriod})`,
        recordCount: sql`COUNT(*)`,
        totalVolume: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
        totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    await log(`\nFinal Status for ${TARGET_DATE}:`);
    await log(`- Settlement Periods: ${finalStatus[0].periodCount}/48`);
    await log(`- Records: ${finalStatus[0].recordCount}`);
    await log(`- Total Volume: ${finalStatus[0].totalVolume} MWh`);
    await log(`- Total Payment: £${finalStatus[0].totalPayment}`);
    
    // Check if we have all 48 periods
    if (Number(finalStatus[0].periodCount) === 48) {
      await log('SUCCESS: All 48 settlement periods are now in the database!');
    } else {
      await log(`WARNING: Expected 48 periods, but found ${finalStatus[0].periodCount}`);
      
      // List the missing periods
      const existingPeriods = await db
        .select({ period: curtailmentRecords.settlementPeriod })
        .from(curtailmentRecords)
        .where(eq(curtailmentRecords.settlementDate, TARGET_DATE))
        .groupBy(curtailmentRecords.settlementPeriod);
        
      const existingPeriodNumbers = existingPeriods.map(r => r.period);
      const missingPeriods = allPeriods.filter(p => !existingPeriodNumbers.includes(p));
      
      await log(`Missing ${missingPeriods.length} periods: ${missingPeriods.join(', ')}`);
    }
    
    const paymentTotal = parseFloat(finalStatus[0].totalPayment);
    const energyTotal = parseFloat(finalStatus[0].totalVolume);
    
    if (Math.abs(paymentTotal - EXPECTED_TOTAL_PAYMENT) > 100) {
      await log(`WARNING: Final payment total £${paymentTotal.toFixed(2)} differs from expected £${EXPECTED_TOTAL_PAYMENT.toFixed(2)}`);
      await log(`Difference: £${Math.abs(paymentTotal - EXPECTED_TOTAL_PAYMENT).toFixed(2)}`);
    } else {
      await log(`SUCCESS: Final payment total £${paymentTotal.toFixed(2)} matches expected total (within £100 margin)`);
    }
    
    if (Math.abs(energyTotal - EXPECTED_TOTAL_ENERGY) > 100) {
      await log(`WARNING: Final energy total ${energyTotal.toFixed(2)} MWh differs from expected ${EXPECTED_TOTAL_ENERGY.toFixed(2)} MWh`);
      await log(`Difference: ${Math.abs(energyTotal - EXPECTED_TOTAL_ENERGY).toFixed(2)} MWh`);
    } else {
      await log(`SUCCESS: Final energy total ${energyTotal.toFixed(2)} MWh matches expected total (within 100 MWh margin)`);
    }
    
    const duration = ((Date.now() - startTime) / 1000).toFixed(1);
    await log(`\nReingestion completed in ${duration} seconds`);
    
  } catch (error) {
    console.error('Error in main process:', error);
    await log(`FATAL ERROR: ${(error as Error).message}`);
    process.exit(1);
  }
}

// Run the script
main().catch(error => {
  console.error("Uncaught error:", error);
  process.exit(1);
});