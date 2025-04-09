/**
 * Complete Data Reingestion Script
 * 
 * This script performs a full reingestion of all curtailment data:
 * 1. Reingests curtailment records from Elexon API for all dates in the database
 * 2. Updates all dependent tables (daily, monthly, yearly summaries)
 * 3. Recalculates all Bitcoin mining potential data
 * 4. Provides detailed logging on the process and results
 */

import { db } from "@db";
import { 
  curtailmentRecords, 
  dailySummaries, 
  monthlySummaries, 
  yearlySummaries, 
  historicalBitcoinCalculations,
  bitcoinDailySummaries,
  bitcoinMonthlySummaries,
  bitcoinYearlySummaries
} from "@db/schema";
import { eq, desc, and, sql } from "drizzle-orm";
import { fetchBidsOffers } from "../services/elexon";
import path from "path";
import fs from "fs/promises";
import { fileURLToPath } from 'url';
import { processSingleDay } from "../services/bitcoinService";
import { format, parse, isValid } from 'date-fns';

// Constants
const BATCH_SIZE = 12; // Process 12 periods at a time
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S']; // All miner models to process

// Path setup for BMU mapping
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const BMU_MAPPING_PATH = path.join(__dirname, "../../data/bmu_mapping.json");

// Progress tracking
interface ProgressData {
  totalDates: number;
  processedDates: number;
  currentDate: string;
  totalRecords: number;
  startTime: Date;
}

// In-memory caches
let windFarmBmuIds: Set<string> | null = null;
let bmuLeadPartyMap: Map<string, string> | null = null;

/**
 * Load wind farm BMU IDs and lead party names from the mapping file
 */
async function loadWindFarmIds(): Promise<Set<string>> {
  try {
    if (windFarmBmuIds === null || bmuLeadPartyMap === null) {
      console.log('Loading BMU mapping from:', BMU_MAPPING_PATH);
      const mappingContent = await fs.readFile(BMU_MAPPING_PATH, 'utf8');
      const bmuMapping = JSON.parse(mappingContent);
      console.log(`Loaded ${bmuMapping.length} BMU mappings`);

      windFarmBmuIds = new Set(
        bmuMapping
          .filter((bmu: any) => bmu.fuelType === "WIND")
          .map((bmu: any) => bmu.elexonBmUnit)
      );

      bmuLeadPartyMap = new Map(
        bmuMapping
          .filter((bmu: any) => bmu.fuelType === "WIND")
          .map((bmu: any) => [bmu.elexonBmUnit, bmu.leadPartyName || 'Unknown'])
      );

      console.log(`Found ${windFarmBmuIds.size} wind farm BMUs`);
    }

    if (!windFarmBmuIds || !bmuLeadPartyMap) {
      throw new Error('Failed to initialize BMU mappings');
    }

    return windFarmBmuIds;
  } catch (error) {
    console.error('Error loading BMU mapping:', error);
    throw error;
  }
}

/**
 * Get all dates that have curtailment data in the database
 */
async function getAllDates(): Promise<string[]> {
  try {
    const result = await db
      .select({
        dates: sql<string>`DISTINCT settlement_date`
      })
      .from(curtailmentRecords)
      .orderBy(desc(sql<string>`settlement_date`));
    
    const dates = result.map(r => r.dates);
    console.log(`Found ${dates.length} unique dates with existing curtailment data`);
    return dates;
  } catch (error) {
    console.error('Error fetching dates:', error);
    return [];
  }
}

/**
 * Reingest curtailment records for a specific date
 */
async function reingestDateCurtailmentRecords(
  date: string, 
  validWindFarmIds: Set<string>
): Promise<{ records: number; volume: number; payment: number }> {
  console.log(`\n=== Starting reingestion for ${date} ===`);
  
  let totalVolume = 0;
  let totalPayment = 0;
  let recordsProcessed = 0;

  // Step 1: Clear existing records for this date
  console.log(`Clearing existing records for ${date}...`);
  await db.delete(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date));
  console.log(`Cleared existing records for ${date}`);

  // Step 2: Process all 48 periods in batches
  for (let startPeriod = 1; startPeriod <= 48; startPeriod += BATCH_SIZE) {
    const endPeriod = Math.min(startPeriod + BATCH_SIZE - 1, 48);
    const periodPromises = [];

    console.log(`Processing periods ${startPeriod} to ${endPeriod} for ${date}...`);

    for (let period = startPeriod; period <= endPeriod; period++) {
      periodPromises.push((async () => {
        try {
          // Fetch data from Elexon API
          const records = await fetchBidsOffers(date, period);
          
          // Filter for valid wind farm curtailment records
          const validRecords = records.filter(record =>
            record.volume < 0 && 
            (record.soFlag || record.cadlFlag) &&
            validWindFarmIds.has(record.id)
          );

          if (validRecords.length > 0) {
            console.log(`[${date} P${period}] Processing ${validRecords.length} records`);
          }

          // Insert each valid record into the database
          const periodResults = await Promise.all(
            validRecords.map(async record => {
              const volume = Math.abs(record.volume);
              const payment = volume * record.originalPrice;

              try {
                await db.insert(curtailmentRecords).values({
                  settlementDate: date,
                  settlementPeriod: period,
                  farmId: record.id,
                  leadPartyName: bmuLeadPartyMap?.get(record.id) || 'Unknown',
                  volume: record.volume.toString(), // Keep the original negative value
                  payment: payment.toString(),
                  originalPrice: record.originalPrice.toString(),
                  finalPrice: record.finalPrice.toString(),
                  soFlag: record.soFlag,
                  cadlFlag: record.cadlFlag
                });

                recordsProcessed++;
                return { volume, payment };
              } catch (error) {
                console.error(`[${date} P${period}] Error inserting record for ${record.id}:`, error);
                return { volume: 0, payment: 0 };
              }
            })
          );

          // Calculate totals for this period
          const periodTotal = periodResults.reduce(
            (acc, curr) => ({
              volume: acc.volume + curr.volume,
              payment: acc.payment + curr.payment
            }),
            { volume: 0, payment: 0 }
          );

          if (periodTotal.volume > 0) {
            console.log(`[${date} P${period}] Total: ${periodTotal.volume.toFixed(2)} MWh, £${periodTotal.payment.toFixed(2)}`);
          }

          totalVolume += periodTotal.volume;
          totalPayment += periodTotal.payment;
          
          return periodTotal;
        } catch (error) {
          console.error(`Error processing period ${period} for date ${date}:`, error);
          return { volume: 0, payment: 0 };
        }
      })());
    }

    // Wait for all period promises to complete
    await Promise.all(periodPromises);
  }

  console.log(`=== Reingestion Summary for ${date} ===`);
  console.log(`Records processed: ${recordsProcessed}`);
  console.log(`Total volume: ${totalVolume.toFixed(2)} MWh`);
  console.log(`Total payment: £${totalPayment.toFixed(2)}`);

  return { records: recordsProcessed, volume: totalVolume, payment: totalPayment };
}

/**
 * Update daily summary for a specific date
 */
async function updateDailySummary(date: string, volume: number, payment: number): Promise<void> {
  try {
    console.log(`Updating daily summary for ${date}...`);
    
    await db.insert(dailySummaries).values({
      summaryDate: date,
      totalCurtailedEnergy: volume.toString(),
      totalPayment: payment.toString()
    }).onConflictDoUpdate({
      target: [dailySummaries.summaryDate],
      set: {
        totalCurtailedEnergy: volume.toString(),
        totalPayment: payment.toString()
      }
    });
    
    console.log(`Successfully updated daily summary for ${date}`);
  } catch (error) {
    console.error(`Error updating daily summary for ${date}:`, error);
    throw error;
  }
}

/**
 * Update monthly summary for a specific date's month
 */
async function updateMonthlySummary(date: string): Promise<void> {
  try {
    const yearMonth = date.substring(0, 7); // YYYY-MM
    console.log(`Updating monthly summary for ${yearMonth}...`);
    
    // Calculate monthly totals from daily summaries
    const monthlyTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(${dailySummaries.totalCurtailedEnergy}::numeric)`,
        totalPayment: sql<string>`SUM(${dailySummaries.totalPayment}::numeric)`
      })
      .from(dailySummaries)
      .where(sql`date_trunc('month', ${dailySummaries.summaryDate}::date) = date_trunc('month', ${date}::date)`);

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
      
      console.log(`Successfully updated monthly summary for ${yearMonth}`);
    } else {
      console.warn(`No data found for monthly summary ${yearMonth}`);
    }
  } catch (error) {
    console.error(`Error updating monthly summary for ${date}:`, error);
    throw error;
  }
}

/**
 * Update yearly summary for a specific date's year
 */
async function updateYearlySummary(date: string): Promise<void> {
  try {
    const year = date.substring(0, 4); // YYYY
    console.log(`Updating yearly summary for ${year}...`);
    
    // Calculate yearly totals from daily summaries
    const yearlyTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(${dailySummaries.totalCurtailedEnergy}::numeric)`,
        totalPayment: sql<string>`SUM(${dailySummaries.totalPayment}::numeric)`
      })
      .from(dailySummaries)
      .where(sql`date_trunc('year', ${dailySummaries.summaryDate}::date) = date_trunc('year', ${date}::date)`);

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
      
      console.log(`Successfully updated yearly summary for ${year}`);
    } else {
      console.warn(`No data found for yearly summary ${year}`);
    }
  } catch (error) {
    console.error(`Error updating yearly summary for ${date}:`, error);
    throw error;
  }
}

/**
 * Process Bitcoin calculations for a specific date
 */
async function processDateBitcoinCalculations(date: string): Promise<void> {
  try {
    console.log(`Processing Bitcoin calculations for ${date}...`);
    
    // First, clean up any existing Bitcoin calculations
    await db.delete(historicalBitcoinCalculations)
      .where(eq(historicalBitcoinCalculations.settlementDate, date));
    
    console.log(`Cleared existing Bitcoin calculations for ${date}`);
    
    // Process each miner model
    for (const minerModel of MINER_MODELS) {
      console.log(`Processing ${minerModel} model for ${date}...`);
      await processSingleDay(date, minerModel);
    }
    
    console.log(`Successfully processed Bitcoin calculations for ${date}`);
  } catch (error) {
    console.error(`Error processing Bitcoin calculations for ${date}:`, error);
    throw error;
  }
}

/**
 * Print progress information
 */
function printProgress(progress: ProgressData): void {
  const elapsedSeconds = (new Date().getTime() - progress.startTime.getTime()) / 1000;
  const percentage = (progress.processedDates / progress.totalDates) * 100;
  const remainingDates = progress.totalDates - progress.processedDates;
  
  let estimatedTimeRemaining = 'Calculating...';
  if (progress.processedDates > 0) {
    const secondsPerDate = elapsedSeconds / progress.processedDates;
    const remainingSeconds = remainingDates * secondsPerDate;
    const remainingMinutes = Math.floor(remainingSeconds / 60);
    const remainingHours = Math.floor(remainingMinutes / 60);
    
    estimatedTimeRemaining = remainingHours > 0 
      ? `~${remainingHours}h ${remainingMinutes % 60}m` 
      : `~${remainingMinutes}m ${Math.floor(remainingSeconds % 60)}s`;
  }
  
  console.log('\n======== REINGESTION PROGRESS ========');
  console.log(`Current date: ${progress.currentDate}`);
  console.log(`Processed: ${progress.processedDates}/${progress.totalDates} dates (${percentage.toFixed(1)}%)`);
  console.log(`Total records: ${progress.totalRecords}`);
  console.log(`Elapsed time: ${(elapsedSeconds / 60).toFixed(1)} minutes`);
  console.log(`Estimated time remaining: ${estimatedTimeRemaining}`);
  console.log('======================================\n');
}

/**
 * Main reingestion function
 */
export async function reingestAllCurtailmentData(
  startDate?: string,
  endDate?: string
): Promise<void> {
  const startTime = new Date();
  console.log(`\n===== Starting complete curtailment data reingestion at ${startTime.toISOString()} =====`);
  
  try {
    // Initialize wind farm IDs
    const validWindFarmIds = await loadWindFarmIds();
    
    // Get all dates to process
    let dates = await getAllDates();
    
    // Apply date filters if provided
    if (startDate && isValid(parse(startDate, 'yyyy-MM-dd', new Date()))) {
      dates = dates.filter(date => date >= startDate);
      console.log(`Filtered to dates from ${startDate} onwards: ${dates.length} dates`);
    }
    
    if (endDate && isValid(parse(endDate, 'yyyy-MM-dd', new Date()))) {
      dates = dates.filter(date => date <= endDate);
      console.log(`Filtered to dates until ${endDate}: ${dates.length} dates`);
    }
    
    if (dates.length === 0) {
      console.log('No dates to process. Exiting.');
      return;
    }
    
    const progress: ProgressData = {
      totalDates: dates.length,
      processedDates: 0,
      currentDate: '',
      totalRecords: 0,
      startTime
    };
    
    // Process each date
    for (const date of dates) {
      progress.currentDate = date;
      printProgress(progress);
      
      try {
        // Step 1: Reingest curtailment records
        const result = await reingestDateCurtailmentRecords(date, validWindFarmIds);
        progress.totalRecords += result.records;
        
        // Step 2: Update daily summary
        await updateDailySummary(date, result.volume, result.payment);
        
        // Step 3: Update monthly summary
        await updateMonthlySummary(date);
        
        // Step 4: Update yearly summary
        await updateYearlySummary(date);
        
        // Step 5: Process Bitcoin calculations
        await processDateBitcoinCalculations(date);
        
        progress.processedDates++;
      } catch (error) {
        console.error(`Error processing date ${date}:`, error);
        console.log('Continuing with next date...');
      }
    }
    
    const endTime = new Date();
    const elapsedMinutes = (endTime.getTime() - startTime.getTime()) / (1000 * 60);
    
    console.log(`\n===== Complete curtailment data reingestion finished at ${endTime.toISOString()} =====`);
    console.log(`Processed ${progress.processedDates}/${progress.totalDates} dates`);
    console.log(`Total records: ${progress.totalRecords}`);
    console.log(`Total time: ${elapsedMinutes.toFixed(1)} minutes`);
    
  } catch (error) {
    console.error('Error during reingestion process:', error);
    throw error;
  }
}

// Only run the script directly if it's the main module
if (require.main === module) {
  (async () => {
    try {
      console.log('Starting complete curtailment data reingestion script...');
      
      // Extract command line arguments for date range
      const args = process.argv.slice(2);
      const startDate = args[0]; // Format: YYYY-MM-DD
      const endDate = args[1];   // Format: YYYY-MM-DD
      
      if (startDate) {
        console.log(`Using start date: ${startDate}`);
      }
      
      if (endDate) {
        console.log(`Using end date: ${endDate}`);
      }
      
      await reingestAllCurtailmentData(startDate, endDate);
      
      console.log('Reingestion process completed successfully');
      process.exit(0);
    } catch (error) {
      console.error('Error during reingestion:', error);
      process.exit(1);
    }
  })();
}