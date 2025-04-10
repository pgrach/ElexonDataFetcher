/**
 * Complete Reingestion Script for April 2, 2025
 * 
 * This script performs a comprehensive reingestion of data for April 2, 2025:
 * 1. Fetches complete data from Elexon API for all 48 settlement periods
 * 2. Processes and stores curtailment records
 * 3. Recalculates Bitcoin mining potential for all three miner models
 * 4. Updates all daily, monthly, and yearly summary tables
 * 
 * Usage: npx tsx reingest_april2_complete.ts
 */

import { db } from "./db";
import { 
  curtailmentRecords, 
  dailySummaries, 
  monthlySummaries, 
  yearlySummaries,
  historicalBitcoinCalculations,
  bitcoinDailySummaries,
  bitcoinMonthlySummaries, 
  bitcoinYearlySummaries
} from "./db/schema";
import axios from "axios";
import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from 'url';
import { format } from "date-fns";
import { eq, sql, and, inArray } from "drizzle-orm";
import pLimit from "p-limit";

// Constants
const TARGET_DATE = "2025-04-02";
const ELEXON_BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1";
const MINER_MODELS = ["S19J_PRO", "S9", "M20S"];
const DIFFICULTY = 113757508810853; // Fallback difficulty if DynamoDB access fails
const BATCH_SIZE = 10; // Limit concurrent requests
const LOG_FILE_PATH = `./logs/reingest_april2_2025_${format(new Date(), "yyyy-MM-dd'T'HH-mm-ss")}.log`;

// Configure logger
const logger = {
  log: (message: string) => {
    const timestamp = new Date().toISOString();
    const logMessage = `[${timestamp}] ${message}`;
    console.log(logMessage);
    fs.appendFile(LOG_FILE_PATH, logMessage + "\n").catch(console.error);
  },
  error: (message: string, error: any) => {
    const timestamp = new Date().toISOString();
    const errorMessage = `[${timestamp}] ERROR: ${message}: ${error?.message || error}`;
    console.error(errorMessage);
    fs.appendFile(LOG_FILE_PATH, errorMessage + "\n").catch(console.error);
    if (error?.stack) {
      fs.appendFile(LOG_FILE_PATH, `[${timestamp}] STACK: ${error.stack}\n`).catch(console.error);
    }
  }
};

// Initialize logger file
async function initLogger() {
  try {
    await fs.writeFile(LOG_FILE_PATH, `===== REINGESTION LOG FOR ${TARGET_DATE} =====\n\n`);
    logger.log(`Log file initialized at ${LOG_FILE_PATH}`);
  } catch (error) {
    console.error("Failed to initialize log file:", error);
    // Continue even if log file creation fails
  }
}

// BMU mapping for wind farms
let windFarmIds: Set<string> | null = null;

// Delay utility function
async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Load BMU mapping for wind farms
async function loadWindFarmIds(): Promise<Set<string>> {
  if (windFarmIds !== null) {
    return windFarmIds;
  }

  try {
    const __filename = fileURLToPath(import.meta.url);
    const __dirname = path.dirname(__filename);
    const bmuMappingPath = path.join(__dirname, "data/bmu_mapping.json");
    
    logger.log(`Loading BMU mapping from: ${bmuMappingPath}`);
    const mappingContent = await fs.readFile(bmuMappingPath, 'utf8');
    const bmuMapping = JSON.parse(mappingContent);
    windFarmIds = new Set(bmuMapping.map((bmu: any) => bmu.elexonBmUnit));
    logger.log(`Loaded ${windFarmIds.size} wind farm BMU IDs`);
    return windFarmIds;
  } catch (error) {
    logger.error('Error loading BMU mapping', error);
    throw error;
  }
}

// Interface for Elexon API response
interface ElexonBidOffer {
  settlementDate: string;
  settlementPeriod: number;
  id: string;
  bmUnit?: string;
  volume: number;
  soFlag: boolean;
  cadlFlag: boolean | null;
  originalPrice: number;
  finalPrice: number;
  leadPartyName?: string;
}

// Fetch data from Elexon API for a specific date and period
async function fetchBidsOffers(date: string, period: number, retryCount = 0): Promise<ElexonBidOffer[]> {
  try {
    const validWindFarmIds = await loadWindFarmIds();
    logger.log(`Fetching data for ${date} Period ${period}...`);

    const url = `${ELEXON_BASE_URL}/datasets/BOALF/sp?settlementDate=${date}&settlementPeriod=${period}`;
    
    const response = await axios.get(url, {
      headers: { 'Accept': 'application/json' },
      timeout: 30000
    });

    if (!response.data || !Array.isArray(response.data.data)) {
      throw new Error(`Invalid response format from Elexon API for ${date} Period ${period}`);
    }

    // Filter to only include wind farm BMUs
    const windFarmData = response.data.data.filter((item: ElexonBidOffer) => 
      item.bmUnit && validWindFarmIds.has(item.bmUnit)
    );

    logger.log(`Retrieved ${windFarmData.length} wind farm records for ${date} Period ${period}`);
    return windFarmData;
  } catch (error) {
    if (axios.isAxiosError(error)) {
      if (error.response?.status === 429) {
        // Rate limited - retry after a delay
        if (retryCount < 5) {
          const delayTime = (retryCount + 1) * 60000; // Exponential backoff
          logger.log(`Rate limited for ${date} P${period}, retrying after ${delayTime/1000}s... (Attempt ${retryCount + 1}/5)`);
          await delay(delayTime);
          return fetchBidsOffers(date, period, retryCount + 1);
        } else {
          logger.error(`Rate limit retry exhausted for ${date} P${period}`, error);
        }
      } else if (error.response?.status === 404) {
        logger.log(`No data available for ${date} P${period} (404 Not Found)`);
        return [];
      } else {
        logger.error(`API error for ${date} P${period}`, error);
      }
    } else {
      logger.error(`Unexpected error fetching data for ${date} P${period}`, error);
    }
    
    // Return empty array on failure after retries
    return [];
  }
}

// Process and store curtailment records for a specific date and period
async function processPeriodCurtailment(date: string, period: number): Promise<number> {
  try {
    const bidsOffers = await fetchBidsOffers(date, period);
    
    // Filter for valid curtailment records:
    // 1. Negative volume (curtailment)
    // 2. soFlag or cadlFlag is true
    const curtailmentData = bidsOffers.filter(record => 
      record.volume < 0 && (record.soFlag === true || record.cadlFlag === true)
    );
    
    if (curtailmentData.length === 0) {
      logger.log(`No valid curtailment records for ${date} Period ${period}`);
      return 0;
    }
    
    // Delete existing records for this date and period
    await db.delete(curtailmentRecords)
      .where(
        and(
          eq(curtailmentRecords.settlementDate, date),
          eq(curtailmentRecords.settlementPeriod, period)
        )
      );
    
    // Process and insert curtailment records
    const insertData = curtailmentData.map(record => ({
      settlementDate: date,
      settlementPeriod: period,
      farmId: record.bmUnit || "",
      leadPartyName: record.leadPartyName || "Unknown",
      volume: Math.abs(record.volume).toString(), // Store as positive number
      price: record.originalPrice.toString(),
      payment: (Math.abs(record.volume) * record.originalPrice).toString(),
      soFlag: record.soFlag,
      cadlFlag: record.cadlFlag || false,
      createdAt: new Date(),
      updatedAt: new Date()
    }));
    
    await db.insert(curtailmentRecords).values(insertData);
    logger.log(`Inserted ${insertData.length} curtailment records for ${date} Period ${period}`);
    
    return insertData.length;
  } catch (error) {
    logger.error(`Error processing period ${period} for ${date}`, error);
    return 0;
  }
}

// Process all settlement periods for a specific date
async function processDailyCurtailment(date: string): Promise<number> {
  try {
    logger.log(`Starting curtailment processing for ${date}...`);
    
    // Process all 48 settlement periods with concurrency limit
    const limit = pLimit(BATCH_SIZE);
    const periods = Array.from({ length: 48 }, (_, i) => i + 1); // 1-48
    
    const results = await Promise.all(
      periods.map(period => limit(() => processPeriodCurtailment(date, period)))
    );
    
    const totalRecords = results.reduce((sum, count) => sum + count, 0);
    logger.log(`Completed curtailment processing for ${date}. Total records: ${totalRecords}`);
    
    return totalRecords;
  } catch (error) {
    logger.error(`Error processing daily curtailment for ${date}`, error);
    return 0;
  }
}

// Calculate and store Bitcoin mining potential for a specific miner model
async function processBitcoinCalculations(date: string, minerModel: string): Promise<number> {
  try {
    logger.log(`Processing Bitcoin calculations for ${date} using ${minerModel}...`);
    
    // Get difficulty value (use fallback if needed)
    let difficulty = DIFFICULTY;
    try {
      const difficultyQuery = await db.select({
        difficulty: sql<string>`difficulty::text`
      })
      .from(historicalBitcoinCalculations)
      .where(
        and(
          eq(historicalBitcoinCalculations.settlementDate, date),
          eq(historicalBitcoinCalculations.minerModel, minerModel)
        )
      )
      .limit(1);
      
      if (difficultyQuery.length > 0 && difficultyQuery[0].difficulty) {
        difficulty = parseInt(difficultyQuery[0].difficulty);
      }
    } catch (error) {
      logger.error(`Error retrieving difficulty, using fallback value ${DIFFICULTY}`, error);
    }
    
    logger.log(`Using difficulty ${difficulty} for ${date}`);
    
    // Delete existing Bitcoin calculations for this date and miner model
    await db.delete(historicalBitcoinCalculations)
      .where(
        and(
          eq(historicalBitcoinCalculations.settlementDate, date),
          eq(historicalBitcoinCalculations.minerModel, minerModel)
        )
      );
    
    logger.log(`Deleted existing calculations for ${date} and ${minerModel}`);
    
    // Get efficiency factors for each miner model
    const efficiencyFactors: Record<string, number> = {
      "S19J_PRO": 0.0000000717, // 0.0717 BTC per GWh at 113.76T difficulty
      "S9": 0.0000000223,       // 0.0223 BTC per GWh at 113.76T difficulty
      "M20S": 0.0000000442      // 0.0442 BTC per GWh at 113.76T difficulty
    };
    
    const efficiencyFactor = efficiencyFactors[minerModel] || efficiencyFactors["S19J_PRO"];
    
    // Query curtailment records for this date
    const curtailmentData = await db
      .select({
        settlementDate: curtailmentRecords.settlementDate,
        settlementPeriod: curtailmentRecords.settlementPeriod,
        farmId: curtailmentRecords.farmId,
        volume: sql<string>`volume::text`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date));
    
    logger.log(`Processing ${curtailmentData.length} curtailment records`);
    
    // Calculate Bitcoin for each curtailment record
    const batchSize = 50; // Process in batches to avoid memory issues
    const batches = Math.ceil(curtailmentData.length / batchSize);
    let totalBitcoin = 0;
    
    for (let i = 0; i < batches; i++) {
      const start = i * batchSize;
      const end = Math.min(start + batchSize, curtailmentData.length);
      const batch = curtailmentData.slice(start, end);
      
      const batchCalculations = batch.map(record => {
        const volume = parseFloat(record.volume);
        const bitcoinMined = (volume * efficiencyFactor * (DIFFICULTY / difficulty)).toString();
        totalBitcoin += parseFloat(bitcoinMined);
        
        return {
          settlementDate: record.settlementDate,
          settlementPeriod: record.settlementPeriod,
          minerModel: minerModel,
          farmId: record.farmId,
          bitcoinMined: bitcoinMined,
          difficulty: difficulty.toString(),
          createdAt: new Date(),
          updatedAt: new Date()
        };
      });
      
      // Insert batch of calculations
      await db.insert(historicalBitcoinCalculations).values(batchCalculations);
      logger.log(`Inserted batch ${i+1}/${batches} (${batchCalculations.length} records)`);
    }
    
    logger.log(`Successfully processed ${curtailmentData.length} Bitcoin calculations for ${minerModel}`);
    logger.log(`Total Bitcoin calculated: ${totalBitcoin.toFixed(8)}`);
    
    return curtailmentData.length;
  } catch (error) {
    logger.error(`Error processing Bitcoin calculations for ${date} and ${minerModel}`, error);
    return 0;
  }
}

// Update daily summary for a specific date
async function updateDailySummary(date: string): Promise<void> {
  try {
    logger.log(`Updating daily summary for ${date}...`);
    
    // Delete existing daily summary for this date
    await db.delete(dailySummaries)
      .where(eq(dailySummaries.summaryDate, date));
    
    // Calculate aggregated values from curtailment records
    const curtailmentStats = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(volume::numeric)::text`,
        totalPayment: sql<string>`SUM(payment::numeric)::text`,
        farmCount: sql<number>`COUNT(DISTINCT farm_id)::int`,
        periodCount: sql<number>`COUNT(DISTINCT settlement_period)::int`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date));
    
    if (curtailmentStats.length > 0 && curtailmentStats[0].totalCurtailedEnergy) {
      // Insert new daily summary
      await db.insert(dailySummaries).values({
        summaryDate: date,
        totalCurtailedEnergy: curtailmentStats[0].totalCurtailedEnergy,
        totalPayment: curtailmentStats[0].totalPayment,
        farmCount: curtailmentStats[0].farmCount,
        periodCount: curtailmentStats[0].periodCount,
        createdAt: new Date(),
        updatedAt: new Date()
      });
      
      logger.log(`Created daily summary for ${date}`);
    } else {
      logger.log(`No data to create daily summary for ${date}`);
    }
  } catch (error) {
    logger.error(`Error updating daily summary for ${date}`, error);
  }
}

// Update monthly summary for a specific month
async function updateMonthlySummary(yearMonth: string): Promise<void> {
  try {
    logger.log(`Updating monthly summary for ${yearMonth}...`);
    
    // Delete existing monthly summary for this month
    await db.delete(monthlySummaries)
      .where(eq(monthlySummaries.yearMonth, yearMonth));
    
    // Calculate aggregated values from daily summaries
    const monthlyStats = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(total_curtailed_energy::numeric)::text`,
        totalPayment: sql<string>`SUM(total_payment::numeric)::text`,
        avgFarmCount: sql<number>`ROUND(AVG(farm_count))::int`,
        totalDays: sql<number>`COUNT(*)::int`
      })
      .from(dailySummaries)
      .where(sql`summary_date::text LIKE ${yearMonth + '%'}`);
    
    if (monthlyStats.length > 0 && monthlyStats[0].totalCurtailedEnergy) {
      // Insert new monthly summary
      await db.insert(monthlySummaries).values({
        yearMonth: yearMonth,
        totalCurtailedEnergy: monthlyStats[0].totalCurtailedEnergy,
        totalPayment: monthlyStats[0].totalPayment,
        avgFarmCount: monthlyStats[0].avgFarmCount,
        totalDays: monthlyStats[0].totalDays,
        createdAt: new Date(),
        updatedAt: new Date()
      });
      
      logger.log(`Created monthly summary for ${yearMonth}`);
    } else {
      logger.log(`No data to create monthly summary for ${yearMonth}`);
    }
  } catch (error) {
    logger.error(`Error updating monthly summary for ${yearMonth}`, error);
  }
}

// Update yearly summary for a specific year
async function updateYearlySummary(year: string): Promise<void> {
  try {
    logger.log(`Updating yearly summary for ${year}...`);
    
    // Delete existing yearly summary for this year
    await db.delete(yearlySummaries)
      .where(eq(yearlySummaries.year, year));
    
    // Calculate aggregated values from monthly summaries
    const yearlyStats = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(total_curtailed_energy::numeric)::text`,
        totalPayment: sql<string>`SUM(total_payment::numeric)::text`,
        avgFarmCount: sql<number>`ROUND(AVG(avg_farm_count))::int`,
        totalDays: sql<number>`SUM(total_days)::int`
      })
      .from(monthlySummaries)
      .where(sql`year_month::text LIKE ${year + '%'}`);
    
    if (yearlyStats.length > 0 && yearlyStats[0].totalCurtailedEnergy) {
      // Insert new yearly summary
      await db.insert(yearlySummaries).values({
        year: year,
        totalCurtailedEnergy: yearlyStats[0].totalCurtailedEnergy,
        totalPayment: yearlyStats[0].totalPayment,
        avgFarmCount: yearlyStats[0].avgFarmCount,
        totalDays: yearlyStats[0].totalDays,
        createdAt: new Date(),
        updatedAt: new Date()
      });
      
      logger.log(`Created yearly summary for ${year}`);
    } else {
      logger.log(`No data to create yearly summary for ${year}`);
    }
  } catch (error) {
    logger.error(`Error updating yearly summary for ${year}`, error);
  }
}

// Update Bitcoin daily summary for a specific date and miner models
async function updateBitcoinDailySummaries(date: string): Promise<void> {
  try {
    logger.log(`Updating Bitcoin daily summaries for ${date}...`);
    
    // Delete existing Bitcoin daily summaries for this date
    await db.delete(bitcoinDailySummaries)
      .where(
        and(
          eq(bitcoinDailySummaries.summaryDate, date),
          inArray(bitcoinDailySummaries.minerModel, MINER_MODELS)
        )
      );
    
    // Process each miner model
    for (const minerModel of MINER_MODELS) {
      // Calculate total Bitcoin mined for the day
      const dailyStats = await db
        .select({
          totalBitcoin: sql<string>`SUM(bitcoin_mined::numeric)::text`
        })
        .from(historicalBitcoinCalculations)
        .where(
          and(
            eq(historicalBitcoinCalculations.settlementDate, date),
            eq(historicalBitcoinCalculations.minerModel, minerModel)
          )
        );
      
      if (dailyStats.length > 0 && dailyStats[0].totalBitcoin) {
        // Insert new Bitcoin daily summary
        await db.insert(bitcoinDailySummaries).values({
          summaryDate: date,
          minerModel: minerModel,
          bitcoinMined: dailyStats[0].totalBitcoin,
          createdAt: new Date(),
          updatedAt: new Date()
        });
        
        logger.log(`Created Bitcoin daily summary for ${date} and ${minerModel}`);
      }
    }
  } catch (error) {
    logger.error(`Error updating Bitcoin daily summaries for ${date}`, error);
  }
}

// Update Bitcoin monthly summary for a specific month
async function updateBitcoinMonthlySummaries(yearMonth: string): Promise<void> {
  try {
    logger.log(`Updating Bitcoin monthly summaries for ${yearMonth}...`);
    
    // Delete existing Bitcoin monthly summaries for this month
    await db.delete(bitcoinMonthlySummaries)
      .where(
        and(
          eq(bitcoinMonthlySummaries.yearMonth, yearMonth),
          inArray(bitcoinMonthlySummaries.minerModel, MINER_MODELS)
        )
      );
    
    // Process each miner model
    for (const minerModel of MINER_MODELS) {
      // Calculate total Bitcoin mined for the month
      const monthlyStats = await db
        .select({
          totalBitcoin: sql<string>`SUM(bitcoin_mined::numeric)::text`
        })
        .from(bitcoinDailySummaries)
        .where(
          and(
            sql`summary_date::text LIKE ${yearMonth + '%'}`,
            eq(bitcoinDailySummaries.minerModel, minerModel)
          )
        );
      
      if (monthlyStats.length > 0 && monthlyStats[0].totalBitcoin) {
        // Insert new Bitcoin monthly summary
        await db.insert(bitcoinMonthlySummaries).values({
          yearMonth: yearMonth,
          minerModel: minerModel,
          bitcoinMined: monthlyStats[0].totalBitcoin,
          createdAt: new Date(),
          updatedAt: new Date()
        });
        
        logger.log(`Created Bitcoin monthly summary for ${yearMonth} and ${minerModel}`);
      }
    }
  } catch (error) {
    logger.error(`Error updating Bitcoin monthly summaries for ${yearMonth}`, error);
  }
}

// Update Bitcoin yearly summary for a specific year
async function updateBitcoinYearlySummaries(year: string): Promise<void> {
  try {
    logger.log(`Updating Bitcoin yearly summaries for ${year}...`);
    
    // Delete existing Bitcoin yearly summaries for this year
    await db.delete(bitcoinYearlySummaries)
      .where(
        and(
          eq(bitcoinYearlySummaries.year, year),
          inArray(bitcoinYearlySummaries.minerModel, MINER_MODELS)
        )
      );
    
    // Process each miner model
    for (const minerModel of MINER_MODELS) {
      // Calculate total Bitcoin mined for the year
      const yearlyStats = await db
        .select({
          totalBitcoin: sql<string>`SUM(bitcoin_mined::numeric)::text`
        })
        .from(bitcoinMonthlySummaries)
        .where(
          and(
            sql`year_month::text LIKE ${year + '%'}`,
            eq(bitcoinMonthlySummaries.minerModel, minerModel)
          )
        );
      
      if (yearlyStats.length > 0 && yearlyStats[0].totalBitcoin) {
        // Insert new Bitcoin yearly summary
        await db.insert(bitcoinYearlySummaries).values({
          year: year,
          minerModel: minerModel,
          bitcoinMined: yearlyStats[0].totalBitcoin,
          createdAt: new Date(),
          updatedAt: new Date()
        });
        
        logger.log(`Created Bitcoin yearly summary for ${year} and ${minerModel}`);
      }
    }
  } catch (error) {
    logger.error(`Error updating Bitcoin yearly summaries for ${year}`, error);
  }
}

// Main function
async function main() {
  try {
    await initLogger();
    
    logger.log(`===== STARTING COMPLETE REINGESTION FOR ${TARGET_DATE} =====`);
    
    // Step 1: Process curtailment records from Elexon API
    logger.log("\n=== Step 1: Processing Curtailment Records ===\n");
    const totalCurtailmentRecords = await processDailyCurtailment(TARGET_DATE);
    logger.log(`Processed ${totalCurtailmentRecords} total curtailment records for ${TARGET_DATE}`);
    
    // Step 2: Calculate Bitcoin mining potential for all miner models
    logger.log("\n=== Step 2: Calculating Bitcoin Mining Potential ===\n");
    for (const minerModel of MINER_MODELS) {
      await processBitcoinCalculations(TARGET_DATE, minerModel);
    }
    
    // Step 3: Update daily summary
    logger.log("\n=== Step 3: Updating Daily Summary ===\n");
    await updateDailySummary(TARGET_DATE);
    
    // Step 4: Update Bitcoin daily summaries
    logger.log("\n=== Step 4: Updating Bitcoin Daily Summaries ===\n");
    await updateBitcoinDailySummaries(TARGET_DATE);
    
    // Step 5: Update monthly summaries (both regular and Bitcoin)
    const yearMonth = TARGET_DATE.substring(0, 7); // YYYY-MM
    logger.log(`\n=== Step 5: Updating Monthly Summaries for ${yearMonth} ===\n`);
    await updateMonthlySummary(yearMonth);
    await updateBitcoinMonthlySummaries(yearMonth);
    
    // Step 6: Update yearly summaries (both regular and Bitcoin)
    const year = TARGET_DATE.substring(0, 4); // YYYY
    logger.log(`\n=== Step 6: Updating Yearly Summaries for ${year} ===\n`);
    await updateYearlySummary(year);
    await updateBitcoinYearlySummaries(year);
    
    // Step 7: Verify the results
    logger.log("\n=== Step 7: Verification ===\n");
    
    // Verify curtailment records
    const curtailmentStats = await db
      .select({
        recordCount: sql<number>`COUNT(*)::int`,
        periodCount: sql<number>`COUNT(DISTINCT settlement_period)::int`,
        farmCount: sql<number>`COUNT(DISTINCT farm_id)::int`,
        totalVolume: sql<string>`SUM(volume::numeric)::text`,
        totalPayment: sql<string>`SUM(payment::numeric)::text`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    logger.log(`Curtailment Records Stats:
- Total Records: ${curtailmentStats[0]?.recordCount || 0}
- Unique Settlement Periods: ${curtailmentStats[0]?.periodCount || 0}/48
- Unique Farms: ${curtailmentStats[0]?.farmCount || 0}
- Total Volume: ${Number(curtailmentStats[0]?.totalVolume || 0).toFixed(2)} MWh
- Total Payment: £${Number(curtailmentStats[0]?.totalPayment || 0).toFixed(2)}`);
    
    // Verify Bitcoin calculations for each miner model
    logger.log("\nBitcoin Calculation Stats:");
    for (const minerModel of MINER_MODELS) {
      const bitcoinStats = await db
        .select({
          recordCount: sql<number>`COUNT(*)::int`,
          periodCount: sql<number>`COUNT(DISTINCT settlement_period)::int`,
          farmCount: sql<number>`COUNT(DISTINCT farm_id)::int`,
          totalBitcoin: sql<string>`SUM(bitcoin_mined::numeric)::text`
        })
        .from(historicalBitcoinCalculations)
        .where(
          and(
            eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
            eq(historicalBitcoinCalculations.minerModel, minerModel)
          )
        );
      
      logger.log(`${minerModel}:
- Records: ${bitcoinStats[0]?.recordCount || 0}
- Periods: ${bitcoinStats[0]?.periodCount || 0}
- Farms: ${bitcoinStats[0]?.farmCount || 0}
- Bitcoin: ${Number(bitcoinStats[0]?.totalBitcoin || 0).toFixed(8)} BTC`);
    }
    
    logger.log("\n===== COMPLETE REINGESTION FINISHED SUCCESSFULLY =====");
    
  } catch (error) {
    logger.error("Main process error", error);
  }
}

// Execute the main function
main().catch(error => {
  console.error("Fatal error:", error);
  process.exit(1);
});