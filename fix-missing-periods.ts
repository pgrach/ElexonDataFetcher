#!/usr/bin/env tsx
/**
 * Fix Missing Settlement Periods
 * 
 * This script specifically targets and reingests data for settlement periods 41-44
 * which are missing from the 2025-03-04 data.
 */

import { fetchBidsOffers } from "./server/services/elexon";
import { db } from "./db";
import { curtailmentRecords, dailySummaries, historicalBitcoinCalculations, InsertHistoricalBitcoinCalculation } from "./db/schema";
import { eq, sql, count, between, inArray } from "drizzle-orm";
import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const BMU_MAPPING_PATH = path.join(__dirname, "server/data/bmuMapping.json");
const TARGET_DATE = '2025-03-04';
const MISSING_PERIODS = [47, 48];
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

// Helper function for logging
function log(message: string, type: "info" | "success" | "warning" | "error" = "info") {
  const timestamp = new Date().toISOString();
  let prefix = "";
  
  switch (type) {
    case "success":
      prefix = "\x1b[32m✓\x1b[0m "; // Green checkmark
      break;
    case "warning":
      prefix = "\x1b[33m⚠\x1b[0m "; // Yellow warning
      break;
    case "error":
      prefix = "\x1b[31m✗\x1b[0m "; // Red X
      break;
    default:
      prefix = "\x1b[36m•\x1b[0m "; // Blue dot for info
  }
  
  console.log(`${prefix}[${timestamp.split('T')[1].split('.')[0]}] ${message}`);
}

// Load wind farm BMU IDs
async function loadWindFarmIds(): Promise<{ windFarmBmuIds: Set<string>; bmuLeadPartyMap: Map<string, string> }> {
  try {
    console.log('Loading BMU mapping from:', BMU_MAPPING_PATH);
    const mappingContent = await fs.readFile(BMU_MAPPING_PATH, 'utf8');
    const bmuMapping = JSON.parse(mappingContent);
    console.log(`Loaded ${bmuMapping.length} BMU mappings`);

    const windFarmBmuIds = new Set<string>(
      bmuMapping
        .filter((bmu: any) => bmu.fuelType === "WIND")
        .map((bmu: any) => bmu.elexonBmUnit as string)
    );

    const bmuLeadPartyMap = new Map<string, string>(
      bmuMapping
        .filter((bmu: any) => bmu.fuelType === "WIND")
        .map((bmu: any) => [bmu.elexonBmUnit as string, (bmu.leadPartyName || 'Unknown') as string])
    );

    console.log(`Found ${windFarmBmuIds.size} wind farm BMUs`);

    return { windFarmBmuIds, bmuLeadPartyMap };
  } catch (error) {
    console.error('Error loading BMU mapping:', error);
    throw error;
  }
}

async function main() {
  log(`Fixing missing settlement periods (${MISSING_PERIODS.join(', ')}) for ${TARGET_DATE}`, "info");
  
  try {
    // Load BMU mapping
    const { windFarmBmuIds, bmuLeadPartyMap } = await loadWindFarmIds();
    let totalVolume = 0;
    let totalPayment = 0;
    
    // Process each missing period
    for (const period of MISSING_PERIODS) {
      try {
        log(`Processing settlement period ${period}...`, "info");
        
        // Fetch data from Elexon API
        const records = await fetchBidsOffers(TARGET_DATE, period);
        const validRecords = records.filter(record =>
          record.volume < 0 &&
          (record.soFlag || record.cadlFlag) &&
          windFarmBmuIds.has(record.id)
        );
        
        if (validRecords.length === 0) {
          log(`No valid records found for period ${period}`, "warning");
          continue;
        }
        
        log(`[${TARGET_DATE} P${period}] Processing ${validRecords.length} records`, "info");
        
        // Insert records into database
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
              
              log(`[${TARGET_DATE} P${period}] Added record for ${record.id}: ${volume} MWh, £${payment}`, "success");
              return { volume, payment };
            } catch (error) {
              log(`[${TARGET_DATE} P${period}] Error inserting record for ${record.id}: ${error}`, "error");
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
        
        totalVolume += periodTotal.volume;
        totalPayment += periodTotal.payment;
        
        log(`[${TARGET_DATE} P${period}] Total: ${periodTotal.volume.toFixed(2)} MWh, £${periodTotal.payment.toFixed(2)}`, "success");
      } catch (error) {
        log(`Error processing period ${period}: ${error}`, "error");
      }
    }
    
    // Update daily summary
    await db.update(dailySummaries)
      .set({
        totalCurtailedEnergy: sql`${dailySummaries.totalCurtailedEnergy}::numeric + ${totalVolume}`,
        totalPayment: sql`${dailySummaries.totalPayment}::numeric + ${totalPayment}`
      })
      .where(eq(dailySummaries.summaryDate, TARGET_DATE));
    
    log(`Updated daily summary with additional volume: ${totalVolume.toFixed(2)} MWh, payment: £${totalPayment.toFixed(2)}`, "success");
    
    // Update monthly and yearly summaries
    const yearMonth = TARGET_DATE.substring(0, 7);
    const year = TARGET_DATE.substring(0, 4);
    
    // Import the specific tables we need
    const { monthlySummaries, yearlySummaries } = await import('./db/schema');
    
    // Update monthly summary
    const monthlyTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(${dailySummaries.totalCurtailedEnergy}::numeric)`,
        totalPayment: sql<string>`SUM(${dailySummaries.totalPayment}::numeric)`
      })
      .from(dailySummaries)
      .where(sql`date_trunc('month', ${dailySummaries.summaryDate}::date) = date_trunc('month', ${TARGET_DATE}::date)`);
    
    if (monthlyTotals[0].totalCurtailedEnergy && monthlyTotals[0].totalPayment) {
      await db.update(monthlySummaries)
        .set({
          totalCurtailedEnergy: monthlyTotals[0].totalCurtailedEnergy,
          totalPayment: monthlyTotals[0].totalPayment,
          updatedAt: new Date()
        })
        .where(eq(monthlySummaries.yearMonth, yearMonth));
    }
    
    // Update yearly summary
    const yearlyTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(${dailySummaries.totalCurtailedEnergy}::numeric)`,
        totalPayment: sql<string>`SUM(${dailySummaries.totalPayment}::numeric)`
      })
      .from(dailySummaries)
      .where(sql`date_trunc('year', ${dailySummaries.summaryDate}::date) = date_trunc('year', ${TARGET_DATE}::date)`);
    
    if (yearlyTotals[0].totalCurtailedEnergy && yearlyTotals[0].totalPayment) {
      await db.update(yearlySummaries)
        .set({
          totalCurtailedEnergy: yearlyTotals[0].totalCurtailedEnergy,
          totalPayment: yearlyTotals[0].totalPayment,
          updatedAt: new Date()
        })
        .where(eq(yearlySummaries.year, year));
    }
    
    // Final statistics
    const finalStats = await db
      .select({
        recordCount: count(curtailmentRecords.id),
        totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
        totalPayment: sql<string>`SUM(payment::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    log("Data fix completed successfully", "success");
    log(`Final statistics for ${TARGET_DATE}:`, "info");
    log(`Total records: ${finalStats[0].recordCount}`, "info");
    log(`Total volume: ${Number(finalStats[0].totalVolume).toFixed(2)} MWh`, "info");
    log(`Total payment: £${Number(finalStats[0].totalPayment).toFixed(2)}`, "info");
    
    // Now that we have new curtailment records, we need to update the Bitcoin calculations
    log("Updating Bitcoin calculations for the new data...", "info");
    
    // Import the necessary modules for Bitcoin calculations
    const { getDifficultyData } = await import('./server/services/dynamodbService');
    const { minerModels } = await import('./server/types/bitcoin');
    
    // Get the current difficulty for the date
    const difficulty = await getDifficultyData(TARGET_DATE);
    log(`Using difficulty ${difficulty} for ${TARGET_DATE}`, "info");
    
    // Define the Bitcoin calculation function based on direct-reingest.ts
    function calculateBitcoinForBMU(
      curtailedMwh: number,
      minerModel: string,
      difficulty: number
    ): { bitcoinMined: number; curtailedMwh: number } {
      // Get miner stats
      const minerStats = minerModels[minerModel];
      if (!minerStats) {
        throw new Error(`Unknown miner model: ${minerModel}`);
      }

      // Convert MWh to kWh
      const kWh = curtailedMwh * 1000;
      
      // Calculate total possible hashes with this energy
      const hashesPerJoule = minerStats.hashrate * 1e12 / (minerStats.power * 3600);
      const totalJoules = kWh * 3.6e6;
      const totalHashes = totalJoules * hashesPerJoule;
      
      // Calculate expected bitcoin
      const BLOCK_REWARD = 3.125;
      const hashesPerBlock = difficulty * 2**32;
      const totalBlocks = totalHashes / hashesPerBlock;
      const bitcoinMined = totalBlocks * BLOCK_REWARD;
      
      return { bitcoinMined, curtailedMwh };
    }
    
    for (const minerModel of MINER_MODELS) {
      log(`Processing Bitcoin calculations for ${minerModel}...`, "info");
      
      // Delete existing calculations for these periods
      await db.delete(historicalBitcoinCalculations)
        .where(
          eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE) &&
          eq(historicalBitcoinCalculations.minerModel, minerModel) &&
          between(historicalBitcoinCalculations.settlementPeriod, MISSING_PERIODS[0], MISSING_PERIODS[MISSING_PERIODS.length - 1])
        );
      
      // Get all curtailment records for the missing periods
      const periodRecords = await db
        .select()
        .from(curtailmentRecords)
        .where(
          eq(curtailmentRecords.settlementDate, TARGET_DATE) &&
          between(curtailmentRecords.settlementPeriod, MISSING_PERIODS[0], MISSING_PERIODS[MISSING_PERIODS.length - 1])
        );
      
      // Group records by period and farm ID for processing
      const groupedRecords = periodRecords.reduce((groups, record) => {
        const key = `${record.settlementPeriod}_${record.farmId}`;
        if (!groups[key]) {
          groups[key] = [];
        }
        groups[key].push(record);
        return groups;
      }, {} as Record<string, any[]>);
      
      // Process each group
      let totalBitcoin = 0;
      const calculatedAt = new Date();
      
      // Using the schema's insert type
      const batchInsertValues: InsertHistoricalBitcoinCalculation[] = [];
      
      for (const [key, records] of Object.entries(groupedRecords)) {
        const [periodStr, farmId] = key.split('_');
        const period = parseInt(periodStr, 10);
        
        // Calculate total curtailed energy for this farm in this period
        const totalCurtailedMwh = records.reduce((sum, record) => {
          return sum + Math.abs(parseFloat(record.volume));
        }, 0);
        
        // Calculate Bitcoin for this farm and period
        const bitcoinCalculation = calculateBitcoinForBMU(
          totalCurtailedMwh,
          minerModel,
          difficulty
        );
        
        totalBitcoin += bitcoinCalculation.bitcoinMined;
        
        // Add to batch insert values
        batchInsertValues.push({
          settlementDate: TARGET_DATE,
          settlementPeriod: period,
          farmId,
          minerModel,
          bitcoinMined: bitcoinCalculation.bitcoinMined.toString(),
          difficulty: difficulty.toString(),
          calculatedAt
        });
      }
      
      // Insert all the bitcoin calculations
      if (batchInsertValues.length > 0) {
        await db.insert(historicalBitcoinCalculations).values(batchInsertValues);
        log(`Inserted ${batchInsertValues.length} Bitcoin calculation records for ${minerModel}`, "success");
        log(`Total Bitcoin calculated: ${totalBitcoin.toFixed(8)} BTC`, "info");
      } else {
        log(`No Bitcoin calculations to insert for ${minerModel}`, "warning");
      }
      
      log(`Bitcoin calculations completed for ${minerModel}`, "success");
    }
    
    log("Fix completed", "success");
    
  } catch (error) {
    log(`Error during processing: ${error}`, "error");
    process.exit(1);
  }
}

main();