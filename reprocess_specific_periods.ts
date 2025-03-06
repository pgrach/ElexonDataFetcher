/**
 * Targeted Re-ingest Script for Specific Missing Periods
 * 
 * This script re-ingests specific missing periods for 2025-03-05 from the Elexon API.
 * 
 * Usage:
 *   npx tsx reprocess_specific_periods.ts
 */

import { db } from "@db";
import { curtailmentRecords, dailySummaries } from "@db/schema";
import { fetchBidsOffers } from "./server/services/elexon";
import { processSingleDay } from "./server/services/bitcoinService";
import { eq, sql } from "drizzle-orm";
import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from 'url';

const TARGET_DATE = '2025-03-05';
const MISSING_PERIODS = [37, 39, 41, 42, 43, 44, 45, 46, 47];

// Path to the BMU mapping file - mirroring the path used in curtailment.ts
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const BMU_MAPPING_PATH = path.join(__dirname, "./server/data/bmuMapping.json");

let windFarmBmuIds: Set<string> | null = null;
let bmuLeadPartyMap: Map<string, string> | null = null;

async function sleep(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

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

async function processPeriod(date: string, period: number): Promise<{volume: number, payment: number}> {
  try {
    const validWindFarmIds = await loadWindFarmIds();
    
    console.log(`Processing ${date} period ${period}...`);
    const records = await fetchBidsOffers(date, period);
    const validRecords = records.filter(record =>
      record.volume < 0 &&
      (record.soFlag || record.cadlFlag) &&
      validWindFarmIds.has(record.id)
    );

    if (validRecords.length > 0) {
      console.log(`[${date} P${period}] Processing ${validRecords.length} records`);
    } else {
      console.log(`[${date} P${period}] No valid records found`);
      return { volume: 0, payment: 0 };
    }

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

          console.log(`[${date} P${period}] Added record for ${record.id}: ${volume.toFixed(2)} MWh, £${payment.toFixed(2)}`);
          return { volume, payment };
        } catch (error) {
          console.error(`[${date} P${period}] Error inserting record for ${record.id}:`, error);
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
      console.log(`[${date} P${period}] Total: ${periodTotal.volume.toFixed(2)} MWh, £${periodTotal.payment.toFixed(2)}`);
    }

    return periodTotal;
  } catch (error) {
    console.error(`Error processing period ${period} for date ${date}:`, error);
    return { volume: 0, payment: 0 };
  }
}

async function updateSummaries(totalVolume: number, totalPayment: number) {
  try {
    // Get current daily summary
    const currentSummary = await db.query.dailySummaries.findFirst({
      where: eq(dailySummaries.summaryDate, TARGET_DATE)
    });

    if (currentSummary) {
      // Add to existing values
      const updatedVolume = Number(currentSummary.totalCurtailedEnergy) + totalVolume;
      const updatedPayment = Number(currentSummary.totalPayment) + totalPayment;

      // Update daily summary
      await db.update(dailySummaries)
        .set({
          totalCurtailedEnergy: updatedVolume.toString(),
          totalPayment: updatedPayment.toString()
        })
        .where(eq(dailySummaries.summaryDate, TARGET_DATE));

      console.log(`Updated daily summary for ${TARGET_DATE}: ${updatedVolume.toFixed(2)} MWh, £${updatedPayment.toFixed(2)}`);
    } else {
      // Insert new daily summary
      await db.insert(dailySummaries).values({
        summaryDate: TARGET_DATE,
        totalCurtailedEnergy: totalVolume.toString(),
        totalPayment: totalPayment.toString()
      });

      console.log(`Created daily summary for ${TARGET_DATE}: ${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)}`);
    }
  } catch (error) {
    console.error(`Error updating summaries for ${TARGET_DATE}:`, error);
  }
}

async function reprocessSpecificPeriods() {
  console.log(`\n=== Reprocessing Specific Periods for ${TARGET_DATE} ===\n`);
  
  // Check initial state
  console.log('Checking initial database state...');
  const beforeCheck = await db
    .select({
      recordCount: sql<number>`COUNT(*)`,
      periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
      totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
      totalPayment: sql<string>`SUM(payment::numeric)`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
  
  console.log(`Initial state: ${beforeCheck[0].recordCount} records across ${beforeCheck[0].periodCount} periods`);
  console.log(`Total volume: ${Number(beforeCheck[0].totalVolume).toFixed(2)} MWh`);
  console.log(`Total payment: £${Number(beforeCheck[0].totalPayment).toFixed(2)}`);

  try {
    console.log(`\nProcessing ${MISSING_PERIODS.length} missing periods for ${TARGET_DATE}...`);
    
    let totalAddedVolume = 0;
    let totalAddedPayment = 0;
    
    // Process each missing period with a delay between requests
    for (const period of MISSING_PERIODS) {
      const result = await processPeriod(TARGET_DATE, period);
      totalAddedVolume += result.volume;
      totalAddedPayment += result.payment;
      
      // Add a small delay between API calls to avoid rate limiting
      await sleep(500);
    }
    
    console.log(`\nTotal added from missing periods: ${totalAddedVolume.toFixed(2)} MWh, £${totalAddedPayment.toFixed(2)}`);
    
    // Update daily summary with the new totals
    await updateSummaries(totalAddedVolume, totalAddedPayment);
    
    // Verify all periods were processed
    const afterCheck = await db
      .select({
        recordCount: sql<number>`COUNT(*)`,
        periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
        totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
        totalPayment: sql<string>`SUM(payment::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log(`\nFinal state: ${afterCheck[0].recordCount} records across ${afterCheck[0].periodCount} periods`);
    console.log(`Total volume: ${Number(afterCheck[0].totalVolume).toFixed(2)} MWh`);
    console.log(`Total payment: £${Number(afterCheck[0].totalPayment).toFixed(2)}`);
    
    // Update Bitcoin calculations based on the refreshed data
    console.log(`\nUpdating Bitcoin calculations for ${TARGET_DATE}...`);
    
    // Process for all miner models
    const minerModels = ['S19J_PRO', 'S9', 'M20S'];
    for (const model of minerModels) {
      await processSingleDay(TARGET_DATE, model);
    }
    
    console.log(`\n=== Reprocessing Complete for ${TARGET_DATE} ===`);
  } catch (error) {
    console.error(`Error reprocessing data for ${TARGET_DATE}:`, error);
  }
}

// Execute the reprocessing
reprocessSpecificPeriods();