/**
 * Fix Missing Periods Script
 * 
 * This script identifies and adds missing periods of curtailment data for a specific date
 * by comparing existing database records with data from the Elexon API.
 * 
 * Usage:
 *   npx tsx scripts/fix_missing_periods.ts [date]
 *   
 * If no date is provided, it defaults to 2025-03-04.
 */

import { db } from "../db";
import { curtailmentRecords, dailySummaries, monthlySummaries, yearlySummaries } from "../db/schema";
import { fetchBidsOffers } from "../server/services/elexon";
import { eq, sql } from "drizzle-orm";
import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const BMU_MAPPING_PATH = path.join(__dirname, "../server/data/bmuMapping.json");
const TARGET_DATE = process.argv[2] || '2025-03-04';

let windFarmBmuIds: Set<string> | null = null;
let bmuLeadPartyMap: Map<string, string> | null = null;

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

async function findMissingPeriods(date: string): Promise<number[]> {
  console.log(`Finding missing periods for ${date}...`);
  
  const existingPeriods = await db
    .select({ period: curtailmentRecords.settlementPeriod })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date))
    .groupBy(curtailmentRecords.settlementPeriod);
  
  const existingPeriodSet = new Set(existingPeriods.map(p => p.period));
  const allPeriods = Array.from({ length: 48 }, (_, i) => i + 1);
  const missingPeriods = allPeriods.filter(p => !existingPeriodSet.has(p));
  
  console.log(`Found ${missingPeriods.length} missing periods: ${missingPeriods.join(', ')}`);
  return missingPeriods;
}

async function processMissingPeriods(date: string, missingPeriods: number[]): Promise<void> {
  const validWindFarmIds = await loadWindFarmIds();
  let addedVolume = 0;
  let addedPayment = 0;
  let addedRecords = 0;
  
  console.log(`Processing ${missingPeriods.length} missing periods for ${date}`);
  
  for (const period of missingPeriods) {
    try {
      console.log(`\nProcessing period ${period}...`);
      const records = await fetchBidsOffers(date, period);
      const validRecords = records.filter(record =>
        record.volume < 0 &&
        (record.soFlag || record.cadlFlag) &&
        validWindFarmIds.has(record.id)
      );

      console.log(`Found ${validRecords.length} valid records for period ${period}`);

      let periodVolume = 0;
      let periodPayment = 0;

      for (const record of validRecords) {
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

          console.log(`Added record for ${record.id}: ${volume.toFixed(2)} MWh, £${payment.toFixed(2)}`);
          periodVolume += volume;
          periodPayment += payment;
          addedRecords++;
        } catch (error) {
          console.error(`Error inserting record for ${record.id}:`, error);
        }
      }

      addedVolume += periodVolume;
      addedPayment += periodPayment;
      
      if (periodVolume > 0) {
        console.log(`Period ${period} total: ${periodVolume.toFixed(2)} MWh, £${periodPayment.toFixed(2)}`);
      }
    } catch (error) {
      console.error(`Error processing period ${period}:`, error);
    }
  }

  console.log(`\nProcessing complete! Added ${addedRecords} records.`);
  console.log(`Total volume added: ${addedVolume.toFixed(2)} MWh`);
  console.log(`Total payment added: £${addedPayment.toFixed(2)}`);

  // Update summaries if records were added
  if (addedRecords > 0) {
    await updateSummaries(date);
  }
}

async function updateSummaries(date: string): Promise<void> {
  try {
    console.log(`\nUpdating summaries for ${date}...`);
    
    // Calculate totals from curtailment records
    const totals = await db
      .select({
        totalVolume: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
        totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date));
    
    const totalVolume = parseFloat(totals[0].totalVolume || '0');
    const totalPayment = parseFloat(totals[0].totalPayment || '0');
    
    console.log(`Daily totals: ${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)}`);

    // Update daily summary
    await db.insert(dailySummaries).values({
      summaryDate: date,
      totalCurtailedEnergy: totalVolume.toString(),
      totalPayment: totalPayment.toString()
    }).onConflictDoUpdate({
      target: [dailySummaries.summaryDate],
      set: {
        totalCurtailedEnergy: totalVolume.toString(),
        totalPayment: totalPayment.toString()
      }
    });
    console.log(`✓ Updated daily summary`);

    // Update monthly summary
    const yearMonth = date.substring(0, 7);
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
      console.log(`✓ Updated monthly summary for ${yearMonth}`);
    }

    // Update yearly summary
    const year = date.substring(0, 4);
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
      console.log(`✓ Updated yearly summary for ${year}`);
    }
  } catch (error) {
    console.error(`Error updating summaries for ${date}:`, error);
  }
}

async function main() {
  try {
    console.log(`Starting fix for missing periods for ${TARGET_DATE}`);
    
    // 1. Find missing periods
    const missingPeriods = await findMissingPeriods(TARGET_DATE);
    
    if (missingPeriods.length === 0) {
      console.log('No missing periods found. Exiting.');
      process.exit(0);
    }
    
    // 2. Process missing periods
    await processMissingPeriods(TARGET_DATE, missingPeriods);
    
    console.log('\nDone! Missing periods have been added.');
    console.log('You may want to run the Bitcoin calculation update next:');
    console.log(`npx tsx server/services/bitcoinService.ts ${TARGET_DATE} ${TARGET_DATE}`);
    
    process.exit(0);
  } catch (error) {
    console.error('Error in main function:', error);
    process.exit(1);
  }
}

// Run the script
main();