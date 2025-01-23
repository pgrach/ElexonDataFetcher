import { db } from "@db";
import { curtailmentRecords, dailySummaries, monthlySummaries } from "@db/schema";
import { fetchBidsOffers } from "./elexon";
import { eq, sql } from "drizzle-orm";
import fs from "fs/promises";
import path from "path";

// Load BMU mapping from the correct location
const BMU_MAPPING_PATH = path.join(process.cwd(), 'server', 'data', 'bmuMapping.json');

let windFarmBmuIds: Set<string> | null = null;
let bmuLeadPartyMap: Map<string, string> | null = null;

async function loadWindFarmIds(): Promise<Set<string>> {
  try {
    // Always reload in development to catch mapping updates
    if (process.env.NODE_ENV === 'development' || windFarmBmuIds === null || bmuLeadPartyMap === null) {
      console.log('Loading BMU mapping from:', BMU_MAPPING_PATH);

      const mappingContent = await fs.readFile(BMU_MAPPING_PATH, 'utf8');
      const bmuMapping = JSON.parse(mappingContent);

      console.log(`Loaded ${bmuMapping.length} BMU mappings`);

      windFarmBmuIds = new Set(
        bmuMapping
          .filter((bmu: any) => bmu.fuelType === "WIND")
          .map((bmu: any) => bmu.elexonBmUnit)
      );

      // Initialize the lead party mapping
      bmuLeadPartyMap = new Map(
        bmuMapping
          .filter((bmu: any) => bmu.fuelType === "WIND")
          .map((bmu: any) => [bmu.elexonBmUnit, bmu.leadPartyName || 'Unknown'])
      );

      console.log(`Found ${windFarmBmuIds.size} wind farm BMUs`);
      console.log('Sample lead party mappings:', 
        Array.from(bmuLeadPartyMap.entries()).slice(0, 3));
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

async function updateMonthlySummary(date: string): Promise<void> {
  const yearMonth = date.substring(0, 7);

  try {
    const monthlyTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(${dailySummaries.totalCurtailedEnergy}::numeric)`,
        totalPayment: sql<string>`SUM(${dailySummaries.totalPayment}::numeric)`
      })
      .from(dailySummaries)
      .where(sql`date_trunc('month', ${dailySummaries.summaryDate}::date) = date_trunc('month', ${date}::date)`);

    const totals = monthlyTotals[0];

    if (!totals.totalCurtailedEnergy || !totals.totalPayment) {
      return;
    }

    await db.insert(monthlySummaries).values({
      yearMonth,
      totalCurtailedEnergy: totals.totalCurtailedEnergy,
      totalPayment: totals.totalPayment,
      updatedAt: new Date()
    }).onConflictDoUpdate({
      target: [monthlySummaries.yearMonth],
      set: {
        totalCurtailedEnergy: totals.totalCurtailedEnergy,
        totalPayment: totals.totalPayment,
        updatedAt: new Date()
      }
    });

  } catch (error) {
    console.error(`Error updating monthly summary for ${yearMonth}:`, error);
    throw error;
  }
}

export async function processDailyCurtailment(date: string): Promise<void> {
  const BATCH_SIZE = 12;
  const validWindFarmIds = await loadWindFarmIds();
  let totalVolume = 0;
  let totalPayment = 0;

  for (let startPeriod = 1; startPeriod <= 48; startPeriod += BATCH_SIZE) {
    const endPeriod = Math.min(startPeriod + BATCH_SIZE - 1, 48);
    const periodPromises = [];

    for (let period = startPeriod; period <= endPeriod; period++) {
      periodPromises.push((async () => {
        try {
          const records = await fetchBidsOffers(date, period);
          const validRecords = records.filter(record =>
            record.volume < 0 &&
            (record.soFlag || record.cadlFlag) &&
            validWindFarmIds.has(record.id)
          );

          const periodResults = await Promise.all(
            validRecords.map(async record => {
              const volume = Math.abs(record.volume);
              const payment = volume * record.originalPrice;

              await db.insert(curtailmentRecords).values({
                settlementDate: date,
                settlementPeriod: period,
                farmId: record.id,
                leadPartyName: bmuLeadPartyMap?.get(record.id) || 'Unknown',
                volume: volume.toString(),
                payment: payment.toString(),
                originalPrice: record.originalPrice.toString(),
                finalPrice: record.finalPrice.toString(),
                soFlag: record.soFlag,
                cadlFlag: record.cadlFlag
              });

              return { volume, payment };
            })
          );

          return periodResults.reduce((acc, curr) => ({
            volume: acc.volume + curr.volume,
            payment: acc.payment + curr.payment
          }), { volume: 0, payment: 0 });
        } catch (error) {
          console.error(`Error processing period ${period} for date ${date}:`, error);
          return { volume: 0, payment: 0 };
        }
      })());
    }

    const batchResults = await Promise.all(periodPromises);
    for (const result of batchResults) {
      totalVolume += result.volume;
      totalPayment += result.payment;
    }

    await new Promise(resolve => setTimeout(resolve, 500));
  }

  try {
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

    await updateMonthlySummary(date);

  } catch (error) {
    console.error(`Error updating daily summary for ${date}:`, error);
    throw error;
  }
}