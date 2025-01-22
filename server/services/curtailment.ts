import { db } from "@db";
import { curtailmentRecords, dailySummaries, monthlySummaries } from "@db/schema";
import { fetchBidsOffers } from "./elexon";
import { eq, sql } from "drizzle-orm";
import fs from "fs/promises";
import path from "path";

// Load BMU mapping from the correct location
const BMU_MAPPING_PATH = path.join(process.cwd(), 'server', 'data', 'bmuMapping.json');

let windFarmBmuIds: Set<string> | null = null;

async function loadWindFarmIds(): Promise<Set<string>> {
  if (windFarmBmuIds !== null) {
    return windFarmBmuIds;
  }

  try {
    const mappingContent = await fs.readFile(BMU_MAPPING_PATH, 'utf8');
    const bmuMapping = JSON.parse(mappingContent);

    windFarmBmuIds = new Set(
      bmuMapping
        .filter((bmu: any) => bmu.fuelType === "WIND")
        .map((bmu: any) => bmu.elexonBmUnit)
    );

    return windFarmBmuIds;
  } catch (error) {
    console.error('Error loading BMU mapping:', error);
    throw error;
  }
}

async function updateMonthlySummary(date: string): Promise<void> {
  const yearMonth = date.substring(0, 7);

  try {
    // Use ABS for payment aggregation to ensure positive values
    const monthlyTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(${dailySummaries.totalCurtailedEnergy}::numeric)`,
        totalPayment: sql<string>`SUM(ABS(${dailySummaries.totalPayment}::numeric))`
      })
      .from(dailySummaries)
      .where(sql`date_trunc('month', ${dailySummaries.summaryDate}::date) = date_trunc('month', ${date}::date)`);

    const totals = monthlyTotals[0];

    if (totals.totalCurtailedEnergy === null || totals.totalPayment === null) {
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
  try {
    // Delete existing records for clean re-ingestion
    await db.delete(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date));

    let totalVolume = 0;
    let totalPayment = 0;
    const validWindFarmIds = await loadWindFarmIds();
    const BATCH_SIZE = 12;

    for (let startPeriod = 1; startPeriod <= 48; startPeriod += BATCH_SIZE) {
      const endPeriod = Math.min(startPeriod + BATCH_SIZE - 1, 48);
      const periodPromises = [];

      for (let period = startPeriod; period <= endPeriod; period++) {
        periodPromises.push((async () => {
          try {
            const records = await fetchBidsOffers(date, period);

            const validRecords = records.filter(record =>
              record.volume < 0 &&  // Filter for curtailment (negative volume)
              (record.soFlag || record.cadlFlag) &&
              validWindFarmIds.has(record.id)
            );

            if (!validRecords.length) {
              return { volume: 0, payment: 0 };
            }

            // Calculate total volume for the period
            const periodVolume = validRecords.reduce((sum, record) => sum + Math.abs(record.volume), 0);

            // Use the first valid record's price for payment calculation
            const periodPrice = Math.abs(validRecords[0].originalPrice);
            const periodPayment = periodVolume * periodPrice;

            // Store individual records with the period price
            await Promise.all(validRecords.map(record => {
              const volume = Math.abs(record.volume);
              // Use the same price for all records in this period
              const payment = volume * periodPrice;

              return db.insert(curtailmentRecords).values({
                settlementDate: date,
                settlementPeriod: period,
                farmId: record.id,
                volume: volume.toString(),
                payment: payment.toString(),
                originalPrice: record.originalPrice.toString(),
                finalPrice: record.finalPrice.toString(),
                soFlag: record.soFlag,
                cadlFlag: record.cadlFlag || false
              });
            }));

            console.log(`[${date} P${period}] Records: ${validRecords.length} (${periodVolume.toFixed(2)} MWh, £${periodPayment.toFixed(2)})`);
            return { volume: periodVolume, payment: periodPayment };
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

      // Add a small delay between batches to avoid overwhelming the API
      await new Promise(resolve => setTimeout(resolve, 500));
    }

    console.log('Curtailment records totals:', {
      totalVolume: totalVolume.toString(),
      totalPayment: totalPayment.toString()
    });

    // Update daily summary with the totals
    await db.insert(dailySummaries).values({
      summaryDate: date,
      totalCurtailedEnergy: totalVolume.toString(),
      totalPayment: totalPayment.toString()  // Store total payment as positive
    }).onConflictDoUpdate({
      target: [dailySummaries.summaryDate],
      set: {
        totalCurtailedEnergy: totalVolume.toString(),
        totalPayment: totalPayment.toString()  // Update with positive value
      }
    });

    await updateMonthlySummary(date);

  } catch (error) {
    console.error(`Error updating daily summary for ${date}:`, error);
    throw error;
  }
}