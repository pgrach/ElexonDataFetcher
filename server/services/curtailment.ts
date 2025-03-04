import { db } from "@db";
import { curtailmentRecords, dailySummaries, monthlySummaries, yearlySummaries } from "@db/schema";
import { fetchBidsOffers } from "./elexon";
import { eq, sql } from "drizzle-orm";
import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const BMU_MAPPING_PATH = path.join(__dirname, "../data/bmuMapping.json");

let windFarmBmuIds: Set<string> | null = null;
let bmuLeadPartyMap: Map<string, string> | null = null;

async function loadWindFarmIds(): Promise<Set<string>> {
  try {
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

export async function processDailyCurtailment(date: string): Promise<void> {
  const BATCH_SIZE = 12;
  const validWindFarmIds = await loadWindFarmIds();
  let totalVolume = 0;
  let totalPayment = 0;

  console.log(`Processing curtailment for ${date}`);

  // Clear existing records for the date to prevent partial updates
  await db.delete(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date));

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

          if (validRecords.length > 0) {
            console.log(`[${date} P${period}] Processing ${validRecords.length} records`);
          }

          const periodResults = await Promise.all(
            validRecords.map(async record => {
              const volume = Math.abs(record.volume);
              const payment = volume * record.originalPrice;

              try {
                // The payment should be a negative value to represent cost
                // Since volume is already negative, we shouldn't need to negate it again
                // Fix: Ensure payment is stored with correct sign (negative)
                const paymentValue = record.volume * record.originalPrice;
                
                await db.insert(curtailmentRecords).values({
                  settlementDate: date,
                  settlementPeriod: period,
                  farmId: record.id,
                  leadPartyName: bmuLeadPartyMap?.get(record.id) || 'Unknown',
                  volume: record.volume.toString(), // Keep the original negative value
                  payment: paymentValue.toString(), // Store with correct sign (negative)
                  originalPrice: record.originalPrice.toString(),
                  finalPrice: record.finalPrice.toString(),
                  soFlag: record.soFlag,
                  cadlFlag: record.cadlFlag
                });

                console.log(`[${date} P${period}] Added record for ${record.id}: ${volume} MWh, £${payment}`);
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
      })());
    }

    const batchResults = await Promise.all(periodPromises);
    for (const result of batchResults) {
      totalVolume += result.volume;
      totalPayment += result.payment;
    }
  }

  try {
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
    }

    console.log(`Successfully processed data for ${date}`);
  } catch (error) {
    console.error(`Error updating summaries for ${date}:`, error);
    throw error;
  }
}