import { db } from "@db";
import { curtailmentRecords, dailySummaries, monthlySummaries } from "@db/schema";
import { fetchBidsOffers } from "./elexon";
import { eq, sql } from "drizzle-orm";
import fs from "fs/promises";
import path from "path";

const BMU_MAPPING_PATH = path.join(process.cwd(), 'server', 'data', 'bmuMapping.json');

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

async function updateMonthlySummary(date: string): Promise<void> {
  const yearMonth = date.substring(0, 7);

  try {
    const monthlyTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`COALESCE(SUM(${dailySummaries.totalCurtailedEnergy}::numeric), '0')`,
        totalPayment: sql<string>`COALESCE(SUM(${dailySummaries.totalPayment}::numeric), '0')`
      })
      .from(dailySummaries)
      .where(sql`date_trunc('month', ${dailySummaries.summaryDate}::date) = date_trunc('month', ${date}::date)`);

    const totals = monthlyTotals[0];

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

    console.log(`Updated monthly summary for ${yearMonth} with energy: ${totals.totalCurtailedEnergy} MWh, payment: £${totals.totalPayment}`);
  } catch (error) {
    console.error(`Error updating monthly summary for ${yearMonth}:`, error);
    throw error;
  }
}

export async function processDailyCurtailment(date: string): Promise<void> {
  const BATCH_SIZE = 12;
  let totalVolume = 0;
  let totalPayment = 0;

  try {
    console.log(`\nProcessing daily curtailment for ${date}`);
    const validWindFarmIds = await loadWindFarmIds();

    // Process in batches to avoid overloading
    for (let startPeriod = 1; startPeriod <= 48; startPeriod += BATCH_SIZE) {
      const endPeriod = Math.min(startPeriod + BATCH_SIZE - 1, 48);
      console.log(`Processing periods ${startPeriod}-${endPeriod}`);

      const periodResults = await Promise.all(
        Array.from({ length: endPeriod - startPeriod + 1 }, async (_, i) => {
          const period = startPeriod + i;
          try {
            const records = await fetchBidsOffers(date, period);
            const validRecords = records.filter(record =>
              record.volume < 0 &&
              (record.soFlag || record.cadlFlag) &&
              validWindFarmIds.has(record.id)
            );

            console.log(`Found ${validRecords.length} valid records for period ${period}`);

            const periodTotal = {
              volume: 0,
              payment: 0
            };

            for (const record of validRecords) {
              const volume = Math.abs(record.volume);
              const payment = volume * record.originalPrice;

              try {
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
                }).onConflictDoUpdate({
                  target: [
                    curtailmentRecords.settlementDate,
                    curtailmentRecords.settlementPeriod,
                    curtailmentRecords.farmId
                  ],
                  set: {
                    volume: volume.toString(),
                    payment: payment.toString(),
                    originalPrice: record.originalPrice.toString(),
                    finalPrice: record.finalPrice.toString(),
                    soFlag: record.soFlag,
                    cadlFlag: record.cadlFlag,
                    leadPartyName: bmuLeadPartyMap?.get(record.id) || 'Unknown'
                  }
                });

                periodTotal.volume += volume;
                periodTotal.payment += payment;
              } catch (error) {
                console.error(`Error inserting record for period ${period}:`, error);
              }
            }

            console.log(`Period ${period} totals: ${periodTotal.volume.toFixed(2)} MWh, £${periodTotal.payment.toFixed(2)}`);
            return periodTotal;
          } catch (error) {
            console.error(`Error processing period ${period}:`, error);
            return { volume: 0, payment: 0 };
          }
        })
      );

      const batchTotal = periodResults.reduce((acc, curr) => ({
        volume: acc.volume + curr.volume,
        payment: acc.payment + curr.payment
      }), { volume: 0, payment: 0 });

      totalVolume += batchTotal.volume;
      totalPayment += batchTotal.payment;

      console.log(`\nBatch ${startPeriod}-${endPeriod} completed:`);
      console.log(`Cumulative total: ${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)}`);
      await new Promise(resolve => setTimeout(resolve, 500));
    }

    // Update daily summary
    console.log(`\nUpdating daily summary for ${date}:`);
    console.log(`Total volume: ${totalVolume.toFixed(2)} MWh`);
    console.log(`Total payment: £${totalPayment.toFixed(2)}`);

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
    console.log(`Successfully updated all summaries for ${date}`);

  } catch (error) {
    console.error(`Fatal error processing daily curtailment for ${date}:`, error);
    throw error;
  }
}

export async function updateLeadPartyNames(): Promise<void> {
  try {
    await loadWindFarmIds();
    if (!bmuLeadPartyMap) {
      throw new Error('BMU lead party mapping not initialized');
    }

    for (const [bmuId, leadPartyName] of Array.from(bmuLeadPartyMap.entries())) {
      await db.update(curtailmentRecords)
        .set({ leadPartyName })
        .where(eq(curtailmentRecords.farmId, bmuId));
      console.log(`Updated lead party name for BMU ${bmuId} to ${leadPartyName}`);
    }
    console.log('Completed updating lead party names');
  } catch (error) {
    console.error('Error updating lead party names:', error);
    throw error;
  }
}