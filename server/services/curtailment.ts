import { db } from "@db";
import { curtailmentRecords, dailySummaries, monthlySummaries } from "@db/schema";
import { fetchBidsOffers } from "./elexon";
import { eq, sql } from "drizzle-orm";
import fs from "fs/promises";
import path from "path";
import { format } from "date-fns";

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

    // Create Set of wind farm BMU IDs for efficient lookup
    windFarmBmuIds = new Set(
      bmuMapping
        .filter((bmu: any) => bmu.fuelType === "WIND")
        .map((bmu: any) => bmu.elexonBmUnit)
    );

    console.log(`Loaded ${windFarmBmuIds.size} wind farm IDs from mapping`);
    return windFarmBmuIds;
  } catch (error) {
    console.error('Error loading BMU mapping:', error);
    throw error;
  }
}

async function updateMonthlySummary(date: string): Promise<void> {
  const yearMonth = date.substring(0, 7); // Extract YYYY-MM from YYYY-MM-DD

  try {
    // Calculate monthly totals from daily_summaries
    const monthlyTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(${dailySummaries.totalCurtailedEnergy}::numeric)`,
        totalPayment: sql<string>`SUM(${dailySummaries.totalPayment}::numeric)`
      })
      .from(dailySummaries)
      .where(sql`date_trunc('month', ${dailySummaries.summaryDate}::date) = date_trunc('month', ${date}::date)`);

    const totals = monthlyTotals[0];

    if (totals.totalCurtailedEnergy === null || totals.totalPayment === null) {
      console.log(`No daily summaries found for ${yearMonth}, skipping monthly summary update`);
      return;
    }

    // Update monthly summary
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

    console.log(`Updated monthly summary for ${yearMonth}:`, {
      totalCurtailedEnergy: totals.totalCurtailedEnergy,
      totalPayment: totals.totalPayment
    });
  } catch (error) {
    console.error(`Error updating monthly summary for ${yearMonth}:`, error);
    throw error;
  }
}

export async function processDailyCurtailment(date: string): Promise<void> {
  let totalVolume = 0;
  let totalPayment = 0;
  let recordsProcessed = 0;

  console.log(`Starting to process ${date}, fetching data for 48 settlement periods...`);

  const validWindFarmIds = await loadWindFarmIds();

  for (let period = 1; period <= 48; period++) {
    try {
      const records = await fetchBidsOffers(date, period);

      // Log raw data for debugging
      console.log(`[${date} P${period}] Processing ${records.length} records`);

      // Filter records using same criteria as reference implementation
      const validRecords = records.filter(record => 
        record.volume < 0 && // Only negative volumes (curtailment)
        record.soFlag && // System operator flagged
        validWindFarmIds.has(record.id) // Check if BMU is a wind farm using mapping
      );

      console.log(`[${date} P${period}] Found ${validRecords.length} valid curtailment records`);

      for (const record of validRecords) {
        try {
          // Following reference implementation logic:
          // 1. Volume comes as negative from API, store absolute value
          // 2. Payment = |Volume| * Price * -1
          const volume = Math.abs(record.volume);
          const payment = volume * record.originalPrice * -1;

          await db.insert(curtailmentRecords).values({
            settlementDate: date,
            settlementPeriod: period,
            farmId: record.id,
            volume: volume.toString(),
            payment: payment.toString(),
            originalPrice: record.originalPrice.toString(),
            finalPrice: record.finalPrice.toString(),
            soFlag: record.soFlag,
            cadlFlag: record.cadlFlag
          });

          totalVolume += volume;
          totalPayment += payment;
          recordsProcessed++;

          console.log(`[${date} P${period}] Processed record:`, {
            farm: record.id,
            volume,
            originalPrice: record.originalPrice,
            payment,
            soFlag: record.soFlag,
          });
        } catch (error) {
          console.error(`Error processing record for ${date} period ${period}:`, error);
          console.error('Record data:', JSON.stringify(record, null, 2));
        }
      }

      if (period % 12 === 0) {
        console.log(`Progress update for ${date}: Completed ${period}/48 periods`);
        console.log(`Records processed: ${recordsProcessed}`);
        console.log(`Running totals: ${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)}`);
      }
    } catch (error) {
      console.error(`Error processing period ${period} for date ${date}:`, error);
      continue;
    }

    // Add delay between API calls to avoid rate limiting
    await new Promise(resolve => setTimeout(resolve, 2000));
  }

  // Update daily summary
  try {
    console.log(`\nUpdating daily summary for ${date}:`);
    console.log(`Total records processed: ${recordsProcessed}`);
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

    console.log(`Successfully updated daily summary for ${date}`);

    // Update monthly summary after daily summary is updated
    await updateMonthlySummary(date);
  } catch (error) {
    console.error(`Error updating daily summary for ${date}:`, error);
    throw error;
  }
}