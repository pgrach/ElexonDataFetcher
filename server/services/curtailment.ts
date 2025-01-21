import { db } from "@db";
import { curtailmentRecords, dailySummaries } from "@db/schema";
import { fetchBidsOffers } from "./elexon";
import { eq } from "drizzle-orm";
import fs from "fs/promises";
import path from "path";
import { sql } from 'drizzle-orm';

// Load BMU mapping from the correct location
const BMU_MAPPING_PATH = path.join(process.cwd(), '..', 'data', 'bmuMapping.json');

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

export async function processDailyCurtailment(date: string): Promise<void> {
  let totalVolume = 0;
  let totalPayment = 0;
  let recordsProcessed = 0;

  console.log(`Starting to process ${date}, fetching data for 48 settlement periods...`);

  const validWindFarmIds = await loadWindFarmIds();

  // First, clear any existing records for this date to avoid duplicates
  await db.delete(curtailmentRecords).where(eq(curtailmentRecords.settlementDate, date));
  console.log(`Cleared existing records for ${date}`);

  for (let period = 1; period <= 48; period++) {
    try {
      const records = await fetchBidsOffers(date, period);

      // Log raw data for debugging
      console.log(`[${date} P${period}] Processing ${records.length} records`);

      // Filter records using relaxed criteria
      const validRecords = records.filter(record => 
        record.volume < 0 && // Only negative volumes (curtailment)
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

          if (recordsProcessed % 100 === 0) {
            console.log(`[${date} P${period}] Processed ${recordsProcessed} records`);
          }
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

  // Update daily summary with recalculated totals from curtailment_records
  try {
    console.log(`\nUpdating daily summary for ${date}:`);

    // Recalculate totals from curtailment_records
    const totals = await db
      .select({
        totalVolume: sql<string>`SUM(${curtailmentRecords.volume}::numeric)`,
        totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date));

    const finalVolume = Number(totals[0]?.totalVolume || 0);
    const finalPayment = Number(totals[0]?.totalPayment || 0);

    console.log(`Final totals from curtailment_records:`);
    console.log(`Total volume: ${finalVolume.toFixed(2)} MWh`);
    console.log(`Total payment: £${finalPayment.toFixed(2)}`);

    await db.insert(dailySummaries).values({
      summaryDate: date,
      totalCurtailedEnergy: finalVolume.toString(),
      totalPayment: finalPayment.toString()
    }).onConflictDoUpdate({
      target: [dailySummaries.summaryDate],
      set: {
        totalCurtailedEnergy: finalVolume.toString(),
        totalPayment: finalPayment.toString()
      }
    });

    console.log(`Successfully updated daily summary for ${date}`);
  } catch (error) {
    console.error(`Error updating daily summary for ${date}:`, error);
    throw error;
  }
}