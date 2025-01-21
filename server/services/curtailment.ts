import { db } from "@db";
import { curtailmentRecords, dailySummaries } from "@db/schema";
import { fetchBidsOffers } from "./elexon";
import { eq } from "drizzle-orm";
import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from 'url';

// Use __dirname to get correct project root path
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const BMU_MAPPING_PATH = path.resolve(__dirname, '..', '..', 'data', 'bmuMapping.json');

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
    // Log more details about the error and file path
    console.error('BMU Mapping Path:', BMU_MAPPING_PATH);
    console.error('Current working directory:', process.cwd());
    throw error;
  }
}

export async function processDailyCurtailment(date: string): Promise<void> {
  let totalVolume = 0;
  let totalPayment = 0;
  let recordsProcessed = 0;

  console.log(`Starting to process ${date}, fetching data for 48 settlement periods...`);

  const validWindFarmIds = await loadWindFarmIds();

  // Process all 48 periods concurrently in smaller batches to avoid overwhelming the API
  const periods = Array.from({ length: 48 }, (_, i) => i + 1);
  const BATCH_SIZE = 6; // Process 6 periods at a time

  for (let i = 0; i < periods.length; i += BATCH_SIZE) {
    const batch = periods.slice(i, i + BATCH_SIZE);

    // Process batch of periods concurrently
    const results = await Promise.allSettled(
      batch.map(async (period) => {
        try {
          const records = await fetchBidsOffers(date, period);

          // Filter records using same criteria as reference implementation
          const validRecords = records.filter(record =>
            record.volume < 0 && // Only negative volumes (curtailment)
            record.soFlag && // System operator flagged
            validWindFarmIds.has(record.id) // Check if BMU is a wind farm using mapping
          );

          console.log(`[${date} P${period}] Found ${validRecords.length} valid curtailment records`);

          for (const record of validRecords) {
            try {
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
                payment,
                soFlag: record.soFlag,
              });
            } catch (error) {
              console.error(`Error processing record for ${date} period ${period}:`, error);
            }
          }

          return { period, success: true, records: validRecords.length };
        } catch (error) {
          console.error(`Error processing period ${period} for date ${date}:`, error);
          return { period, success: false, error };
        }
      })
    );

    // Log batch progress
    const successful = results.filter(r => r.status === 'fulfilled' && r.value.success).length;
    const recordCount = results
      .filter((r): r is PromiseFulfilledResult<{ records: number }> =>
        r.status === 'fulfilled' && r.value.success
      )
      .reduce((sum, r) => sum + r.value.records, 0);

    if (successful < batch.length) {
      console.log(`Warning: Only ${successful}/${batch.length} periods processed successfully in current batch`);
    }

    console.log(`Progress: Completed periods ${i + 1}-${Math.min(i + BATCH_SIZE, 48)} (${recordCount} records in this batch)`);
    console.log(`Running totals: ${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)}`);

    // Small delay between batches to avoid rate limiting
    if (i + BATCH_SIZE < periods.length) {
      await new Promise(resolve => setTimeout(resolve, 500)); // 500ms delay between batches
    }
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
  } catch (error) {
    console.error(`Error updating daily summary for ${date}:`, error);
    throw error;
  }
}