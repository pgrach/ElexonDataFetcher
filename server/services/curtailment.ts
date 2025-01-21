import { db } from "@db";
import { curtailmentRecords, dailySummaries } from "@db/schema";
import { fetchBidsOffers, fetchMultiplePeriods } from "./elexon";
import { eq } from "drizzle-orm";
import fs from "fs/promises";
import path from "path";

// Load BMU mapping from the correct location
const BMU_MAPPING_PATH = path.join(process.cwd(), 'server', 'data', 'bmuMapping.json');
const BATCH_SIZE = 12; // Process 12 periods at a time (1/4th of a day)

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

  console.log(`Starting to process ${date}, fetching data in batches...`);
  const validWindFarmIds = await loadWindFarmIds();

  // Process the day in 4 batches (12 periods each)
  const batches = [
    { start: 1, end: 12 },
    { start: 13, end: 24 },
    { start: 25, end: 36 },
    { start: 37, end: 48 }
  ];

  for (const batch of batches) {
    try {
      console.log(`\nProcessing periods ${batch.start}-${batch.end} for ${date}`);
      const records = await fetchMultiplePeriods(date, batch.start, batch.end);

      // Filter and process records
      const validRecords = records.filter(record => 
        record.volume < 0 && 
        record.soFlag && 
        validWindFarmIds.has(record.id)
      );

      console.log(`Found ${validRecords.length} valid curtailment records in batch`);

      for (const record of validRecords) {
        try {
          const volume = Math.abs(record.volume);
          const payment = volume * record.originalPrice * -1;

          await db.insert(curtailmentRecords).values({
            settlementDate: date,
            settlementPeriod: record.settlementPeriod,
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

        } catch (error) {
          console.error(`Error processing record for ${date}:`, error);
          console.error('Record data:', JSON.stringify(record, null, 2));
        }
      }

      // Log progress after each batch
      console.log(`Progress update for ${date}:`);
      console.log(`Processed ${batch.end}/48 periods`);
      console.log(`Records processed: ${recordsProcessed}`);
      console.log(`Running totals: ${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)}`);

    } catch (error) {
      console.error(`Error processing batch for ${date}:`, error);
      // Continue with next batch even if one fails
      continue;
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