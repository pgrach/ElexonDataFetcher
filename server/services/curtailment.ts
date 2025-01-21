import { db } from "@db";
import { curtailmentRecords, dailySummaries } from "@db/schema";
import { fetchBidsOffers, fetchMultiplePeriods } from "./elexon";
import { eq } from "drizzle-orm";
import fs from "fs/promises";
import path from "path";

// Load BMU mapping from the correct location
const BMU_MAPPING_PATH = path.join(process.cwd(), 'server', 'data', 'bmuMapping.json');
const BATCH_SIZE = 2; // Process 2 periods at a time to be extra careful
const BATCH_DELAY = 5000; // 5 seconds between batches

let windFarmBmuIds: Set<string> | null = null;
let recordsByPeriod: Record<number, { volume: number, payment: number }> = {};

async function loadWindFarmIds(): Promise<Set<string>> {
  if (windFarmBmuIds !== null) {
    return windFarmBmuIds;
  }

  try {
    const mappingContent = await fs.readFile(BMU_MAPPING_PATH, 'utf8');
    const bmuMapping = JSON.parse(mappingContent);
    windFarmBmuIds = new Set(bmuMapping.map((bmu: any) => bmu.elexonBmUnit));
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
  let totalValidRecords = 0;
  let totalInvalidRecords = 0;
  let recordsByPeriod: Record<number, { volume: number, payment: number }> = {};

  console.log(`\nStarting to process ${date}`);
  console.log('Loading wind farm mapping...');
  const validWindFarmIds = await loadWindFarmIds();

  // Process the day in small batches
  const batches = Array.from({ length: Math.ceil(48 / BATCH_SIZE) }, (_, i) => ({
    start: i * BATCH_SIZE + 1,
    end: Math.min((i + 1) * BATCH_SIZE, 48)
  }));

  console.log(`Processing ${batches.length} batches of ${BATCH_SIZE} periods each`);

  for (const batch of batches) {
    try {
      console.log(`\nProcessing periods ${batch.start}-${batch.end} for ${date}`);
      const records = await fetchMultiplePeriods(date, batch.start, batch.end);

      // Log raw record counts
      const rawCounts = {
        total: records.length,
        withNegativeVolume: records.filter(r => r.volume < 0).length,
        withSOFlag: records.filter(r => r.soFlag === true).length,
        windFarms: records.filter(r => validWindFarmIds.has(r.id)).length
      };
      console.log(`Raw records for periods ${batch.start}-${batch.end}:`, rawCounts);

      // Filter and process records with detailed validation
      const validRecords = records.filter(record => {
        const isWindFarm = validWindFarmIds.has(record.id);
        const hasNegativeVolume = record.volume < 0;
        const isSOFlagged = record.soFlag === true;

        // Log validation details for records with negative volume
        if (hasNegativeVolume && !isWindFarm) {
          console.log(`Record from non-wind farm BMU:`, {
            id: record.id,
            period: record.settlementPeriod,
            volume: record.volume,
            soFlag: record.soFlag
          });
        }

        if (hasNegativeVolume && isWindFarm && !isSOFlagged) {
          console.log(`Wind farm record without SO flag:`, {
            id: record.id,
            period: record.settlementPeriod,
            volume: record.volume,
            soFlag: record.soFlag
          });
        }

        const isValid = isWindFarm && hasNegativeVolume && isSOFlagged;

        if (!isValid && hasNegativeVolume) {
          totalInvalidRecords++;
        }

        return isValid;
      });

      totalValidRecords += validRecords.length;

      if (validRecords.length > 0) {
        console.log(`Found ${validRecords.length} valid curtailment records`);

        // Log sample valid record
        console.log('Sample valid record:', JSON.stringify(validRecords[0], null, 2));
      }

      for (const record of validRecords) {
        try {
          const volume = Math.abs(record.volume);
          const payment = volume * record.originalPrice * -1;

          // Store records by period for validation
          recordsByPeriod[record.settlementPeriod] = recordsByPeriod[record.settlementPeriod] || { volume: 0, payment: 0 };
          recordsByPeriod[record.settlementPeriod].volume += volume;
          recordsByPeriod[record.settlementPeriod].payment += payment;

          await db.insert(curtailmentRecords).values({
            settlementDate: date,
            settlementPeriod: record.settlementPeriod,
            farmId: record.id,
            volume: volume.toString(),
            payment: payment.toString(),
            originalPrice: record.originalPrice.toString(),
            finalPrice: record.finalPrice.toString(),
            soFlag: record.soFlag,
            cadlFlag: record.cadlFlag,
          });

          totalVolume += volume;
          totalPayment += payment;
          recordsProcessed++;

          console.log(`Processed record:`, {
            period: record.settlementPeriod,
            farmId: record.id,
            volume: volume.toFixed(2),
            payment: payment.toFixed(2)
          });

        } catch (error) {
          console.error(`Error processing record for ${date}:`, error);
          console.error('Record data:', JSON.stringify(record, null, 2));
        }
      }

      // Log progress after each batch
      console.log(`\nBatch progress for ${date}:`);
      console.log(`Processed periods ${batch.start}-${batch.end} of 48`);
      console.log(`Valid records in batch: ${validRecords.length}`);
      console.log(`Running totals: ${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)}`);

      // Add delay between batches
      if (batch.end < 48) {
        console.log(`Waiting ${BATCH_DELAY}ms before next batch...`);
        await new Promise(resolve => setTimeout(resolve, BATCH_DELAY));
      }

    } catch (error) {
      console.error(`Error processing batch for ${date}:`, error);
      // Continue with next batch even if one fails
      continue;
    }
  }

  // Update daily summary with validation
  try {
    console.log(`\nProcessing summary for ${date}:`);
    console.log(`Records processed: ${recordsProcessed}`);
    console.log(`Valid records: ${totalValidRecords}`);
    console.log(`Invalid records: ${totalInvalidRecords}`);

    // Print period-by-period breakdown if we have any data
    if (Object.keys(recordsByPeriod).length > 0) {
      console.log('\nBreakdown by settlement period:');
      Object.entries(recordsByPeriod)
        .sort(([a], [b]) => Number(a) - Number(b))
        .forEach(([period, data]) => {
          console.log(`Period ${period}: ${data.volume.toFixed(2)} MWh, £${data.payment.toFixed(2)}`);
        });
    }

    console.log(`\nFinal totals:`);
    console.log(`Total volume: ${totalVolume.toFixed(2)} MWh`);
    console.log(`Total payment: £${totalPayment.toFixed(2)}`);

    // Only update if we have valid data
    if (totalVolume > 0 || totalPayment !== 0) {
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
    } else {
      console.log(`No valid curtailment data found for ${date}`);
      throw new Error('No valid curtailment data found');
    }

  } catch (error) {
    console.error(`Error updating daily summary for ${date}:`, error);
    throw error;
  }
}