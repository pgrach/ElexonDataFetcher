import { db } from "@db";
import { curtailmentRecords, dailySummaries } from "@db/schema";
import { fetchBidsOffers } from "./elexon";
import { eq } from "drizzle-orm";

export async function processDailyCurtailment(date: string): Promise<void> {
  let totalVolume = 0;
  let totalPayment = 0;
  let recordsProcessed = 0;

  console.log(`Starting to process ${date}, fetching data for 48 settlement periods...`);

  for (let period = 1; period <= 48; period++) {
    try {
      const records = await fetchBidsOffers(date, period);

      // Filter and process records exactly like reference implementation
      const validRecords = records.filter(record => 
        record.volume < 0 && // Only negative volumes (curtailment)
        record.soFlag &&     // System operator flagged
        (record.id.startsWith('T_') || record.id.startsWith('E_')) // Wind farm BMUs
      );

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
  } catch (error) {
    console.error(`Error updating daily summary for ${date}:`, error);
    throw error;
  }
}