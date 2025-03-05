import { fetchBidsOffers } from "./server/services/elexon";
import { processSingleDay } from "./server/services/bitcoinService";
import { db } from "@db";
import { curtailmentRecords, dailySummaries } from "@db/schema";
import { eq, sql } from "drizzle-orm";

const targetDate = "2025-03-04";
const minerModels = ["S19J_PRO", "S9", "M20S"];

// Function to get a list of periods that exist in the database
async function getExistingPeriods(date: string): Promise<Set<number>> {
  const results = await db
    .select({ period: curtailmentRecords.settlementPeriod })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date))
    .groupBy(curtailmentRecords.settlementPeriod);

  return new Set(results.map(r => r.period));
}

// Function to get a set of wind farm IDs from the mapping file
async function loadWindFarmIds(): Promise<Set<string>> {
  try {
    console.log('Loading BMU mapping using existing records...');
    
    // Get existing farm IDs from the database as a fallback approach
    const existingFarmResults = await db
      .select({ farmId: curtailmentRecords.farmId })
      .from(curtailmentRecords)
      .groupBy(curtailmentRecords.farmId);
    
    const farmIds = new Set(existingFarmResults.map(r => r.farmId));
    console.log(`Loaded ${farmIds.size} wind farm BMU IDs from existing records`);
    
    return farmIds;
  } catch (error) {
    console.error('Error loading BMU mapping:', error);
    throw error;
  }
}

// Process a specific period
async function processPeriod(date: string, period: number, validWindFarmIds: Set<string>): Promise<{
  volume: number;
  payment: number;
}> {
  try {
    console.log(`Processing period ${period} for ${date}`);
    const records = await fetchBidsOffers(date, period);
    
    // Filter for valid wind farm records with negative volume
    const validRecords = records.filter(record =>
      record.volume < 0 &&
      (record.soFlag || record.cadlFlag) &&
      validWindFarmIds.has(record.id)
    );

    if (validRecords.length > 0) {
      console.log(`[${date} P${period}] Processing ${validRecords.length} records`);
    } else {
      console.log(`[${date} P${period}] No valid records found`);
      return { volume: 0, payment: 0 };
    }

    let totalVolume = 0;
    let totalPayment = 0;

    for (const record of validRecords) {
      const volume = Math.abs(record.volume);
      const payment = volume * record.originalPrice;
      
      try {
        await db.insert(curtailmentRecords).values({
          settlementDate: date,
          settlementPeriod: period,
          farmId: record.id,
          leadPartyName: record.leadPartyName || 'Unknown',
          volume: record.volume.toString(), // Keep the original negative value
          payment: payment.toString(),
          originalPrice: record.originalPrice.toString(),
          finalPrice: record.finalPrice.toString(),
          soFlag: record.soFlag,
          cadlFlag: record.cadlFlag
        });

        console.log(`[${date} P${period}] Added record for ${record.id}: ${volume} MWh, £${payment}`);
        totalVolume += volume;
        totalPayment += payment;
      } catch (error) {
        console.error(`[${date} P${period}] Error inserting record for ${record.id}:`, error);
      }
    }

    console.log(`[${date} P${period}] Total: ${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)}`);
    return { volume: totalVolume, payment: totalPayment };
  } catch (error) {
    console.error(`Error processing period ${period} for date ${date}:`, error);
    return { volume: 0, payment: 0 };
  }
}

// Update the daily summary
async function updateDailySummary(date: string): Promise<void> {
  try {
    // Calculate total volume and payment from curtailment_records
    const totals = await db
      .select({
        totalVolume: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
        totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date));

    if (!totals[0].totalVolume || !totals[0].totalPayment) {
      console.log(`No valid totals for ${date}`);
      return;
    }

    const totalVolume = parseFloat(totals[0].totalVolume);
    const totalPayment = parseFloat(totals[0].totalPayment);

    // Update the daily summary
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

    console.log(`Updated daily summary for ${date}: ${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)}`);
  } catch (error) {
    console.error(`Error updating daily summary for ${date}:`, error);
  }
}

async function main() {
  console.log(`Starting fix for missing periods on ${targetDate}...`);
  
  try {
    // Get existing periods
    const existingPeriods = await getExistingPeriods(targetDate);
    console.log(`Found ${existingPeriods.size} existing periods for ${targetDate}: ${[...existingPeriods].sort((a, b) => a - b).join(', ')}`);
    
    // Load valid wind farm IDs
    const validWindFarmIds = await loadWindFarmIds();
    
    // Process missing periods
    let totalAddedVolume = 0;
    let totalAddedPayment = 0;
    
    for (let period = 1; period <= 48; period++) {
      if (!existingPeriods.has(period)) {
        console.log(`Processing missing period ${period}`);
        const result = await processPeriod(targetDate, period, validWindFarmIds);
        totalAddedVolume += result.volume;
        totalAddedPayment += result.payment;
      }
    }
    
    console.log(`Added ${totalAddedVolume.toFixed(2)} MWh and £${totalAddedPayment.toFixed(2)} from missing periods`);
    
    // Update the daily summary
    await updateDailySummary(targetDate);
    
    // Process Bitcoin calculations for each miner model
    console.log(`Updating Bitcoin calculations for ${targetDate}...`);
    for (const minerModel of minerModels) {
      await processSingleDay(targetDate, minerModel)
        .catch(error => {
          console.error(`Error processing Bitcoin calculations for ${targetDate} with ${minerModel}:`, error);
        });
    }
    
    console.log(`Successfully fixed missing periods for ${targetDate}`);
  } catch (error) {
    console.error(`Error fixing missing periods for ${targetDate}:`, error);
    process.exit(1);
  }
}

main();