import { db } from "./db";
import { curtailmentRecords, dailySummaries, monthlySummaries, yearlySummaries } from "./db/schema";
import { fetchBidsOffers } from "./server/services/elexon";
import { eq, sql } from "drizzle-orm";

async function addFinalPeriods() {
  const TARGET_DATE = '2025-03-05';
  const PERIODS = [48];
  
  try {
    let totalAdded = 0;
    
    // Add each period
    for (const period of PERIODS) {
      const records = await fetchBidsOffers(TARGET_DATE, period);
      console.log(`Adding ${records.length} records for period ${period}...`);
      
      let periodAdded = 0;
      for (const record of records) {
        const volume = Math.abs(record.volume);
        const payment = volume * record.originalPrice;
        
        await db.insert(curtailmentRecords).values({
          settlementDate: TARGET_DATE,
          settlementPeriod: period,
          farmId: record.id,
          leadPartyName: record.leadPartyName || 'Unknown',
          volume: record.volume.toString(),
          payment: payment.toString(),
          originalPrice: record.originalPrice.toString(),
          finalPrice: record.finalPrice.toString(),
          soFlag: record.soFlag,
          cadlFlag: record.cadlFlag
        });
        
        periodAdded++;
      }
      console.log(`Completed period ${period} (Added ${periodAdded} records)`);
      totalAdded += periodAdded;
      
      // To avoid timeouts, only process 3 periods at a time
      if (PERIODS.indexOf(period) === 2) {
        console.log(`\nPausing after 3 periods to avoid timeout. Run this script again for the remaining periods.`);
        break;
      }
    }
    
    console.log(`\nTotal records added: ${totalAdded}`);
    
    // Update the daily summary
    await updateDailySummary(TARGET_DATE);
    
  } catch (error) {
    console.error('Error adding final periods:', error);
  }
}

async function updateDailySummary(date: string) {
  try {
    // Get total curtailment for the date
    const curtailmentStats = await db
      .select({
        totalVolume: sql<string>`SUM(ABS(volume::numeric))::text`,
        totalPayment: sql<string>`SUM(payment::numeric)::text`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date));

    const totalVolume = Number(curtailmentStats[0].totalVolume);
    const totalPayment = Number(curtailmentStats[0].totalPayment);

    console.log(`\n=== Updating Summaries ===`);
    console.log(`Total for ${date}: ${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)}`);

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

    console.log(`Updated daily summary for ${date}`);
    
    return true;
  } catch (error) {
    console.error('Error updating daily summary:', error);
    return false;
  }
}

addFinalPeriods();