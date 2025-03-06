import { db } from "./db";
import { curtailmentRecords, dailySummaries, monthlySummaries, yearlySummaries } from "./db/schema";
import { fetchBidsOffers } from "./server/services/elexon";
import { eq, sql } from "drizzle-orm";

const TARGET_DATE = '2025-03-05';
// Focus on the end of day periods (assuming periods > 31 might be missing)
const START_PERIOD = 32;
const END_PERIOD = 48;

// Process specific period records from the API and add missing ones to the database
async function processMissingPeriodRecords(date: string, period: number) {
  try {
    console.log(`Processing period ${period} for ${date}...`);
    
    // Fetch records from API
    const apiRecords = await fetchBidsOffers(date, period);
    
    if (apiRecords.length === 0) {
      console.log(`No records found in API for period ${period}`);
      return { added: 0, volume: 0, payment: 0 };
    }
    
    console.log(`Found ${apiRecords.length} records in API for period ${period}`);
    
    // Get existing records from database
    const existingRecords = await db
      .select({
        farmId: curtailmentRecords.farmId,
        period: curtailmentRecords.settlementPeriod,
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date))
      .where(eq(curtailmentRecords.settlementPeriod, period));
    
    // Create a map of existing DB records
    const existingRecordMap = new Map();
    for (const record of existingRecords) {
      const key = `${record.farmId}-${record.period}`;
      existingRecordMap.set(key, true);
    }
    
    // Find and add missing records
    const missingRecords = apiRecords.filter(record => {
      const key = `${record.id}-${period}`;
      return !existingRecordMap.has(key);
    });
    
    if (missingRecords.length === 0) {
      console.log(`No missing records found for period ${period}`);
      return { added: 0, volume: 0, payment: 0 };
    }
    
    console.log(`Found ${missingRecords.length} missing records for period ${period}`);
    
    // Add missing records to the database
    let addedCount = 0;
    let totalAddedVolume = 0;
    let totalAddedPayment = 0;
    
    for (const record of missingRecords) {
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
        
        console.log(`Added record for ${record.id}: ${volume.toFixed(2)} MWh, £${payment.toFixed(2)}`);
        addedCount++;
        totalAddedVolume += volume;
        totalAddedPayment += payment;
      } catch (error) {
        console.error(`Error inserting record for ${record.id}:`, error);
      }
    }
    
    return { 
      added: addedCount, 
      volume: totalAddedVolume,
      payment: totalAddedPayment
    };
  } catch (error) {
    console.error(`Error processing period ${period}:`, error);
    return { added: 0, volume: 0, payment: 0 };
  }
}

// Update daily, monthly, and yearly summaries
async function updateSummaries(date: string) {
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

    console.log(`Updated daily summary for ${date}: ${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)}`);

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
      
      console.log(`Updated monthly summary for ${yearMonth}`);
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
      
      console.log(`Updated yearly summary for ${year}`);
    }
  } catch (error) {
    console.error(`Error updating summaries:`, error);
  }
}

async function addMissingData() {
  try {
    console.log(`\n=== Adding Missing Curtailment Data for ${TARGET_DATE} ===\n`);

    // Get current DB periods to confirm what's missing
    const existingPeriods = await db
      .select({
        period: curtailmentRecords.settlementPeriod,
        recordCount: sql<number>`COUNT(*)::int`,
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE))
      .groupBy(curtailmentRecords.settlementPeriod)
      .orderBy(curtailmentRecords.settlementPeriod);

    const existingPeriodSet = new Set(existingPeriods.map(p => p.period));
    
    console.log(`Existing periods in database: ${Array.from(existingPeriodSet).join(', ')}`);
    
    let totalAdded = 0;
    let totalVolume = 0;
    let totalPayment = 0;

    // Process each period
    for (let period = START_PERIOD; period <= END_PERIOD; period++) {
      const result = await processMissingPeriodRecords(TARGET_DATE, period);
      totalAdded += result.added;
      totalVolume += result.volume;
      totalPayment += result.payment;
    }

    console.log(`\n=== Summary ===`);
    console.log(`Added ${totalAdded} missing records`);
    console.log(`Total volume added: ${totalVolume.toFixed(2)} MWh`);
    console.log(`Total payment added: £${totalPayment.toFixed(2)}`);

    // Update the summaries
    if (totalAdded > 0) {
      await updateSummaries(TARGET_DATE);
    }

    // Get final periods to confirm what we have now
    const finalPeriods = await db
      .select({
        period: curtailmentRecords.settlementPeriod,
        recordCount: sql<number>`COUNT(*)::int`,
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE))
      .groupBy(curtailmentRecords.settlementPeriod)
      .orderBy(curtailmentRecords.settlementPeriod);

    console.log(`\nFinal periods in database after update: ${finalPeriods.map(p => p.period).join(', ')}`);
    
    // Get final totals
    const finalStats = await db
      .select({
        recordCount: sql<number>`COUNT(*)::int`,
        periodCount: sql<number>`COUNT(DISTINCT settlement_period)::int`,
        totalVolume: sql<string>`SUM(ABS(volume::numeric))::text`,
        totalPayment: sql<string>`SUM(payment::numeric)::text`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log(`\nFinal database state for ${TARGET_DATE}:`);
    console.log(`${finalStats[0].recordCount} records across ${finalStats[0].periodCount} periods`);
    console.log(`Total volume: ${Number(finalStats[0].totalVolume).toFixed(2)} MWh`);
    console.log(`Total payment: £${Number(finalStats[0].totalPayment).toFixed(2)}`);

  } catch (error) {
    console.error(`Error adding missing data:`, error);
  }
}

// Run the process
addMissingData();