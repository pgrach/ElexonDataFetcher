import { db } from "./db";
import { curtailmentRecords, dailySummaries, monthlySummaries, yearlySummaries } from "./db/schema";
import { fetchBidsOffers } from "./server/services/elexon";
import { eq, sql } from "drizzle-orm";

const TARGET_DATE = '2025-03-05';
// Process all periods
const MISSING_PERIODS = [32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48];

// Process bulk missing period records
async function processBulkMissingPeriods(date: string, periodList: number[]) {
  try {
    console.log(`\n=== Processing Missing Curtailment Data for ${date} ===\n`);
    
    // Get existing periods
    const existingPeriods = await db
      .select({
        period: curtailmentRecords.settlementPeriod,
        recordCount: sql<number>`COUNT(*)::int`,
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date))
      .groupBy(curtailmentRecords.settlementPeriod)
      .orderBy(curtailmentRecords.settlementPeriod);

    const existingPeriodSet = new Set(existingPeriods.map(p => p.period));
    
    console.log(`Currently have periods in database: ${Array.from(existingPeriodSet).join(', ')}`);
    console.log(`Missing periods to add: ${periodList.join(', ')}`);
    
    let totalAdded = 0;
    let totalVolume = 0;
    let totalPayment = 0;
    
    // Process each period in the list
    for (const period of periodList) {
      console.log(`\nProcessing period ${period} for ${date}...`);
      
      // Skip if period already exists in the database
      if (existingPeriodSet.has(period)) {
        console.log(`Period ${period} already exists in database, skipping.`);
        continue;
      }
      
      // Fetch records from API
      const apiRecords = await fetchBidsOffers(date, period);
      
      if (apiRecords.length === 0) {
        console.log(`No records found in API for period ${period}`);
        continue;
      }
      
      console.log(`Found ${apiRecords.length} records in API for period ${period}`);
      
      // Add all records from this period to the database
      let periodAddedCount = 0;
      let periodAddedVolume = 0;
      let periodAddedPayment = 0;
      
      for (const record of apiRecords) {
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
          
          periodAddedCount++;
          periodAddedVolume += volume;
          periodAddedPayment += payment;
        } catch (error) {
          console.error(`Error inserting record for ${record.id}:`, error);
        }
      }
      
      console.log(`Period ${period}: Added ${periodAddedCount} records, ${periodAddedVolume.toFixed(2)} MWh, £${periodAddedPayment.toFixed(2)}`);
      
      totalAdded += periodAddedCount;
      totalVolume += periodAddedVolume;
      totalPayment += periodAddedPayment;
    }
    
    console.log(`\n=== Summary ===`);
    console.log(`Added ${totalAdded} missing records`);
    console.log(`Total volume added: ${totalVolume.toFixed(2)} MWh`);
    console.log(`Total payment added: £${totalPayment.toFixed(2)}`);
    
    return { added: totalAdded, volume: totalVolume, payment: totalPayment };
  } catch (error) {
    console.error(`Error processing missing periods:`, error);
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

    console.log(`\n=== Updating Summaries ===`);
    console.log(`New total for ${date}: ${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)}`);

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
    
    return { success: true };
  } catch (error) {
    console.error(`Error updating summaries:`, error);
    return { success: false };
  }
}

// Trigger Bitcoin calculations update
async function triggerBitcoinCalculationUpdates(date: string) {
  try {
    const { reconcileDay } = await import('./server/services/historicalReconciliation');
    
    console.log(`\n=== Updating Bitcoin Calculations ===`);
    console.log(`Triggering Bitcoin calculation update for ${date}`);
    
    await reconcileDay(date);
    
    console.log(`Bitcoin calculations updated for ${date}`);
    
    return { success: true };
  } catch (error) {
    console.error(`Error updating Bitcoin calculations:`, error);
    return { success: false };
  }
}

async function fixMissingPeriods() {
  try {
    // First step: Process all the missing periods
    const result = await processBulkMissingPeriods(TARGET_DATE, MISSING_PERIODS);
    
    if (result.added > 0) {
      // Step 2: Update the summary tables
      await updateSummaries(TARGET_DATE);
      
      // Step 3: Trigger Bitcoin calculation updates
      await triggerBitcoinCalculationUpdates(TARGET_DATE);
    }
    
    // Step 4: Get final database state
    const finalStats = await db
      .select({
        recordCount: sql<number>`COUNT(*)::int`,
        periodCount: sql<number>`COUNT(DISTINCT settlement_period)::int`,
        totalVolume: sql<string>`SUM(ABS(volume::numeric))::text`,
        totalPayment: sql<string>`SUM(payment::numeric)::text`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    // Step 5: Get final period list
    const finalPeriods = await db
      .select({
        period: curtailmentRecords.settlementPeriod,
        recordCount: sql<number>`COUNT(*)::int`,
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE))
      .groupBy(curtailmentRecords.settlementPeriod)
      .orderBy(curtailmentRecords.settlementPeriod);
    
    console.log(`\n=== Final Database State ===`);
    console.log(`${TARGET_DATE} now has ${finalStats[0].recordCount} records across ${finalStats[0].periodCount} periods`);
    console.log(`Total volume: ${parseFloat(finalStats[0].totalVolume).toFixed(2)} MWh`);
    console.log(`Total payment: £${parseFloat(finalStats[0].totalPayment).toFixed(2)}`);
    console.log(`\nPeriods now in database: ${finalPeriods.map(p => p.period).join(', ')}`);

    if (finalStats[0].periodCount === 48) {
      console.log(`\n✅ SUCCESS: All 48 periods are now present for ${TARGET_DATE}!`);
    } else {
      console.log(`\n⚠️ WARNING: Only ${finalStats[0].periodCount} periods are present for ${TARGET_DATE}. Expected 48 periods.`);
      const missingPeriods = [];
      for (let i = 1; i <= 48; i++) {
        if (!finalPeriods.find(p => p.period === i)) {
          missingPeriods.push(i);
        }
      }
      console.log(`Missing periods: ${missingPeriods.join(', ')}`);
    }
  } catch (error) {
    console.error(`Error fixing missing periods:`, error);
  }
}

// Run the process
fixMissingPeriods();