import { db } from "./db";
import { curtailmentRecords, dailySummaries, monthlySummaries, yearlySummaries } from "./db/schema";
import { fetchBidsOffers } from "./server/services/elexon";
import { eq, sql } from "drizzle-orm";

const TARGET_DATE = '2025-03-05';
const MISSING_PERIODS = [37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48];

async function addRemainingPeriods() {
  try {
    console.log(`Processing remaining missing periods: ${MISSING_PERIODS.join(', ')}`);
    let totalAdded = 0;
    let totalVolume = 0;
    let totalPayment = 0;
    
    for (const period of MISSING_PERIODS) {
      console.log(`\nProcessing period ${period}...`);
      const apiRecords = await fetchBidsOffers(TARGET_DATE, period);
      console.log(`Found ${apiRecords.length} records in API for period ${period}`);
      
      let periodVolume = 0;
      let periodPayment = 0;
      let recordsAdded = 0;
      
      for (const record of apiRecords) {
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
        
        recordsAdded++;
        periodVolume += volume;
        periodPayment += payment;
      }
      
      console.log(`Added ${recordsAdded} records for period ${period} (${periodVolume.toFixed(2)} MWh, £${periodPayment.toFixed(2)})`);
      totalAdded += recordsAdded;
      totalVolume += periodVolume;
      totalPayment += periodPayment;
    }
    
    console.log(`\nTotal records added: ${totalAdded}`);
    console.log(`Total volume added: ${totalVolume.toFixed(2)} MWh`);
    console.log(`Total payment added: £${totalPayment.toFixed(2)}`);
    
    // Update the summaries
    await updateSummaries(TARGET_DATE);
    
    // Trigger Bitcoin calculation updates
    await triggerBitcoinCalculationUpdates(TARGET_DATE);
    
    // Check final state
    await checkFinalState(TARGET_DATE);
    
  } catch (error) {
    console.error('Error adding remaining periods:', error);
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

// Check final database state
async function checkFinalState(date: string) {
  try {
    // Get final database state
    const finalStats = await db
      .select({
        recordCount: sql<number>`COUNT(*)::int`,
        periodCount: sql<number>`COUNT(DISTINCT settlement_period)::int`,
        totalVolume: sql<string>`SUM(ABS(volume::numeric))::text`,
        totalPayment: sql<string>`SUM(payment::numeric)::text`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date));
    
    // Get final period list
    const finalPeriods = await db
      .select({
        period: curtailmentRecords.settlementPeriod,
        recordCount: sql<number>`COUNT(*)::int`,
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date))
      .groupBy(curtailmentRecords.settlementPeriod)
      .orderBy(curtailmentRecords.settlementPeriod);
    
    console.log(`\n=== Final Database State ===`);
    console.log(`${date} now has ${finalStats[0].recordCount} records across ${finalStats[0].periodCount} periods`);
    console.log(`Total volume: ${parseFloat(finalStats[0].totalVolume).toFixed(2)} MWh`);
    console.log(`Total payment: £${parseFloat(finalStats[0].totalPayment).toFixed(2)}`);
    
    if (finalStats[0].periodCount === 48) {
      console.log(`\n✅ SUCCESS: All 48 periods are now present for ${date}!`);
    } else {
      console.log(`\n⚠️ WARNING: Only ${finalStats[0].periodCount} periods are present for ${date}. Expected 48 periods.`);
      
      // Find any remaining missing periods
      const existingPeriods = new Set(finalPeriods.map(p => p.period));
      const missingPeriods = [];
      
      for (let i = 1; i <= 48; i++) {
        if (!existingPeriods.has(i)) {
          missingPeriods.push(i);
        }
      }
      
      if (missingPeriods.length > 0) {
        console.log(`Missing periods: ${missingPeriods.join(', ')}`);
      }
    }
  } catch (error) {
    console.error('Error checking final state:', error);
  }
}

// Execute the function
addRemainingPeriods();