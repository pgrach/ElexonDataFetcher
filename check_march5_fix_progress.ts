import { db } from "./db";
import { curtailmentRecords } from "./db/schema";
import { eq, sql } from "drizzle-orm";

const TARGET_DATE = '2025-03-05';

async function checkFixProgress() {
  try {
    console.log(`\n=== Checking Fix Progress for ${TARGET_DATE} ===\n`);
    
    // Get periods and their record counts
    const periodStats = await db
      .select({
        period: curtailmentRecords.settlementPeriod,
        count: sql<number>`count(*)::int`,
        totalVolume: sql<string>`sum(abs(volume::numeric))::text`,
        totalPayment: sql<string>`sum(payment::numeric)::text`,
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE))
      .groupBy(curtailmentRecords.settlementPeriod)
      .orderBy(curtailmentRecords.settlementPeriod);
    
    // List all periods
    console.log(`\nDetailed Period Stats:`);
    console.log('Period | Records | Volume (MWh) | Payment (£)');
    console.log('-------|---------|--------------|------------');
    
    for (const period of periodStats) {
      console.log(`${period.period.toString().padStart(6, ' ')} | ${period.count.toString().padStart(7, ' ')} | ${parseFloat(period.totalVolume).toFixed(2).padStart(12, ' ')} | ${parseFloat(period.totalPayment).toFixed(2).padStart(12, ' ')}`);
    }
    
    // Find missing periods
    const existingPeriods = new Set(periodStats.map(p => p.period));
    const missingPeriods = [];
    
    for (let i = 1; i <= 48; i++) {
      if (!existingPeriods.has(i)) {
        missingPeriods.push(i);
      }
    }
    
    // Calculate totals
    const totalStats = await db
      .select({
        recordCount: sql<number>`count(*)::int`,
        periodCount: sql<number>`count(distinct settlement_period)::int`,
        totalVolume: sql<string>`sum(abs(volume::numeric))::text`,
        totalPayment: sql<string>`sum(payment::numeric)::text`,
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log(`\nDatabase Total: ${totalStats[0].recordCount} records across ${totalStats[0].periodCount} periods`);
    console.log(`Total Volume: ${parseFloat(totalStats[0].totalVolume).toFixed(2)} MWh`);
    console.log(`Total Payment: £${parseFloat(totalStats[0].totalPayment).toFixed(2)}`);
    
    if (missingPeriods.length > 0) {
      console.log(`\nMissing periods: ${missingPeriods.join(', ')}`);
      
      // Generate a focused script only for the remaining missing periods
      console.log(`\n=== Script for Remaining Periods ===`);
      console.log(`\nTo fix the remaining periods, create a new script with the following code:`);
      console.log(`\nimport { db } from "./db";`);
      console.log(`import { curtailmentRecords } from "./db/schema";`);
      console.log(`import { fetchBidsOffers } from "./server/services/elexon";`);
      console.log(`import { eq } from "drizzle-orm";`);
      console.log(`\nconst TARGET_DATE = '${TARGET_DATE}';`);
      console.log(`const MISSING_PERIODS = [${missingPeriods.join(', ')}];`);
      console.log(`\nasync function addRemainingPeriods() {`);
      console.log(`  try {`);
      console.log(`    console.log(\`Processing remaining missing periods: \${MISSING_PERIODS.join(', ')}\`);`);
      console.log(`    let totalAdded = 0;`);
      console.log(`    \n    for (const period of MISSING_PERIODS) {`);
      console.log(`      console.log(\`\\nProcessing period \${period}...\`);`);
      console.log(`      const apiRecords = await fetchBidsOffers(TARGET_DATE, period);`);
      console.log(`      console.log(\`Found \${apiRecords.length} records in API for period \${period}\`);`);
      console.log(`      \n      let recordsAdded = 0;`);
      console.log(`      for (const record of apiRecords) {`);
      console.log(`        const volume = Math.abs(record.volume);`);
      console.log(`        const payment = volume * record.originalPrice;`);
      console.log(`        \n        await db.insert(curtailmentRecords).values({`);
      console.log(`          settlementDate: TARGET_DATE,`);
      console.log(`          settlementPeriod: period,`);
      console.log(`          farmId: record.id,`);
      console.log(`          leadPartyName: record.leadPartyName || 'Unknown',`);
      console.log(`          volume: record.volume.toString(),`);
      console.log(`          payment: payment.toString(),`);
      console.log(`          originalPrice: record.originalPrice.toString(),`);
      console.log(`          finalPrice: record.finalPrice.toString(),`);
      console.log(`          soFlag: record.soFlag,`);
      console.log(`          cadlFlag: record.cadlFlag`);
      console.log(`        });`);
      console.log(`        recordsAdded++;`);
      console.log(`      }`);
      console.log(`      console.log(\`Added \${recordsAdded} records for period \${period}\`);`);
      console.log(`      totalAdded += recordsAdded;`);
      console.log(`    }`);
      console.log(`    console.log(\`\\nTotal records added: \${totalAdded}\`);`);
      console.log(`  } catch (error) {`);
      console.log(`    console.error('Error adding remaining periods:', error);`);
      console.log(`  }`);
      console.log(`}`);
      console.log(`\naddRemainingPeriods();`);
    } else {
      console.log(`\n✅ All 48 periods are present for ${TARGET_DATE}!`);
      
      // Generate verification query
      console.log(`\n=== Verification Query ===`);
      console.log(`To verify the Bitcoin calculations for this date, use the following function call:`);
      console.log(`\nimport { reconcileDay } from "./server/services/historicalReconciliation";`);
      console.log(`\n// Trigger Bitcoin calculation update for ${TARGET_DATE}`);
      console.log(`async function verifyBitcoinCalculations() {`);
      console.log(`  await reconcileDay('${TARGET_DATE}');`);
      console.log(`  console.log('Bitcoin calculations updated for ${TARGET_DATE}');`);
      console.log(`}`);
      console.log(`\nverifyBitcoinCalculations();`);
    }
  } catch (error) {
    console.error(`Error checking fix progress:`, error);
  }
}

// Run the check
checkFixProgress();