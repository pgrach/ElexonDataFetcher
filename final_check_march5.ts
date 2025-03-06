import { db } from "./db";
import { curtailmentRecords } from "./db/schema";
import { eq, sql } from "drizzle-orm";

const TARGET_DATE = '2025-03-05';

async function checkFinalState() {
  try {
    console.log(`\n=== Checking Final State for ${TARGET_DATE} ===\n`);
    
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
    
    console.log(`Database Total: ${totalStats[0].recordCount} records across ${totalStats[0].periodCount} periods`);
    console.log(`Total Volume: ${parseFloat(totalStats[0].totalVolume).toFixed(2)} MWh`);
    console.log(`Total Payment: £${parseFloat(totalStats[0].totalPayment).toFixed(2)}`);
    
    // Find missing periods
    const existingPeriods = new Set(periodStats.map(p => p.period));
    const missingPeriods = [];
    
    for (let i = 1; i <= 48; i++) {
      if (!existingPeriods.has(i)) {
        missingPeriods.push(i);
      }
    }
    
    if (missingPeriods.length > 0) {
      console.log(`\nMissing periods: ${missingPeriods.join(', ')}`);
      console.log(`\n⚠️ WARNING: Still missing ${missingPeriods.length} periods for ${TARGET_DATE}!`);
    } else {
      console.log(`\n✅ SUCCESS: All 48 periods are now present for ${TARGET_DATE}!`);
    }
    
    // Get Bitcoin calculation status
    const bitcoinStats = await db
      .execute(sql`
        SELECT 
          COUNT(*) as calculation_count,
          COUNT(DISTINCT miner_model) as model_count,
          COUNT(DISTINCT farm_id) as farm_count
        FROM historical_bitcoin_calculations
        WHERE settlement_date = ${TARGET_DATE}
      `);
    
    if (bitcoinStats.length > 0) {
      const calcCount = Number(bitcoinStats[0].calculation_count);
      const modelCount = Number(bitcoinStats[0].model_count);
      const farmCount = Number(bitcoinStats[0].farm_count);
      
      console.log(`\nBitcoin Calculations: ${calcCount} records for ${modelCount} miner models across ${farmCount} farms`);
      
      // Calculate expected number of calculations
      const expectedCalcs = totalStats[0].recordCount * 3; // 3 miner models 
      if (calcCount < expectedCalcs) {
        console.log(`⚠️ WARNING: Expected approximately ${expectedCalcs} Bitcoin calculations, found ${calcCount}`);
      } else {
        console.log(`✅ Bitcoin calculations appear to be complete!`);
      }
    } else {
      console.log(`⚠️ WARNING: No Bitcoin calculations found for ${TARGET_DATE}`);
    }
    
    // Check daily summary
    const dailySummary = await db
      .execute(sql`
        SELECT 
          total_curtailed_energy,
          total_payment
        FROM daily_summaries
        WHERE summary_date = ${TARGET_DATE}
      `);
    
    if (dailySummary.length > 0) {
      const summaryVolume = parseFloat(dailySummary[0].total_curtailed_energy);
      const summaryPayment = parseFloat(dailySummary[0].total_payment);
      
      console.log(`\nDaily Summary: ${summaryVolume.toFixed(2)} MWh, £${summaryPayment.toFixed(2)}`);
      
      // Compare with calculated totals
      const dbVolume = parseFloat(totalStats[0].totalVolume);
      const dbPayment = parseFloat(totalStats[0].totalPayment);
      
      if (Math.abs(summaryVolume - dbVolume) > 0.01 || Math.abs(summaryPayment - dbPayment) > 0.01) {
        console.log(`⚠️ WARNING: Daily summary doesn't match database totals!`);
        console.log(`Database: ${dbVolume.toFixed(2)} MWh, £${dbPayment.toFixed(2)}`);
        console.log(`Summary:  ${summaryVolume.toFixed(2)} MWh, £${summaryPayment.toFixed(2)}`);
      } else {
        console.log(`✅ Daily summary matches database totals!`);
      }
    } else {
      console.log(`⚠️ WARNING: No daily summary found for ${TARGET_DATE}`);
    }
    
    // Generate remaining action items if needed
    if (missingPeriods.length > 0) {
      console.log(`\n=== Required Actions ===`);
      console.log(`Need to add the following periods: ${missingPeriods.join(', ')}`);
      
      // Only generate code for up to 3 periods to avoid timeouts
      const periodsToFix = missingPeriods.slice(0, 3);
      console.log(`\nCreate a script to add just these periods: ${periodsToFix.join(', ')}`);
      console.log(`import { db } from "./db";`);
      console.log(`import { curtailmentRecords } from "./db/schema";`);
      console.log(`import { fetchBidsOffers } from "./server/services/elexon";`);
      console.log(`\nasync function addFinalPeriods() {`);
      console.log(`  const TARGET_DATE = '${TARGET_DATE}';`);
      console.log(`  const PERIODS = [${periodsToFix.join(', ')}];`);
      console.log(`  \n  // Add each period`);
      console.log(`  for (const period of PERIODS) {`);
      console.log(`    const records = await fetchBidsOffers(TARGET_DATE, period);`);
      console.log(`    console.log(\`Adding \${records.length} records for period \${period}...\`);`);
      console.log(`    \n    for (const record of records) {`);
      console.log(`      const volume = Math.abs(record.volume);`);
      console.log(`      const payment = volume * record.originalPrice;`);
      console.log(`      \n      await db.insert(curtailmentRecords).values({`);
      console.log(`        settlementDate: TARGET_DATE,`);
      console.log(`        settlementPeriod: period,`);
      console.log(`        farmId: record.id,`);
      console.log(`        leadPartyName: record.leadPartyName || 'Unknown',`);
      console.log(`        volume: record.volume.toString(),`);
      console.log(`        payment: payment.toString(),`);
      console.log(`        originalPrice: record.originalPrice.toString(),`);
      console.log(`        finalPrice: record.finalPrice.toString(),`);
      console.log(`        soFlag: record.soFlag,`);
      console.log(`        cadlFlag: record.cadlFlag`);
      console.log(`      });`);
      console.log(`    }`);
      console.log(`    console.log(\`Completed period \${period}\`);`);
      console.log(`  }`);
      console.log(`  console.log('Done!');`);
      console.log(`}`);
      console.log(`\naddFinalPeriods();`);
    } else if (bitcoinStats.length === 0 || Number(bitcoinStats[0].calculation_count) === 0) {
      console.log(`\n=== Required Actions ===`);
      console.log(`Need to trigger Bitcoin calculations for ${TARGET_DATE}`);
      console.log(`\nimport { reconcileDay } from "./server/services/historicalReconciliation";`);
      console.log(`\nasync function updateBitcoinCalculations() {`);
      console.log(`  await reconcileDay('${TARGET_DATE}');`);
      console.log(`  console.log('Bitcoin calculations updated for ${TARGET_DATE}');`);
      console.log(`}`);
      console.log(`\nupdateBitcoinCalculations();`);
    } else {
      console.log(`\n=== Data Reconciliation Complete ===`);
      console.log(`${TARGET_DATE} data is now fully reconciled with all 48 periods and Bitcoin calculations!`);
      console.log(`\nNext steps:`);
      console.log(`1. Update dataUpdater.ts to ensure full day reconciliation in the future`);
      console.log(`2. Check other historical dates for similar issues`);
      console.log(`3. Implement a comprehensive monitoring solution to detect missing periods`);
    }
  } catch (error) {
    console.error(`Error checking final state:`, error);
  }
}

// Run the check
checkFinalState();