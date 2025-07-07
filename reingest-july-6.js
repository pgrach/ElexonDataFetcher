import { processDailyCurtailment } from './server/services/curtailmentService.ts';
import { db } from './db/index.ts';
import { curtailmentRecords, dailySummaries } from './db/schema.ts';
import { eq, sql } from 'drizzle-orm';

async function reingestJuly6() {
  console.log('=== Re-ingesting July 6 Data Using Proven Service ===\n');
  
  const date = '2025-07-06';
  
  try {
    // Check current state
    console.log('Before re-ingestion:');
    const beforeStats = await db
      .select({
        recordCount: sql<number>`COUNT(*)`,
        periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
        totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
        totalPayment: sql<string>`SUM(payment::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date));
    
    console.log(`Records: ${beforeStats[0]?.recordCount || 0}`);
    console.log(`Periods: ${beforeStats[0]?.periodCount || 0}`);
    console.log(`Volume: ${Number(beforeStats[0]?.totalVolume || 0).toFixed(2)} MWh`);
    console.log(`Payment: £${Number(beforeStats[0]?.totalPayment || 0).toFixed(2)}`);
    
    // Use the proven processDailyCurtailment service
    console.log('\nStarting re-ingestion using processDailyCurtailment()...');
    await processDailyCurtailment(date);
    console.log('Re-ingestion complete!');
    
    // Check after re-ingestion
    console.log('\nAfter re-ingestion:');
    const afterStats = await db
      .select({
        recordCount: sql<number>`COUNT(*)`,
        periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
        totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
        totalPayment: sql<string>`SUM(payment::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date));
    
    console.log(`Records: ${afterStats[0]?.recordCount || 0}`);
    console.log(`Periods: ${afterStats[0]?.periodCount || 0}`);
    console.log(`Volume: ${Number(afterStats[0]?.totalVolume || 0).toFixed(2)} MWh`);
    console.log(`Payment: £${Number(afterStats[0]?.totalPayment || 0).toFixed(2)}`);
    
    // Fix payment signs to ensure they're positive (subsidies paid TO wind farms)
    console.log('\nFixing payment signs to ensure positive values...');
    await db.execute(
      sql`UPDATE curtailment_records SET payment = ABS(payment::numeric) WHERE settlement_date = ${date}`
    );
    
    await db.execute(
      sql`UPDATE daily_summaries SET total_payment = ABS(total_payment::numeric) WHERE summary_date = ${date}`
    );
    
    // Final verification
    console.log('\nFinal verification:');
    const finalStats = await db
      .select({
        recordCount: sql<number>`COUNT(*)`,
        periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
        totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
        totalPayment: sql<string>`SUM(payment::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date));
    
    console.log(`Records: ${finalStats[0]?.recordCount || 0}`);
    console.log(`Periods: ${finalStats[0]?.periodCount || 0}`);
    console.log(`Volume: ${Number(finalStats[0]?.totalVolume || 0).toFixed(2)} MWh`);
    console.log(`Payment: £${Number(finalStats[0]?.totalPayment || 0).toFixed(2)} (POSITIVE ✓)`);
    
    // Check daily summary
    const dailySummary = await db
      .select()
      .from(dailySummaries)
      .where(eq(dailySummaries.summaryDate, date));
    
    if (dailySummary.length > 0) {
      console.log(`\nDaily Summary:`);
      console.log(`Energy: ${Number(dailySummary[0].totalCurtailedEnergy).toFixed(2)} MWh`);
      console.log(`Payment: £${Number(dailySummary[0].totalPayment).toFixed(2)} (POSITIVE ✓)`);
    }
    
    console.log('\n✅ July 6 data successfully re-ingested and verified!');
    console.log('All payment values are now positive (subsidies paid TO wind farms)');
    
  } catch (error) {
    console.error('Error during re-ingestion:', error);
    throw error;
  }
}

reingestJuly6().catch(error => {
  console.error('Re-ingestion failed:', error);
  process.exit(1);
});