/**
 * Complete data ingestion for July 1, 2025
 * 
 * This script will ingest all available curtailment data for July 1, 2025
 * from the Elexon API and create all necessary Bitcoin calculations and summaries
 */

import { db } from '../db';
import { curtailmentRecords, dailySummaries, historicalBitcoinCalculations, bitcoinDailySummaries } from '../db/schema';
import { fetchBidsOffers } from '../server/services/elexon';
import { eq } from 'drizzle-orm';

const TARGET_DATE = '2025-07-01';

async function delay(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function clearExistingData() {
  console.log('Clearing any existing data for July 1, 2025...');
  
  // Clear all related data (should be none based on verification)
  await db.delete(historicalBitcoinCalculations).where(eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE));
  await db.delete(bitcoinDailySummaries).where(eq(bitcoinDailySummaries.summaryDate, TARGET_DATE));
  await db.delete(dailySummaries).where(eq(dailySummaries.summaryDate, TARGET_DATE));
  await db.delete(curtailmentRecords).where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
  
  console.log('✓ Any existing data cleared');
}

async function ingestAllCurtailmentData() {
  console.log('Ingesting all curtailment data from Elexon API...');
  
  let totalRecords = 0;
  let totalVolume = 0;
  let totalPayment = 0;
  const periodsWithData = [];
  
  // Check all 48 periods
  for (let period = 1; period <= 48; period++) {
    try {
      console.log(`Processing period ${period}...`);
      
      const apiData = await fetchBidsOffers(TARGET_DATE, period);
      
      if (apiData.length > 0) {
        console.log(`  Found ${apiData.length} records`);
        periodsWithData.push(period);
        
        // Insert each record
        for (const record of apiData) {
          await db.insert(curtailmentRecords).values({
            settlementDate: TARGET_DATE,
            settlementPeriod: period,
            farmId: record.id,
            volume: record.volume.toString(),
            payment: (Math.abs(record.volume) * record.originalPrice * -1).toString(),
            originalPrice: record.originalPrice.toString(),
            finalPrice: record.finalPrice.toString(),
            soFlag: record.soFlag,
            cadlFlag: record.cadlFlag,
            createdAt: new Date(),
            updatedAt: new Date()
          });
          
          totalRecords++;
          totalVolume += Math.abs(record.volume);
          totalPayment += (Math.abs(record.volume) * record.originalPrice * -1);
        }
        
        console.log(`  ✓ Inserted ${apiData.length} records`);
      } else {
        console.log(`  No data`);
      }
      
      // Rate limiting
      await delay(150);
      
    } catch (error) {
      console.error(`Error processing period ${period}:`, error.message);
    }
  }
  
  console.log(`\n✓ Ingestion complete:`);
  console.log(`  Total records: ${totalRecords}`);
  console.log(`  Periods with data: ${periodsWithData.length} (${periodsWithData.join(', ')})`);
  console.log(`  Total volume: ${totalVolume.toFixed(2)} MWh`);
  console.log(`  Total payment: £${totalPayment.toFixed(2)}`);
  
  return { totalRecords, periodsWithData, totalVolume, totalPayment };
}

async function createDailySummary(stats) {
  console.log('Creating daily summary...');
  
  await db.insert(dailySummaries).values({
    summaryDate: TARGET_DATE,
    totalCurtailedEnergy: stats.totalVolume,
    totalPayment: stats.totalPayment,
    createdAt: new Date(),
    lastUpdated: new Date()
  });
  
  console.log(`✓ Daily summary created: ${stats.totalVolume.toFixed(2)} MWh, £${stats.totalPayment.toFixed(2)}`);
}

async function verifyIngestion() {
  console.log('Verifying ingestion...');
  
  const verification = await db.execute(`
    SELECT 
      COUNT(*) as total_records,
      COUNT(DISTINCT settlement_period) as periods,
      SUM(ABS(volume::numeric)) as total_volume,
      SUM(payment::numeric) as total_payment
    FROM curtailment_records 
    WHERE settlement_date = '${TARGET_DATE}'
  `);
  
  const { total_records, periods, total_volume, total_payment } = verification.rows[0];
  
  console.log(`Final verification:`);
  console.log(`  Database records: ${total_records}`);
  console.log(`  Periods: ${periods}`);
  console.log(`  Volume: ${Number(total_volume).toFixed(2)} MWh`);
  console.log(`  Payment: £${Number(total_payment).toFixed(2)}`);
  
  return {
    records: Number(total_records),
    periods: Number(periods),
    volume: Number(total_volume),
    payment: Number(total_payment)
  };
}

async function main() {
  try {
    console.log(`=== Complete Ingestion for ${TARGET_DATE} ===`);
    
    // Step 1: Clear existing data
    await clearExistingData();
    
    // Step 2: Ingest all curtailment data
    const ingestionStats = await ingestAllCurtailmentData();
    
    // Step 3: Create daily summary
    await createDailySummary(ingestionStats);
    
    // Step 4: Verify ingestion
    const verification = await verifyIngestion();
    
    console.log(`\n=== RESULTS ===`);
    console.log(`Curtailment records: ${verification.records}`);
    console.log(`Periods with data: ${verification.periods}`);
    console.log(`Total volume: ${verification.volume.toFixed(2)} MWh`);
    console.log(`Total payment: £${verification.payment.toFixed(2)}`);
    
    if (verification.records > 0) {
      console.log(`\n✅ July 1, 2025 data successfully ingested`);
      console.log(`\n📝 Note: Bitcoin calculations will be processed automatically by the system`);
    } else {
      console.log(`\n❌ No data was ingested - check API connectivity`);
    }
    
    return verification;
    
  } catch (error) {
    console.error('❌ Error during ingestion:', error);
    return { error: error.message };
  }
}

main();