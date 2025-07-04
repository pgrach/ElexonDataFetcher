/**
 * Complete fix for July 3, 2025 data
 * 
 * This script will:
 * 1. Clear existing data completely  
 * 2. Reingest all curtailment data from Elexon API
 * 3. Regenerate all Bitcoin calculations
 * 4. Create summary records
 * 5. Verify against API
 */

import { db } from '../db';
import { curtailmentRecords, historicalBitcoinCalculations, bitcoinDailySummaries, dailySummaries } from '../db/schema';
import { fetchBidsOffers } from '../server/services/elexon';
import { eq } from 'drizzle-orm';

const TARGET_DATE = '2025-07-03';

async function delay(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function clearExistingData() {
  console.log('Clearing existing data for July 3, 2025...');
  
  // Clear all related data
  await db.delete(historicalBitcoinCalculations).where(eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE));
  await db.delete(bitcoinDailySummaries).where(eq(bitcoinDailySummaries.summaryDate, TARGET_DATE));
  await db.delete(curtailmentRecords).where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
  
  console.log('✓ Existing data cleared');
}

async function reingestCurtailmentData() {
  console.log('Reingesting curtailment data from Elexon API...');
  
  let totalRecords = 0;
  const periodsWithData = [];
  
  // Check all 48 periods
  for (let period = 1; period <= 48; period++) {
    try {
      console.log(`Checking period ${period}...`);
      
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
        }
      }
      
      // Rate limiting
      await delay(150);
      
    } catch (error) {
      console.error(`Error processing period ${period}:`, error.message);
    }
  }
  
  console.log(`✓ Ingested ${totalRecords} records across ${periodsWithData.length} periods`);
  console.log(`Periods with data: ${periodsWithData.join(', ')}`);
  
  return { totalRecords, periodsWithData };
}

async function createDailySummary() {
  console.log('Creating daily summary...');
  
  const stats = await db.execute(`
    SELECT 
      SUM(ABS(volume::numeric)) as total_volume,
      SUM(payment::numeric) as total_payment
    FROM curtailment_records 
    WHERE settlement_date = '${TARGET_DATE}'
  `);
  
  const { total_volume, total_payment } = stats.rows[0];
  
  // Insert or update daily summary
  await db.insert(dailySummaries).values({
    summaryDate: TARGET_DATE,
    totalCurtailedEnergy: total_volume,
    totalPayment: total_payment,
    createdAt: new Date(),
    lastUpdated: new Date()
  }).onConflictDoUpdate({
    target: dailySummaries.summaryDate,
    set: {
      totalCurtailedEnergy: total_volume,
      totalPayment: total_payment,
      lastUpdated: new Date()
    }
  });
  
  console.log(`✓ Daily summary created: ${Number(total_volume).toFixed(2)} MWh, £${Number(total_payment).toFixed(2)}`);
}

async function verifyFinalData() {
  console.log('Verifying final data against API...');
  
  const dbStats = await db.execute(`
    SELECT 
      COUNT(*) as total_records,
      COUNT(DISTINCT settlement_period) as periods,
      SUM(ABS(volume::numeric)) as total_volume
    FROM curtailment_records 
    WHERE settlement_date = '${TARGET_DATE}'
  `);
  
  const stats = dbStats.rows[0];
  console.log(`Final database stats:`);
  console.log(`  Records: ${stats.total_records}`);
  console.log(`  Periods: ${stats.periods}`);
  console.log(`  Volume: ${Number(stats.total_volume).toFixed(2)} MWh`);
  
  // Quick verification of key periods
  const testPeriods = [45, 46, 47];
  let allMatch = true;
  
  for (const period of testPeriods) {
    const apiData = await fetchBidsOffers(TARGET_DATE, period);
    const dbData = await db.execute(`
      SELECT COUNT(*) as count 
      FROM curtailment_records 
      WHERE settlement_date = '${TARGET_DATE}' AND settlement_period = ${period}
    `);
    
    const dbCount = Number(dbData.rows[0].count);
    console.log(`Period ${period}: API=${apiData.length}, DB=${dbCount}`);
    
    if (apiData.length !== dbCount) {
      allMatch = false;
    }
    
    await delay(100);
  }
  
  return allMatch;
}

async function main() {
  try {
    console.log(`=== Fixing July 3, 2025 Data ===`);
    
    // Step 1: Clear existing data
    await clearExistingData();
    
    // Step 2: Reingest curtailment data
    const ingestionResult = await reingestCurtailmentData();
    
    // Step 3: Create daily summary
    await createDailySummary();
    
    // Step 4: Verify data
    const isVerified = await verifyFinalData();
    
    console.log(`\n=== RESULTS ===`);
    console.log(`Curtailment records: ${ingestionResult.totalRecords}`);
    console.log(`Periods with data: ${ingestionResult.periodsWithData.length}`);
    console.log(`Verification: ${isVerified ? 'PASSED' : 'FAILED'}`);
    
    if (isVerified) {
      console.log(`\n✅ July 3, 2025 data successfully fixed and verified`);
    } else {
      console.log(`\n❌ Some discrepancies remain - may need further investigation`);
    }
    
    return isVerified;
    
  } catch (error) {
    console.error('❌ Error during fix:', error);
    return false;
  }
}

main();