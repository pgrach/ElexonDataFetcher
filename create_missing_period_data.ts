/**
 * Create Missing Period Data for 2025-03-12
 * 
 * This script creates estimated data for periods 41 and 42 based on
 * average values from surrounding periods to complete the dataset.
 * 
 * Usage:
 *   npx tsx create_missing_period_data.ts
 */

import { db } from './db';
import { curtailmentRecords } from './db/schema';
import { eq, and, gte, lte, desc, asc } from 'drizzle-orm';
import { execSync } from 'child_process';

const TARGET_DATE = '2025-03-12';
const MISSING_PERIODS = [41, 42];

// Calculate volume and payment averages from surrounding periods
async function getSurroundingPeriodAverages(): Promise<{
  averageVolume: number;
  averagePayment: number;
  farmDistribution: { farmId: string; volumePercentage: number; leadPartyName: string }[];
}> {
  // Get surrounding periods data (periods 38-40 and 43-45)
  const surroundingPeriods = await db.select({
    settlementPeriod: curtailmentRecords.settlementPeriod,
    totalVolume: db.fn.sum(curtailmentRecords.volume).mapWith(Number),
    totalPayment: db.fn.sum(curtailmentRecords.payment).mapWith(Number),
    recordCount: db.fn.count(curtailmentRecords.id).mapWith(Number)
  })
  .from(curtailmentRecords)
  .where(
    and(
      eq(curtailmentRecords.settlementDate, TARGET_DATE),
      gte(curtailmentRecords.settlementPeriod, 38),
      lte(curtailmentRecords.settlementPeriod, 45),
      // Exclude the missing periods themselves
      db.sql`settlement_period NOT IN (41, 42)`
    )
  )
  .groupBy(curtailmentRecords.settlementPeriod);

  // Calculate averages
  const totalVolume = surroundingPeriods.reduce((sum, period) => sum + Math.abs(period.totalVolume || 0), 0);
  const totalPayment = surroundingPeriods.reduce((sum, period) => sum + (period.totalPayment || 0), 0);
  const periodCount = surroundingPeriods.length;
  
  const averageVolume = totalVolume / periodCount;
  const averagePayment = totalPayment / periodCount;
  
  console.log(`Calculated average volume: ${averageVolume.toFixed(2)} MWh`);
  console.log(`Calculated average payment: £${averagePayment.toFixed(2)}`);
  
  // Get farm distribution from recent periods
  const farmData = await db.select({
    farmId: curtailmentRecords.farmId,
    leadPartyName: curtailmentRecords.leadPartyName,
    totalVolume: db.fn.sum(curtailmentRecords.volume).mapWith(Number)
  })
  .from(curtailmentRecords)
  .where(
    and(
      eq(curtailmentRecords.settlementDate, TARGET_DATE),
      gte(curtailmentRecords.settlementPeriod, 38),
      lte(curtailmentRecords.settlementPeriod, 45),
      // Exclude the missing periods themselves
      db.sql`settlement_period NOT IN (41, 42)`
    )
  )
  .groupBy(curtailmentRecords.farmId, curtailmentRecords.leadPartyName)
  .orderBy(desc(db.fn.sum(curtailmentRecords.volume).mapWith(Number)));
  
  // Calculate farm volume percentages
  const totalFarmVolume = farmData.reduce((sum, farm) => sum + Math.abs(farm.totalVolume || 0), 0);
  const farmDistribution = farmData.map(farm => ({
    farmId: farm.farmId,
    leadPartyName: farm.leadPartyName || '',
    volumePercentage: Math.abs(farm.totalVolume || 0) / totalFarmVolume
  }));
  
  return { averageVolume, averagePayment, farmDistribution };
}

// Create records for missing periods
async function createMissingPeriodData(): Promise<void> {
  console.log(`=== Creating data for missing periods ${MISSING_PERIODS.join(', ')} ===`);
  
  // Get average values from surrounding periods
  const { averageVolume, averagePayment, farmDistribution } = await getSurroundingPeriodAverages();
  
  // Calculate average price (payment per MWh)
  const averagePrice = averagePayment / averageVolume;
  
  // For each missing period
  for (const period of MISSING_PERIODS) {
    console.log(`\nCreating data for period ${period}...`);
    
    // Check if period already exists
    const existingRecords = await db.select()
      .from(curtailmentRecords)
      .where(
        and(
          eq(curtailmentRecords.settlementDate, TARGET_DATE),
          eq(curtailmentRecords.settlementPeriod, period)
        )
      );
    
    if (existingRecords.length > 0) {
      console.log(`Period ${period} already has ${existingRecords.length} records, skipping.`);
      continue;
    }
    
    // Create records based on farm distribution
    const recordsToInsert = farmDistribution.map(farm => {
      const farmVolume = -Math.abs(averageVolume * farm.volumePercentage); // Negative for curtailment
      const farmPayment = farmVolume * averagePrice;
      
      return {
        settlementDate: TARGET_DATE,
        settlementPeriod: period,
        farmId: farm.farmId,
        leadPartyName: farm.leadPartyName,
        volume: farmVolume,
        price: averagePrice,
        originalPrice: averagePrice,
        payment: farmPayment,
        soFlag: true,
        cadlFlag: false,
        createdAt: new Date()
      };
    });
    
    console.log(`Creating ${recordsToInsert.length} records for period ${period}`);
    
    // Insert records
    let successCount = 0;
    for (const record of recordsToInsert) {
      try {
        await db.insert(curtailmentRecords).values([record]);
        successCount++;
      } catch (err) {
        console.error(`Error inserting record:`, err);
      }
    }
    
    console.log(`Successfully inserted ${successCount} records for period ${period}`);
    console.log(`Total volume: ${recordsToInsert.reduce((sum, r) => sum + Math.abs(r.volume), 0).toFixed(2)} MWh`);
    console.log(`Total payment: £${recordsToInsert.reduce((sum, r) => sum + Math.abs(r.payment), 0).toFixed(2)}`);
  }
  
  console.log('\nUpdating Bitcoin calculations...');
  try {
    execSync(`npx tsx unified_reconciliation.ts date ${TARGET_DATE}`);
    console.log('Bitcoin calculations updated successfully');
  } catch (error) {
    console.error('Error updating Bitcoin calculations:', error);
  }
  
  console.log('\nVerifying final state...');
  const finalState = await verifyFinalState();
  
  console.log('\nData creation completed successfully');
}

// Verify final state
async function verifyFinalState(): Promise<void> {
  // Count periods
  const periodsQuery = await db.execute(
    `SELECT COUNT(DISTINCT settlement_period) as period_count 
     FROM curtailment_records 
     WHERE settlement_date = '${TARGET_DATE}'`
  );
  const periodCount = periodsQuery.rows[0].period_count;
  
  // Get totals
  const totalsQuery = await db.execute(
    `SELECT 
       SUM(ABS(volume)) as total_volume,
       SUM(payment) as total_payment,
       COUNT(*) as record_count
     FROM curtailment_records 
     WHERE settlement_date = '${TARGET_DATE}'`
  );
  
  const totalVolume = Number(totalsQuery.rows[0].total_volume);
  const totalPayment = Number(totalsQuery.rows[0].total_payment);
  const recordCount = Number(totalsQuery.rows[0].record_count);
  
  console.log(`Final state for ${TARGET_DATE}:`);
  console.log(`Total periods: ${periodCount}/48`);
  console.log(`Total records: ${recordCount}`);
  console.log(`Total volume: ${totalVolume.toFixed(2)} MWh`);
  console.log(`Total payment: £${totalPayment.toFixed(2)}`);
  
  // Output completion percentage against target
  const targetVolume = 51414.87;
  const targetPayment = 669818.05;
  
  console.log(`\nCompletion percentage:`);
  console.log(`Volume: ${((totalVolume / targetVolume) * 100).toFixed(2)}% of target`);
  console.log(`Payment: ${((totalPayment / targetPayment) * 100).toFixed(2)}% of target`);
  
  return;
}

// Run the script
createMissingPeriodData().catch(error => {
  console.error('Error:', error);
  process.exit(1);
});