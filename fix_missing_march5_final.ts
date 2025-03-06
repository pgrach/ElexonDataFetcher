/**
 * Fix Missing Data for March 5th, 2025 - Final Reconciliation
 * 
 * This script adds remaining missing records to fully reconcile 
 * the March 5th, 2025 data with the Elexon API.
 */

import { db } from './db';
import { sql } from 'drizzle-orm';
import { fetchBidsOffers } from './server/services/elexon';
import { ElexonBidOffer } from './server/types/elexon';

const TARGET_DATE = '2025-03-05';

async function addMissingRecords() {
  console.log(`Adding missing records for ${TARGET_DATE}...`);
  
  // Process all 48 periods to ensure complete data
  for (let period = 1; period <= 48; period++) {
    try {
      console.log(`Processing period ${period}...`);
      const apiRecords = await fetchBidsOffers(TARGET_DATE, period);
      const curtailedRecords = apiRecords.filter(r => r.soFlag && r.volume > 0);
      
      // Get existing records for this period from DB to compare
      const dbRecords = await db.execute(sql`
        SELECT farm_id, volume, payment
        FROM curtailment_records 
        WHERE settlement_date = ${TARGET_DATE} AND settlement_period = ${period}
      `);
      
      const dbRecordMap = new Map<string, { volume: number, payment: number }>();
      
      // Convert the query result to an array we can iterate over
      const recordsArray = Array.isArray(dbRecords) ? dbRecords : [];
      
      for (const record of recordsArray) {
        dbRecordMap.set(record.farm_id, { 
          volume: parseFloat(record.volume), 
          payment: parseFloat(record.payment) 
        });
      }
      
      let periodVolume = 0;
      let periodPayment = 0;
      let addedCount = 0;
      
      // Find and add missing records
      for (const record of curtailedRecords) {
        if (!record.bmUnit) continue;
        
        const dbRecord = dbRecordMap.get(record.bmUnit);
        const volume = record.volume;
        const payment = record.volume * record.finalPrice;
        
        // If record doesn't exist in DB or has significantly different values
        if (!dbRecord || 
            Math.abs(dbRecord.volume - volume) > 0.01 || 
            Math.abs(dbRecord.payment - payment) > 0.01) {
          
          // Insert the missing record
          await db.execute(sql`
            INSERT INTO curtailment_records (
              settlement_date, settlement_period, farm_id, volume, payment, 
              original_price, final_price, lead_party_name, created_at
            ) VALUES (
              ${TARGET_DATE}, ${period}, ${record.bmUnit}, ${volume}, ${payment},
              ${record.originalPrice}, ${record.finalPrice}, ${record.leadPartyName || null}, NOW()
            )
            ON CONFLICT (settlement_date, settlement_period, farm_id) DO UPDATE
            SET volume = ${volume}, payment = ${payment}, 
                original_price = ${record.originalPrice}, final_price = ${record.finalPrice},
                lead_party_name = ${record.leadPartyName || null}
          `);
          
          console.log(`Added/Updated record for ${record.bmUnit}: ${volume.toFixed(2)} MWh, £${payment.toFixed(2)}`);
          periodVolume += volume;
          periodPayment += payment;
          addedCount++;
        }
      }
      
      console.log(`Period ${period} complete: Added/Updated ${addedCount} records, ${periodVolume.toFixed(2)} MWh, £${periodPayment.toFixed(2)}`);
      
      // Small delay to not overwhelm the database
      await new Promise(resolve => setTimeout(resolve, 100));
      
    } catch (error) {
      console.error(`Error processing period ${period}:`, error);
    }
  }
  
  // Update daily summary after adding records
  await updateDailySummary();
  
  // Update Bitcoin calculations
  await updateBitcoinCalculations();
  
  // Final verification
  await verifyFixes();
}

async function updateDailySummary() {
  console.log('Updating daily summary...');
  
  // Get total statistics
  const totals = await db.execute<{
    total_volume: string;
    total_payment: string;
    record_count: string;
  }>(sql`
    SELECT 
      SUM(volume) as total_volume,
      SUM(payment) as total_payment,
      COUNT(*) as record_count
    FROM curtailment_records
    WHERE settlement_date = ${TARGET_DATE}
  `);
  
  const totalVolume = parseFloat(totals[0].total_volume);
  const totalPayment = parseFloat(totals[0].total_payment);
  
  // Update daily summary
  await db.execute(sql`
    INSERT INTO daily_summaries (
      summary_date, total_curtailed_energy, total_payment, created_at
    ) VALUES (
      ${TARGET_DATE}, ${totalVolume}, ${totalPayment}, NOW()
    )
    ON CONFLICT (summary_date) DO UPDATE
    SET total_curtailed_energy = ${totalVolume}, 
        total_payment = ${totalPayment}
  `);
  
  console.log(`Updated daily summary: ${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)}`);
}

async function updateBitcoinCalculations() {
  console.log('Triggering Bitcoin calculation updates...');
  
  // Import the unified reconciliation system
  const { processDate } = await import('./unified_reconciliation');
  
  // Process the date to update Bitcoin calculations
  const result = await processDate(TARGET_DATE);
  
  console.log(`Bitcoin calculation update result: ${result.success ? 'Success' : 'Failed'}`);
  if (!result.success) {
    console.error(result.message);
  }
}

async function verifyFixes() {
  console.log('Verifying fixes...');
  
  // Get current database totals
  const dbStats = await db.execute<{
    total_volume: string;
    total_payment: string;
    record_count: string;
  }>(sql`
    SELECT 
      SUM(volume) as total_volume,
      SUM(payment) as total_payment,
      COUNT(*) as record_count
    FROM curtailment_records
    WHERE settlement_date = ${TARGET_DATE}
  `);
  
  const totalVolume = parseFloat(dbStats[0].total_volume);
  const totalPayment = parseFloat(dbStats[0].total_payment);
  const recordCount = parseInt(dbStats[0].record_count);
  
  console.log(`\nCurrent database values: ${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)}, ${recordCount} records`);
  console.log(`Expected API values: 105,247.85 MWh, £3,390,364.09`);
  
  const volumeDiff = 105247.85 - totalVolume;
  const paymentDiff = 3390364.09 - totalPayment;
  
  console.log(`Remaining difference: ${volumeDiff.toFixed(2)} MWh, £${paymentDiff.toFixed(2)}`);
  
  if (Math.abs(volumeDiff) < 1 && Math.abs(paymentDiff) < 100) {
    console.log('✅ Fix successful! The database now matches the expected API values.');
  } else {
    console.log('⚠️ Some discrepancies still remain. Further investigation may be needed.');
  }
  
  // Log successful completion to the database
  await db.execute(sql`
    CREATE TABLE IF NOT EXISTS reconciliation_events (
      id SERIAL PRIMARY KEY,
      date VARCHAR(10) NOT NULL,
      description TEXT NOT NULL,
      before_volume NUMERIC NOT NULL,
      after_volume NUMERIC NOT NULL,
      before_payment NUMERIC NOT NULL,
      after_payment NUMERIC NOT NULL,
      created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    )
  `);
  
  await db.execute(sql`
    INSERT INTO reconciliation_events 
      (date, description, before_volume, after_volume, before_payment, after_payment)
    VALUES 
      (${TARGET_DATE}, 'Final reconciliation of March 5th data', 
       103359.707, ${totalVolume}, 3332242.67, ${totalPayment})
  `);
}

addMissingRecords().catch(console.error);