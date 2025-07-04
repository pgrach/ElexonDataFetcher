/**
 * Comprehensive verification for July 1, 2025
 * 
 * This script performs proper period-by-period verification against the Elexon API
 * to identify all missing data and provide complete analysis
 */

import { db } from '../db';
import { curtailmentRecords } from '../db/schema';
import { fetchBidsOffers } from '../server/services/elexon';
import { eq, and } from 'drizzle-orm';

const TARGET_DATE = '2025-07-01';

async function delay(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function checkDatabaseState() {
  console.log('=== DATABASE STATE CHECK ===');
  
  const stats = await db.execute(`
    SELECT 
      COUNT(*) as total_records,
      COUNT(DISTINCT settlement_period) as periods,
      SUM(ABS(volume::numeric)) as total_volume,
      SUM(payment::numeric) as total_payment
    FROM curtailment_records 
    WHERE settlement_date = '${TARGET_DATE}'
  `);
  
  const { total_records, periods, total_volume, total_payment } = stats.rows[0];
  
  console.log(`Database records for ${TARGET_DATE}:`);
  console.log(`  Total records: ${total_records}`);
  console.log(`  Periods with data: ${periods}`);
  console.log(`  Total volume: ${total_volume ? Number(total_volume).toFixed(2) : 0} MWh`);
  console.log(`  Total payment: £${total_payment ? Number(total_payment).toFixed(2) : 0}`);
  
  if (Number(total_records) > 0) {
    // Show period breakdown
    const periodBreakdown = await db.execute(`
      SELECT 
        settlement_period,
        COUNT(*) as records,
        SUM(ABS(volume::numeric)) as volume
      FROM curtailment_records 
      WHERE settlement_date = '${TARGET_DATE}'
      GROUP BY settlement_period
      ORDER BY settlement_period
    `);
    
    console.log('\nPeriod breakdown:');
    periodBreakdown.rows.forEach(row => {
      console.log(`  Period ${row.settlement_period}: ${row.records} records, ${Number(row.volume).toFixed(2)} MWh`);
    });
  }
  
  return {
    totalRecords: Number(total_records),
    periodsWithData: Number(periods),
    totalVolume: Number(total_volume || 0),
    totalPayment: Number(total_payment || 0)
  };
}

async function verifyAgainstAPI() {
  console.log('\n=== API VERIFICATION ===');
  console.log('Checking all 48 periods against Elexon API...');
  
  const results = {
    periodsWithData: [],
    periodsWithoutData: [],
    totalApiRecords: 0,
    totalApiVolume: 0,
    discrepancies: []
  };
  
  for (let period = 1; period <= 48; period++) {
    try {
      console.log(`Checking period ${period}...`);
      
      // Fetch from API
      const apiData = await fetchBidsOffers(TARGET_DATE, period);
      
      // Check database
      const dbData = await db.select()
        .from(curtailmentRecords)
        .where(and(
          eq(curtailmentRecords.settlementDate, TARGET_DATE),
          eq(curtailmentRecords.settlementPeriod, period)
        ));
      
      const apiCount = apiData.length;
      const dbCount = dbData.length;
      
      if (apiCount > 0) {
        results.periodsWithData.push(period);
        results.totalApiRecords += apiCount;
        
        // Calculate API volume
        const apiVolume = apiData.reduce((sum, record) => sum + Math.abs(record.volume), 0);
        results.totalApiVolume += apiVolume;
        
        console.log(`  Period ${period}: API=${apiCount}, DB=${dbCount}, Volume=${apiVolume.toFixed(2)} MWh`);
        
        if (apiCount !== dbCount) {
          results.discrepancies.push({
            period,
            apiCount,
            dbCount,
            missing: apiCount - dbCount,
            apiVolume: apiVolume.toFixed(2)
          });
        }
      } else {
        results.periodsWithoutData.push(period);
        console.log(`  Period ${period}: No data`);
      }
      
      // Rate limiting
      await delay(150);
      
    } catch (error) {
      console.error(`  Period ${period}: API Error - ${error.message}`);
      results.periodsWithoutData.push(period);
    }
  }
  
  return results;
}

async function generateReport(dbStats, apiResults) {
  console.log('\n=== COMPREHENSIVE VERIFICATION REPORT ===');
  console.log(`Date: ${TARGET_DATE}`);
  console.log(`Verification time: ${new Date().toISOString()}`);
  
  console.log('\nDatabase Summary:');
  console.log(`  Records: ${dbStats.totalRecords}`);
  console.log(`  Periods: ${dbStats.periodsWithData}`);
  console.log(`  Volume: ${dbStats.totalVolume.toFixed(2)} MWh`);
  console.log(`  Payment: £${dbStats.totalPayment.toFixed(2)}`);
  
  console.log('\nAPI Summary:');
  console.log(`  Records: ${apiResults.totalApiRecords}`);
  console.log(`  Periods with data: ${apiResults.periodsWithData.length}`);
  console.log(`  Volume: ${apiResults.totalApiVolume.toFixed(2)} MWh`);
  console.log(`  Periods: ${apiResults.periodsWithData.join(', ')}`);
  
  console.log('\nDiscrepancies:');
  if (apiResults.discrepancies.length === 0) {
    if (dbStats.totalRecords === 0 && apiResults.totalApiRecords === 0) {
      console.log('  ✓ No data available for this date in either source');
    } else if (dbStats.totalRecords === apiResults.totalApiRecords) {
      console.log('  ✓ Perfect match - no discrepancies found');
    } else {
      console.log(`  ❌ Total count mismatch: DB=${dbStats.totalRecords}, API=${apiResults.totalApiRecords}`);
    }
  } else {
    console.log(`  ❌ Found ${apiResults.discrepancies.length} periods with discrepancies:`);
    apiResults.discrepancies.forEach(disc => {
      console.log(`    Period ${disc.period}: Missing ${disc.missing} records (${disc.apiVolume} MWh)`);
    });
    
    const totalMissing = apiResults.discrepancies.reduce((sum, disc) => sum + disc.missing, 0);
    console.log(`  Total missing records: ${totalMissing}`);
  }
  
  console.log('\nData Quality Assessment:');
  if (dbStats.totalRecords === 0 && apiResults.totalApiRecords === 0) {
    console.log('  STATUS: ✓ COMPLETE - No data available for this date');
  } else if (dbStats.totalRecords === apiResults.totalApiRecords && apiResults.discrepancies.length === 0) {
    console.log('  STATUS: ✓ COMPLETE - Perfect data integrity');
  } else {
    console.log('  STATUS: ❌ INCOMPLETE - Data ingestion required');
    console.log(`  Missing: ${apiResults.totalApiRecords - dbStats.totalRecords} records`);
    console.log(`  Volume gap: ${(apiResults.totalApiVolume - dbStats.totalVolume).toFixed(2)} MWh`);
  }
  
  return {
    isComplete: dbStats.totalRecords === apiResults.totalApiRecords && apiResults.discrepancies.length === 0,
    totalMissing: apiResults.totalApiRecords - dbStats.totalRecords,
    volumeGap: apiResults.totalApiVolume - dbStats.totalVolume
  };
}

async function main() {
  try {
    console.log(`Starting comprehensive verification for ${TARGET_DATE}...`);
    
    // Step 1: Check current database state
    const dbStats = await checkDatabaseState();
    
    // Step 2: Verify against API
    const apiResults = await verifyAgainstAPI();
    
    // Step 3: Generate comprehensive report
    const assessment = await generateReport(dbStats, apiResults);
    
    console.log('\n=== FINAL ASSESSMENT ===');
    if (assessment.isComplete) {
      console.log('✅ July 1, 2025 data is COMPLETE and verified');
    } else {
      console.log('❌ July 1, 2025 data is INCOMPLETE');
      console.log(`Missing: ${assessment.totalMissing} records`);
      console.log(`Volume gap: ${assessment.volumeGap.toFixed(2)} MWh`);
      console.log('\nRecommendation: Full data ingestion required');
    }
    
    return assessment;
    
  } catch (error) {
    console.error('❌ Verification failed:', error);
    return { isComplete: false, error: error.message };
  }
}

main();