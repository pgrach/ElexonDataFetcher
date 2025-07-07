import { fetchBidsOffers } from '../server/services/elexon.js';
import { db } from '../db/index.js';
import { curtailmentRecords } from '../db/schema.js';
import { eq } from 'drizzle-orm';

async function verifyJuly3() {
  const date = '2025-07-03';
  
  console.log('\n=== PHASE 1: API VERIFICATION ===');
  
  let totalApiRecords = 0;
  let totalApiVolume = 0;
  let periodsWithData = [];
  
  // Check key periods first (1-3, 33-48 are common for curtailment)
  for (let period of [1,2,3,33,34,35,38,39,40,41,42,43,44,45,46,47,48]) {
    try {
      const records = await fetchBidsOffers(date, period);
      if (records.length > 0) {
        periodsWithData.push(period);
        totalApiRecords += records.length;
        totalApiVolume += records.reduce((sum: number, r: any) => sum + Math.abs(r.volume), 0);
      }
    } catch (error) {
      console.error(`Error fetching period ${period}:`, error);
    }
  }
  
  console.log(`API Data: ${totalApiRecords} records, ${totalApiVolume.toFixed(2)} MWh, periods: ${periodsWithData.join(',')}`);
  
  console.log('\n=== PHASE 2: DATABASE COMPARISON ===');
  
  const dbRecords = await db.select().from(curtailmentRecords).where(eq(curtailmentRecords.settlementDate, date));
  const dbVolume = dbRecords.reduce((sum, r) => sum + Math.abs(parseFloat(r.volume)), 0);
  const dbPayment = dbRecords.reduce((sum, r) => sum + parseFloat(r.payment), 0);
  
  console.log(`Database Data: ${dbRecords.length} records, ${dbVolume.toFixed(2)} MWh, Payment: £${dbPayment.toFixed(2)}`);
  
  console.log('\n=== COMPARISON RESULTS ===');
  console.log(`Record Count Match: ${Math.abs(totalApiRecords - dbRecords.length) < 10 ? '✅' : '❌'}`);
  console.log(`Volume Match: ${Math.abs(totalApiVolume - dbVolume) < 100 ? '✅' : '❌'}`);
  console.log(`Payment Sign: ${dbPayment > 0 ? '✅ POSITIVE' : '❌ NEGATIVE'}`);
  
  if (Math.abs(totalApiRecords - dbRecords.length) > 10 || Math.abs(totalApiVolume - dbVolume) > 100 || dbPayment < 0) {
    console.log('\n🚨 DATA INTEGRITY ISSUES DETECTED');
    console.log('Recommended action: Re-ingest data and fix payment signs');
  } else {
    console.log('\n✅ DATA INTEGRITY VERIFIED');
  }
  
  return {
    apiRecords: totalApiRecords,
    apiVolume: totalApiVolume,
    dbRecords: dbRecords.length,
    dbVolume,
    dbPayment,
    periodsWithData
  };
}

// Run verification
verifyJuly3().then(() => {
  console.log('\nVerification complete');
  process.exit(0);
}).catch(error => {
  console.error('Verification failed:', error);
  process.exit(1);
});