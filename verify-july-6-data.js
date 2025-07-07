import { fetchBidsOffers } from './server/services/elexon.ts';
import { db } from './db/index.ts';
import { curtailmentRecords } from './db/schema.ts';
import { eq } from 'drizzle-orm';

async function verifyJuly6Data() {
  console.log('=== Verifying July 6 Data Against Elexon API ===\n');
  
  const date = '2025-07-06';
  let totalApiVolume = 0;
  let totalApiPayment = 0;
  let totalApiRecords = 0;
  const periodsWithData = [];
  
  console.log('Fetching data from Elexon API for all settlement periods...');
  
  // Fetch data for all 48 settlement periods
  for (let period = 1; period <= 48; period++) {
    try {
      const apiRecords = await fetchBidsOffers(date, period);
      
      if (apiRecords.length > 0) {
        periodsWithData.push(period);
        const periodVolume = apiRecords.reduce((sum, r) => sum + Math.abs(r.volume), 0);
        const periodPayment = apiRecords.reduce((sum, r) => sum + (Math.abs(r.volume) * r.originalPrice), 0);
        
        totalApiVolume += periodVolume;
        totalApiPayment += periodPayment;
        totalApiRecords += apiRecords.length;
        
        console.log(`Period ${period}: ${apiRecords.length} records, ${periodVolume.toFixed(2)} MWh, £${periodPayment.toFixed(2)}`);
      }
    } catch (error) {
      console.error(`Error fetching period ${period}:`, error.message);
    }
  }
  
  console.log(`\n=== API Data Summary ===`);
  console.log(`Total Records: ${totalApiRecords}`);
  console.log(`Total Volume: ${totalApiVolume.toFixed(2)} MWh`);
  console.log(`Total Payment: £${totalApiPayment.toFixed(2)}`);
  console.log(`Periods with data: ${periodsWithData.join(', ')}`);
  
  // Now fetch database data for comparison
  console.log('\n=== Database Data ===');
  const dbRecords = await db.select().from(curtailmentRecords).where(eq(curtailmentRecords.settlementDate, date));
  
  const dbVolume = dbRecords.reduce((sum, r) => sum + Math.abs(Number(r.volume)), 0);
  const dbPayment = dbRecords.reduce((sum, r) => sum + Number(r.payment), 0);
  const dbPeriods = [...new Set(dbRecords.map(r => r.settlementPeriod))].sort();
  
  console.log(`Total Records: ${dbRecords.length}`);
  console.log(`Total Volume: ${dbVolume.toFixed(2)} MWh`);
  console.log(`Total Payment: £${dbPayment.toFixed(2)}`);
  console.log(`Periods with data: ${dbPeriods.join(', ')}`);
  
  // Comparison
  console.log('\n=== Comparison ===');
  console.log(`Records Match: ${totalApiRecords === dbRecords.length ? '✓' : '✗'} (API: ${totalApiRecords}, DB: ${dbRecords.length})`);
  console.log(`Volume Match: ${Math.abs(totalApiVolume - dbVolume) < 0.01 ? '✓' : '✗'} (API: ${totalApiVolume.toFixed(2)}, DB: ${dbVolume.toFixed(2)})`);
  console.log(`Payment Match: ${Math.abs(totalApiPayment - dbPayment) < 0.01 ? '✓' : '✗'} (API: £${totalApiPayment.toFixed(2)}, DB: £${dbPayment.toFixed(2)})`);
  console.log(`Periods Match: ${JSON.stringify(periodsWithData) === JSON.stringify(dbPeriods) ? '✓' : '✗'} (API: ${periodsWithData.join(',')}, DB: ${dbPeriods.join(',')})`);
  
  if (totalApiRecords !== dbRecords.length || Math.abs(totalApiVolume - dbVolume) >= 0.01) {
    console.log('\n⚠️  DATA MISMATCH DETECTED - Re-ingestion needed!');
    return false;
  } else {
    console.log('\n✅ Data verification passed - Database matches API data');
    return true;
  }
}

verifyJuly6Data().then(result => {
  if (!result) {
    console.log('\nRun re-ingestion using: processDailyCurtailment("2025-07-06")');
  }
  process.exit(result ? 0 : 1);
}).catch(error => {
  console.error('Verification failed:', error);
  process.exit(1);
});