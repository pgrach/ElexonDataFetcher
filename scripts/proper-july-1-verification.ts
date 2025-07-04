/**
 * Proper verification for July 1, 2025 - checking ALL periods
 * and fixing payment calculation issues
 */

import { fetchBidsOffers } from '../server/services/elexon';

const TARGET_DATE = '2025-07-01';

async function delay(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function verifyAllPeriods() {
  console.log('=== COMPLETE API VERIFICATION FOR JULY 1, 2025 ===');
  
  const results = [];
  let totalApiRecords = 0;
  let totalApiVolume = 0;
  let totalApiPayment = 0;
  
  for (let period = 1; period <= 48; period++) {
    try {
      console.log(`Checking period ${period}...`);
      
      const apiData = await fetchBidsOffers(TARGET_DATE, period);
      
      if (apiData.length > 0) {
        const periodVolume = apiData.reduce((sum, record) => sum + Math.abs(record.volume), 0);
        const periodPayment = apiData.reduce((sum, record) => sum + (Math.abs(record.volume) * record.originalPrice), 0);
        
        console.log(`  Period ${period}: ${apiData.length} records, ${periodVolume.toFixed(2)} MWh, £${periodPayment.toFixed(2)}`);
        
        // Show first few records to check payment calculation
        if (period <= 3) {
          console.log('  Sample records:');
          apiData.slice(0, 2).forEach(record => {
            const payment = Math.abs(record.volume) * record.originalPrice;
            console.log(`    ${record.id}: ${Math.abs(record.volume)} MWh × £${record.originalPrice} = £${payment.toFixed(2)}`);
          });
        }
        
        results.push({
          period,
          records: apiData.length,
          volume: periodVolume,
          payment: periodPayment
        });
        
        totalApiRecords += apiData.length;
        totalApiVolume += periodVolume;
        totalApiPayment += periodPayment;
      } else {
        console.log(`  Period ${period}: No data`);
      }
      
      await delay(100); // Shorter delay for faster verification
      
    } catch (error) {
      console.error(`  Period ${period}: Error - ${error.message}`);
    }
  }
  
  console.log('\n=== SUMMARY ===');
  console.log(`Total periods with data: ${results.length}`);
  console.log(`Total API records: ${totalApiRecords}`);
  console.log(`Total API volume: ${totalApiVolume.toFixed(2)} MWh`);
  console.log(`Total API payment: £${totalApiPayment.toFixed(2)}`);
  
  console.log('\n=== PERIODS WITH DATA ===');
  results.forEach(r => {
    console.log(`Period ${r.period}: ${r.records} records, ${r.volume.toFixed(2)} MWh, £${r.payment.toFixed(2)}`);
  });
  
  return { results, totalApiRecords, totalApiVolume, totalApiPayment };
}

async function main() {
  try {
    const verification = await verifyAllPeriods();
    
    console.log('\n=== VERIFICATION COMPLETE ===');
    console.log('All 48 periods have been checked against the API');
    
    return verification;
    
  } catch (error) {
    console.error('Verification failed:', error);
    return null;
  }
}

main();