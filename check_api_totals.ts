/**
 * Check API totals for March 5th, 2025
 * 
 * This script fetches data from the Elexon API for March 5th, 2025
 * and calculates totals for comparison with database values.
 */

import { fetchBidsOffers } from './server/services/elexon';

const TARGET_DATE = '2025-03-05';

async function main() {
  console.log(`Checking API data for ${TARGET_DATE}...`);
  
  let totalVolume = 0;
  let totalPayment = 0;
  let totalRecords = 0;
  
  // For all 48 periods of the day
  for (let period = 1; period <= 48; period++) {
    try {
      const records = await fetchBidsOffers(TARGET_DATE, period);
      
      // Filter to include only curtailed records (soFlag true, volume > 0)
      const curtailedRecords = records.filter(r => r.soFlag && r.volume > 0);
      
      // Calculate totals for this period
      const periodVolume = curtailedRecords.reduce((sum, r) => sum + r.volume, 0);
      const periodPayment = curtailedRecords.reduce((sum, r) => sum + (r.volume * r.finalPrice), 0);
      
      // Update totals
      totalVolume += periodVolume;
      totalPayment += periodPayment;
      totalRecords += curtailedRecords.length;
      
      console.log(`Period ${period}: ${curtailedRecords.length} records, ${periodVolume.toFixed(2)} MWh, £${periodPayment.toFixed(2)}`);
    } catch (error) {
      console.error(`Error fetching period ${period}:`, error);
    }
    
    // Small delay to not overwhelm the API
    await new Promise(resolve => setTimeout(resolve, 200));
  }
  
  console.log(`\n=== API Data Summary ===`);
  console.log(`Total records: ${totalRecords}`);
  console.log(`Total volume: ${totalVolume.toFixed(2)} MWh`);
  console.log(`Total payment: £${totalPayment.toFixed(2)}`);
  
  // Compare with known database values
  console.log(`\n=== Comparison with Database ===`);
  console.log(`Database volume: 103,359.71 MWh`);
  console.log(`Database payment: £3,332,242.67`);
  console.log(`API volume: ${totalVolume.toFixed(2)} MWh`);
  console.log(`API payment: £${totalPayment.toFixed(2)}`);
  
  const volumeDiff = totalVolume - 103359.71;
  const paymentDiff = totalPayment - 3332242.67;
  
  console.log(`\n=== Differences ===`);
  console.log(`Volume difference: ${volumeDiff.toFixed(2)} MWh`);
  console.log(`Payment difference: £${paymentDiff.toFixed(2)}`);
}

main().catch(console.error);