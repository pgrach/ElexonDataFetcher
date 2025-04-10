/**
 * Inspect Elexon API Records for 2025-04-03
 * 
 * This script is specifically focused on inspecting the raw API response
 * to understand why there's a mismatch between API and DB data.
 */

import { fetchBidsOffers } from "../server/services/elexon";

// Target date and periods
const TARGET_DATE = "2025-04-03";
const PERIODS_TO_CHECK = [35, 36, 37, 38, 39, 40, 45, 46, 47, 48];

// Main function
async function inspectRecords() {
  console.log("===== INSPECTING ELEXON API DATA FOR 2025-04-03 =====\n");
  
  for (const period of PERIODS_TO_CHECK) {
    console.log(`\n--- Period ${period} ---`);
    
    try {
      // Fetch records from API
      const records = await fetchBidsOffers(TARGET_DATE, period);
      
      if (!records || records.length === 0) {
        console.log(`No records found for Period ${period}`);
        continue;
      }
      
      console.log(`Found ${records.length} records from API`);
      
      // Extract just wind farm records (those starting with T_)
      const windRecords = records.filter(r => r.id && r.id.startsWith('T_'));
      console.log(`Of which ${windRecords.length} are wind farm records`);
      
      // Extract curtailment records (those with negative volume)
      const curtailmentRecords = windRecords.filter(r => Number(r.volume) < 0);
      console.log(`Of which ${curtailmentRecords.length} are curtailment records (negative volume)`);
      
      // Display detailed breakdown of curtailment records
      if (curtailmentRecords.length > 0) {
        console.log("\nCurtailment Records:");
        let totalVolume = 0;
        let totalPayment = 0;
        
        curtailmentRecords.forEach(record => {
          const absVolume = Math.abs(Number(record.volume));
          const payment = absVolume * Number(record.originalPrice) * -1;
          
          console.log(`- Farm: ${record.id}, Lead Party: ${record.leadPartyName || 'Unknown'}`);
          console.log(`  Volume: ${absVolume.toFixed(2)} MWh, Price: £${Number(record.originalPrice).toFixed(2)}, Payment: £${payment.toFixed(2)}`);
          console.log(`  Raw data - volume: ${record.volume}, price: ${record.originalPrice}`);
          
          totalVolume += absVolume;
          totalPayment += payment;
        });
        
        console.log(`\nTotal Volume: ${totalVolume.toFixed(2)} MWh`);
        console.log(`Total Payment: £${totalPayment.toFixed(2)}`);
      }
      
      // Also inspect non-curtailment wind records if any
      const nonCurtailmentRecords = windRecords.filter(r => Number(r.volume) >= 0);
      if (nonCurtailmentRecords.length > 0) {
        console.log("\nNon-Curtailment Wind Records:");
        nonCurtailmentRecords.forEach(record => {
          console.log(`- Farm: ${record.id}, Volume: ${record.volume}, Lead Party: ${record.leadPartyName || 'Unknown'}`);
        });
      }
      
    } catch (error) {
      console.error(`Error fetching period ${period}:`, error);
    }
  }
  
  console.log("\n===== INSPECTION COMPLETE =====");
}

// Run the inspection
inspectRecords().catch(err => {
  console.error("Fatal error:", err);
  process.exit(1);
});