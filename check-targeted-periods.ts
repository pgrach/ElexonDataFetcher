/**
 * Specifically check periods 16, 17, 32-36, and 47-48 for 2025-04-13
 * to ensure we have all data
 */

import { fetchBidsOffers } from "./server/services/elexon";

const DATE = "2025-04-13";
const PERIODS = [16, 17, 32, 33, 34, 35, 36, 47, 48];

async function checkSpecificPeriods() {
  console.log(`Specifically checking periods ${PERIODS.join(", ")} on ${DATE} from Elexon API...`);
  
  // Print a header for the results table
  console.log("\nPeriod | Records | Volume (MWh) | Payment (£) | Has Curtailment?");
  console.log("------|---------|--------------|------------|---------------");
  
  for (const period of PERIODS) {
    try {
      const records = await fetchBidsOffers(DATE, period);
      
      // Check for curtailment (negative volume and either soFlag or cadlFlag)
      const curtailmentRecords = records.filter(r => 
        r.volume < 0 && (r.soFlag || r.cadlFlag)
      );
      
      // Calculate totals
      const volume = curtailmentRecords.reduce((sum, r) => sum + Math.abs(r.volume), 0);
      const payment = curtailmentRecords.reduce((sum, r) => sum + (Math.abs(r.volume) * r.originalPrice), 0);
      
      const hasCurtailment = curtailmentRecords.length > 0 ? "Yes" : "No";
      
      // Print row with data
      console.log(`${period.toString().padStart(6)} | ${records.length.toString().padStart(7)} | ${volume.toFixed(2).padStart(12)} | ${payment.toFixed(2).padStart(10)} | ${hasCurtailment.padStart(15)}`);
      
      // If we have curtailment records, show a sample
      if (curtailmentRecords.length > 0) {
        console.log(`  Sample curtailment records for period ${period}:`);
        for (let i = 0; i < Math.min(curtailmentRecords.length, 2); i++) {
          const r = curtailmentRecords[i];
          console.log(`  - BMU: ${r.id}, Volume: ${Math.abs(r.volume).toFixed(2)} MWh, Price: £${r.originalPrice.toFixed(2)}, Payment: £${(Math.abs(r.volume) * r.originalPrice).toFixed(2)}`);
        }
      }
      
      // Add a separator for readability
      console.log("");
    } catch (error) {
      console.error(`Error fetching period ${period}:`, error);
    }
  }
}

// Run the check
checkSpecificPeriods()
  .then(() => {
    console.log("Check completed");
    process.exit(0);
  })
  .catch(err => {
    console.error("Error:", err);
    process.exit(1);
  });