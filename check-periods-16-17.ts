/**
 * Check if Elexon API has data for periods 16 and 17 on 2025-04-13
 */

import { fetchBidsOffers } from "./server/services/elexon";

const DATE = "2025-04-13";
const PERIODS = [16, 17];

async function checkPeriods() {
  console.log(`Checking Elexon API for periods ${PERIODS.join(", ")} on ${DATE}`);
  
  for (const period of PERIODS) {
    console.log(`\nFetching period ${period} data...`);
    try {
      const records = await fetchBidsOffers(DATE, period);
      console.log(`Period ${period} data: ${records.length} records`);
      
      if (records.length > 0) {
        const totalVolume = records.reduce((sum, r) => sum + Math.abs(r.volume), 0);
        const totalPayment = records.reduce((sum, r) => sum + (Math.abs(r.volume) * r.originalPrice), 0);
        
        console.log(`Total volume: ${totalVolume.toFixed(2)} MWh`);
        console.log(`Total payment: £${totalPayment.toFixed(2)}`);
        console.log("Sample data:", records.slice(0, 2));
      } else {
        console.log("No curtailment records found for this period");
      }
    } catch (error) {
      console.error(`Error fetching period ${period}:`, error);
    }
  }
}

// Run the check
checkPeriods()
  .then(() => {
    console.log("\nCheck completed");
    process.exit(0);
  })
  .catch(err => {
    console.error("Error:", err);
    process.exit(1);
  });