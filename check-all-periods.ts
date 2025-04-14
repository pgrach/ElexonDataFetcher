/**
 * Check all 48 periods in Elexon API for 2025-04-13
 * This will identify all periods that have curtailment data
 */

import { fetchBidsOffers } from "./server/services/elexon";

const DATE = "2025-04-13";

async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function checkAllPeriods() {
  console.log(`Checking all 48 periods on ${DATE} from Elexon API...`);
  
  const periodsWithData = [];
  const summaryData = {
    totalRecords: 0,
    totalVolume: 0,
    totalPayment: 0
  };
  
  // Print a header for the results table
  console.log("\nPeriod | Records | Volume (MWh) | Payment (£)");
  console.log("------|---------|--------------|------------");
  
  for (let period = 1; period <= 48; period++) {
    try {
      const records = await fetchBidsOffers(DATE, period);
      
      // Calculate totals
      const volume = records.reduce((sum, r) => sum + Math.abs(r.volume), 0);
      const payment = records.reduce((sum, r) => sum + (Math.abs(r.volume) * r.originalPrice), 0);
      
      // If we have records, add to our list and totals
      if (records.length > 0) {
        periodsWithData.push(period);
        summaryData.totalRecords += records.length;
        summaryData.totalVolume += volume;
        summaryData.totalPayment += payment;
        
        // Print row with data
        console.log(`${period.toString().padStart(6)} | ${records.length.toString().padStart(7)} | ${volume.toFixed(2).padStart(12)} | ${payment.toFixed(2).padStart(10)}`);
      }
      
      // Add slight delay to prevent rate limiting
      if (period < 48) {
        await delay(300);
      }
    } catch (error) {
      console.error(`Error fetching period ${period}:`, error);
    }
  }
  
  // Print summary
  console.log("\n=== Summary ===");
  console.log(`Periods with data: ${periodsWithData.length} (${periodsWithData.join(", ")})`);
  console.log(`Total records: ${summaryData.totalRecords}`);
  console.log(`Total volume: ${summaryData.totalVolume.toFixed(2)} MWh`);
  console.log(`Total payment: £${summaryData.totalPayment.toFixed(2)}`);
}

// Run the check
checkAllPeriods()
  .then(() => {
    console.log("\nCheck completed");
    process.exit(0);
  })
  .catch(err => {
    console.error("Error:", err);
    process.exit(1);
  });