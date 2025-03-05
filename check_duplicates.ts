/**
 * Check for duplicates and missing data in curtailment records
 * 
 * This script analyzes curtailment records for a specific date, identifying duplicate entries
 * and comparing with the expected total values to find discrepancies.
 */

import { db } from "./db";
import { curtailmentRecords } from "./db/schema";
import { eq, and } from "drizzle-orm";
import { format } from "date-fns";

async function checkDuplicatesAndDiscrepancies(date: string) {
  console.log(`\nAnalyzing curtailment records for ${date}...\n`);

  // Get all records for the date
  const records = await db.select().from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date));
  
  console.log(`Total records found: ${records.length}`);
  
  // Calculate total volume and payment
  const totalVolume = records.reduce((sum, record) => sum + Number(record.volume), 0);
  const totalPayment = records.reduce((sum, record) => sum + Number(record.payment), 0);
  
  console.log(`Current totals: ${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)}`);
  console.log(`Expected totals: 93,531.21 MWh, £-2,519,672.84`);
  
  const volumeDifference = 93531.21 - totalVolume;
  const paymentDifference = -2519672.84 - totalPayment;
  
  console.log(`Discrepancy: ${volumeDifference.toFixed(2)} MWh, £${paymentDifference.toFixed(2)}\n`);
  
  // Find periods with duplicate entries for the same farm
  console.log("Searching for duplicate farm/period combinations...");
  
  const periodFarmCounts = new Map<string, {count: number, volume: number, payment: number}>();
  
  for (const record of records) {
    const key = `${record.farmId}-P${record.settlementPeriod}`;
    const current = periodFarmCounts.get(key) || {count: 0, volume: 0, payment: 0};
    
    periodFarmCounts.set(key, {
      count: current.count + 1,
      volume: current.volume + Number(record.volume),
      payment: current.payment + Number(record.payment)
    });
  }
  
  // Get duplicates
  const duplicates = [...periodFarmCounts.entries()]
    .filter(([_, data]) => data.count > 1)
    .sort((a, b) => b[1].count - a[1].count);
  
  console.log(`Found ${duplicates.length} farm/period combinations with duplicate records.`);
  
  if (duplicates.length > 0) {
    console.log("\nTop 10 duplicates:");
    console.log("Farm-Period, Count, Total Volume, Total Payment");
    
    duplicates.slice(0, 10).forEach(([key, data]) => {
      console.log(`${key}, ${data.count}, ${data.volume.toFixed(2)}, £${data.payment.toFixed(2)}`);
    });
    
    // Calculate total volume and payment in duplicates
    const totalDuplicateVolume = duplicates.reduce((sum, [_, data]) => sum + data.volume, 0);
    const totalDuplicatePayment = duplicates.reduce((sum, [_, data]) => sum + data.payment, 0);
    
    console.log(`\nTotal in all duplicates: ${totalDuplicateVolume.toFixed(2)} MWh, £${totalDuplicatePayment.toFixed(2)}`);
  }
  
  // Find missing periods
  console.log("\nChecking for missing periods...");
  
  const periodsInData = new Set(records.map(r => r.settlementPeriod));
  const allPeriods = Array.from({length: 48}, (_, i) => i + 1);
  const missingPeriods = allPeriods.filter(p => !periodsInData.has(p));
  
  console.log(`Periods present in data: ${Array.from(periodsInData).sort((a, b) => a - b).join(', ')}`);
  
  if (missingPeriods.length > 0) {
    console.log(`Missing periods: ${missingPeriods.join(', ')}`);
  } else {
    console.log("All 48 periods are present in the data.");
  }
}

async function main() {
  const date = process.argv[2] || "2025-03-04";
  await checkDuplicatesAndDiscrepancies(date);
  process.exit(0);
}

main().catch(error => {
  console.error("Error:", error);
  process.exit(1);
});