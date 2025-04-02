/**
 * Check March 28, 2025 Status (Fixed Version)
 * 
 * This script checks the status of data for March 28, 2025 and reports on settlement period coverage
 * without attempting to process the entire day. This helps diagnose data completeness issues.
 */

import { db } from "./db";
import { sql, and, eq } from "drizzle-orm";
import { curtailmentRecords } from "./db/schema";

// Target date
const TARGET_DATE = '2025-03-28';

async function checkPeriodCoverage(): Promise<void> {
  console.log(`\n=== Checking Period Coverage for ${TARGET_DATE} ===`);
  console.log(`Started at: ${new Date().toISOString()}`);
  
  try {
    // Get all periods that have data
    const result = await db.select({
      recordCount: sql<number>`count(*)`.as('record_count'),
      totalVolume: sql<number>`sum(abs(cast(${curtailmentRecords.volume} as float)))`.as('total_volume'),
      totalPayment: sql<number>`sum(cast(${curtailmentRecords.payment} as float))`.as('total_payment')
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    // Get period breakdowns
    const periodsWithData = await db.select({
      period: curtailmentRecords.settlementPeriod,
      count: sql<number>`count(*)`,
      totalVolume: sql<number>`sum(abs(cast(${curtailmentRecords.volume} as float)))`,
      totalPayment: sql<number>`sum(cast(${curtailmentRecords.payment} as float))`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE))
    .groupBy(curtailmentRecords.settlementPeriod)
    .orderBy(curtailmentRecords.settlementPeriod);
    
    // Create a map for all 48 periods
    const periodMap = new Map<number, { count: number, volume: number, payment: number }>();
    for (let i = 1; i <= 48; i++) {
      periodMap.set(i, { count: 0, volume: 0, payment: 0 });
    }
    
    // Fill in data for periods that have records
    periodsWithData.forEach(period => {
      periodMap.set(period.period, { 
        count: period.count, 
        volume: Math.abs(Number(period.totalVolume) || 0),
        payment: Number(period.totalPayment) || 0
      });
    });
    
    // Print the results
    console.log("\nSettlement Period Coverage:");
    console.log("Period | Records | Volume (MWh) | Payment (£)");
    console.log("-------|---------|--------------|------------");
    
    let totalCount = 0;
    let totalVolume = 0;
    let totalPayment = 0;
    let periodsWithRecords = 0;
    
    periodMap.forEach((data, period) => {
      const hasData = data.count > 0;
      if (hasData) periodsWithRecords++;
      
      console.log(
        `${period.toString().padStart(6)} | ${data.count.toString().padStart(7)} | ${data.volume.toFixed(2).padStart(12)} | ${data.payment.toFixed(2).padStart(10)}`
      );
      
      totalCount += data.count;
      totalVolume += data.volume;
      totalPayment += data.payment;
    });
    
    // Print summary - using actual totals from aggregate query
    console.log("-------|---------|--------------|------------");
    
    // Use the aggregate query result to check for inconsistencies
    const totalFromAggregate = result[0] ? {
      count: Number(result[0].recordCount) || 0,
      volume: Number(result[0].totalVolume) || 0,
      payment: Number(result[0].totalPayment) || 0
    } : { count: 0, volume: 0, payment: 0 };
    
    console.log(
      `Total  | ${totalFromAggregate.count.toString().padStart(7)} | ${totalFromAggregate.volume.toFixed(2).padStart(12)} | ${totalFromAggregate.payment.toFixed(2).padStart(10)}`
    );
    
    console.log(`\nPeriods with data: ${periodsWithRecords}/48 (${((periodsWithRecords / 48) * 100).toFixed(1)}% coverage)`);
    
    // Print quick insight into gaps
    const missingPeriods = [];
    for (let i = 1; i <= 48; i++) {
      if (periodMap.get(i)?.count === 0) {
        missingPeriods.push(i);
      }
    }
    
    if (missingPeriods.length > 0) {
      console.log(`\nMissing periods: ${missingPeriods.join(', ')}`);
      
      // Group consecutive missing periods
      if (missingPeriods.length > 10) {
        let ranges = [];
        let startRange = missingPeriods[0];
        let endRange = startRange;
        
        for (let i = 1; i < missingPeriods.length; i++) {
          if (missingPeriods[i] === endRange + 1) {
            endRange = missingPeriods[i];
          } else {
            ranges.push(startRange === endRange ? `${startRange}` : `${startRange}-${endRange}`);
            startRange = missingPeriods[i];
            endRange = startRange;
          }
        }
        
        // Add the last range
        ranges.push(startRange === endRange ? `${startRange}` : `${startRange}-${endRange}`);
        
        console.log(`Ranges of missing periods: ${ranges.join(', ')}`);
      }
    } else {
      console.log(`\nAll periods have data! Complete coverage.`);
    }
    
  } catch (error) {
    console.error(`Error checking period coverage: ${error}`);
  }
}

async function main(): Promise<void> {
  await checkPeriodCoverage();
  process.exit(0);
}

main().catch(error => {
  console.error('Unhandled error:', error);
  process.exit(1);
});