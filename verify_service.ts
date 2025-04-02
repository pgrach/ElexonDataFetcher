/**
 * Verify Data Integrity Between Tables
 * 
 * This script verifies that the curtailment_records and daily_summaries tables
 * have consistent data, particularly focusing on payment calculations.
 */

import { db } from "./db";
import { curtailmentRecords, dailySummaries } from "./db/schema";
import { eq, sql } from "drizzle-orm";

// List of dates to verify
const DATES_TO_CHECK = [
  '2025-03-28',
  '2025-03-29',
  '2025-03-30',
  '2025-03-31',
  '2025-04-01'
];

/**
 * Verify the data integrity between curtailment_records and daily_summaries 
 * for a specific date
 */
async function verifyDataIntegrity(date: string): Promise<{
  isConsistent: boolean;
  recordCount: number;
  dbEnergy: number;
  dbPayment: number;
  summaryEnergy: number;
  summaryPayment: number;
}> {
  try {
    // Get totals directly from curtailment_records
    const dbTotals = await db
      .select({
        recordCount: sql<string>`COUNT(*)`,
        totalCurtailedEnergy: sql<string>`SUM(ABS(${curtailmentRecords.volume})::numeric)`,
        totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date));

    // Get values from daily_summaries
    const summary = await db
      .select()
      .from(dailySummaries)
      .where(eq(dailySummaries.summaryDate, date));

    const dbEnergy = dbTotals[0]?.totalCurtailedEnergy ? parseFloat(dbTotals[0].totalCurtailedEnergy) : 0;
    const dbPayment = dbTotals[0]?.totalPayment ? parseFloat(dbTotals[0].totalPayment) : 0;
    
    let summaryEnergy = 0;
    let summaryPayment = 0;
    
    if (summary.length > 0) {
      summaryEnergy = parseFloat(summary[0].totalCurtailedEnergy);
      summaryPayment = parseFloat(summary[0].totalPayment);
    }

    // Check if values are consistent (within 0.01 tolerance)
    const isEnergyConsistent = Math.abs(dbEnergy - summaryEnergy) < 0.01;
    const isPaymentConsistent = Math.abs(dbPayment - summaryPayment) < 0.01;
    const isConsistent = isEnergyConsistent && isPaymentConsistent;

    return {
      isConsistent,
      recordCount: parseInt(dbTotals[0]?.recordCount as string || '0'),
      dbEnergy,
      dbPayment,
      summaryEnergy,
      summaryPayment
    };
  } catch (error) {
    console.error(`Error verifying data integrity for ${date}:`, error);
    throw error;
  }
}

async function verifyDates() {
  console.log('=== Curtailment Data Integrity Verification ===');
  
  for (const date of DATES_TO_CHECK) {
    try {
      console.log(`\nVerifying ${date}...`);
      
      const result = await verifyDataIntegrity(date);
      
      console.log(`Records: ${result.recordCount}`);
      console.log(`Data consistency: ${result.isConsistent ? 'CONSISTENT ✓' : 'INCONSISTENT ✗'}`);
      
      console.log('Raw database values:');
      console.log(`- Energy: ${result.dbEnergy.toFixed(2)} MWh`);
      console.log(`- Payment: £${result.dbPayment.toFixed(2)}`);
      
      console.log('Summary table values:');
      console.log(`- Energy: ${result.summaryEnergy.toFixed(2)} MWh`);
      console.log(`- Payment: £${result.summaryPayment.toFixed(2)}`);
      
      if (!result.isConsistent) {
        console.log('\nDifferences:');
        console.log(`- Energy diff: ${Math.abs(result.dbEnergy - result.summaryEnergy).toFixed(2)} MWh`);
        console.log(`- Payment diff: £${Math.abs(result.dbPayment - result.summaryPayment).toFixed(2)}`);
      }
    } catch (error) {
      console.error(`Error verifying ${date}:`, error);
    }
  }
  
  console.log('\n=== Verification Complete ===');
}

verifyDates()
  .then(() => {
    console.log('Script completed successfully!');
    process.exit(0);
  })
  .catch(error => {
    console.error('Script failed:', error);
    process.exit(1);
  });