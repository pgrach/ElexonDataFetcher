/**
 * Verify 2024 Data Coverage
 * 
 * This script performs a comprehensive period-by-period analysis for all 2024 dates
 * to ensure complete reconciliation between curtailment_records and historicalBitcoinCalculations.
 * 
 * It checks each date and highlights any periods that might be missing from the Bitcoin calculations.
 */

import { db } from "./db";
import { curtailmentRecords, historicalBitcoinCalculations } from "./db/schema";
import { eq, and, sql, between } from "drizzle-orm";
import { format } from "date-fns";

// Constants
const START_DATE = '2024-01-01';
const END_DATE = '2024-12-31';
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

interface DatePeriodCoverage {
  date: string;
  curtailmentPeriods: number[];
  calculationPeriods: {
    [key: string]: number[]; // key is miner model
  };
  missingPeriods: {
    [key: string]: number[]; // key is miner model
  };
  hasIssues: boolean;
}

async function verifyPeriodCoverage() {
  console.log('=== Verifying Period Coverage for All 2024 Dates ===');
  
  // Get all dates with curtailment records in 2024
  const datesResult = await db
    .select({
      date: curtailmentRecords.settlementDate
    })
    .from(curtailmentRecords)
    .where(
      between(curtailmentRecords.settlementDate, START_DATE, END_DATE)
    )
    .groupBy(curtailmentRecords.settlementDate)
    .orderBy(curtailmentRecords.settlementDate);
  
  const dates = datesResult.map(row => format(row.date, 'yyyy-MM-dd'));
  console.log(`Found ${dates.length} dates in 2024 with curtailment records`);
  
  // Track issues
  const datesWithIssues: DatePeriodCoverage[] = [];
  let totalIssues = 0;
  
  // Process each date
  for (let i = 0; i < dates.length; i++) {
    const date = dates[i];
    const progress = ((i + 1) / dates.length * 100).toFixed(1);
    
    // Get all periods with curtailment for this date
    const curtailmentPeriodsResult = await db
      .select({
        period: curtailmentRecords.settlementPeriod
      })
      .from(curtailmentRecords)
      .where(
        and(
          eq(curtailmentRecords.settlementDate, date),
          sql`ABS(volume::numeric) > 0`
        )
      )
      .groupBy(curtailmentRecords.settlementPeriod)
      .orderBy(curtailmentRecords.settlementPeriod);
    
    const curtailmentPeriods = curtailmentPeriodsResult.map(row => row.period);
    
    if (curtailmentPeriods.length === 0) {
      console.log(`[${progress}%] Skipping ${date}: No curtailment records with volume`);
      continue;
    }
    
    // Check Bitcoin calculation coverage for each miner model
    const coverage: DatePeriodCoverage = {
      date,
      curtailmentPeriods,
      calculationPeriods: {},
      missingPeriods: {},
      hasIssues: false
    };
    
    for (const minerModel of MINER_MODELS) {
      // Get periods with Bitcoin calculations for this date and model
      const calculationPeriodsResult = await db
        .select({
          period: historicalBitcoinCalculations.settlementPeriod
        })
        .from(historicalBitcoinCalculations)
        .where(
          and(
            eq(historicalBitcoinCalculations.settlementDate, date),
            eq(historicalBitcoinCalculations.minerModel, minerModel)
          )
        )
        .groupBy(historicalBitcoinCalculations.settlementPeriod)
        .orderBy(historicalBitcoinCalculations.settlementPeriod);
      
      const calculationPeriods = calculationPeriodsResult.map(row => row.period);
      coverage.calculationPeriods[minerModel] = calculationPeriods;
      
      // Find missing periods
      const missingPeriods = curtailmentPeriods.filter(period => !calculationPeriods.includes(period));
      coverage.missingPeriods[minerModel] = missingPeriods;
      
      if (missingPeriods.length > 0) {
        coverage.hasIssues = true;
        totalIssues += missingPeriods.length;
      }
    }
    
    if (coverage.hasIssues) {
      datesWithIssues.push(coverage);
      console.log(`[${progress}%] ${date}: Found issues with ${Object.values(coverage.missingPeriods).flat().length} periods`);
    } else {
      console.log(`[${progress}%] ${date}: ✓ All ${curtailmentPeriods.length} periods reconciled`);
    }
  }
  
  // Print summary
  console.log('\n=== Verification Complete ===');
  console.log(`Total dates checked: ${dates.length}`);
  console.log(`Dates with issues: ${datesWithIssues.length}`);
  console.log(`Total missing periods: ${totalIssues}`);
  
  // Print details for dates with issues
  if (datesWithIssues.length > 0) {
    console.log('\n=== Detailed Issues ===');
    
    datesWithIssues.forEach(coverage => {
      console.log(`\nDate: ${coverage.date}`);
      console.log(`Curtailment periods: ${coverage.curtailmentPeriods.join(', ')}`);
      
      for (const minerModel of MINER_MODELS) {
        if (coverage.missingPeriods[minerModel].length > 0) {
          console.log(`${minerModel}: Missing ${coverage.missingPeriods[minerModel].length} periods: ${coverage.missingPeriods[minerModel].join(', ')}`);
        }
      }
    });
  } else {
    console.log('\n✓ All dates fully reconciled for all miner models and periods');
  }
}

// Run the verification
verifyPeriodCoverage()
  .then(() => {
    console.log('\nVerification completed');
    process.exit(0);
  })
  .catch(error => {
    console.error('Error during verification:', error);
    process.exit(1);
  });