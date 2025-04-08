/**
 * Verify Data Integrity Between Tables
 * 
 * This script verifies that the curtailment_records and daily_summaries tables
 * have consistent data, particularly focusing on payment calculations.
 */

import { db } from './db';
import { curtailmentRecords, dailySummaries, 
         historicalBitcoinCalculations, bitcoinMonthlySummaries } from './db/schema';
import { eq, sql } from 'drizzle-orm';

/**
 * Verify the data integrity between curtailment_records and daily_summaries 
 * for a specific date
 */
async function verifyDataIntegrity(date: string): Promise<{
  curtailmentRecords: {
    total: number;
    totalVolume: number;
    totalPayment: number;
  },
  dailySummary: {
    totalCurtailedEnergy: number;
    totalPayment: number;
  },
  bitcoin: {
    totalRecords: number;
    S19J_PRO: number;
    S9: number;
    M20S: number;
  },
  match: boolean;
}> {
  try {
    console.log(`\n=== Verifying Data Integrity for ${date} ===\n`);
    
    // Check curtailment records
    const curtailmentTotals = await db.select({
      total: sql<number>`COUNT(*)`,
      totalVolume: sql<number>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
      totalPayment: sql<number>`SUM(${curtailmentRecords.payment}::numeric)`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date));
    
    const curtailmentTotal = Number(curtailmentTotals[0]?.total || 0);
    const curtailmentVolume = Number(curtailmentTotals[0]?.totalVolume || 0);
    const curtailmentPayment = Number(curtailmentTotals[0]?.totalPayment || 0);
    
    console.log(`Curtailment Records Analysis:`);
    console.log(`- Total Records: ${curtailmentTotal}`);
    console.log(`- Total Volume: ${curtailmentVolume.toFixed(2)} MWh`);
    console.log(`- Total Payment: £${curtailmentPayment.toFixed(2)}`);
    
    // Check daily summary
    const dailySummaryData = await db.query.dailySummaries.findFirst({
      where: eq(dailySummaries.summaryDate, date)
    });
    
    let dailySummaryEnergy = 0;
    let dailySummaryPayment = 0;
    
    if (dailySummaryData) {
      dailySummaryEnergy = Number(dailySummaryData.totalCurtailedEnergy);
      dailySummaryPayment = Number(dailySummaryData.totalPayment);
      
      console.log(`\nDaily Summary Analysis:`);
      console.log(`- Total Curtailed Energy: ${dailySummaryEnergy.toFixed(2)} MWh`);
      console.log(`- Total Payment: £${dailySummaryPayment.toFixed(2)}`);
    } else {
      console.log(`\nNo daily summary found for ${date}`);
    }
    
    // Bitcoin calculations
    const bitcoinCalculations = await db
      .select({
        totalRecords: sql<number>`COUNT(*)`,
        totalBitcoinMined: sql<number>`SUM(${historicalBitcoinCalculations.bitcoinMined}::numeric)`,
        minerModel: historicalBitcoinCalculations.minerModel
      })
      .from(historicalBitcoinCalculations)
      .where(eq(historicalBitcoinCalculations.settlementDate, date))
      .groupBy(historicalBitcoinCalculations.minerModel);
    
    let totalRecords = 0;
    let s19jProTotal = 0;
    let s9Total = 0;
    let m20sTotal = 0;
    
    for (const calculation of bitcoinCalculations) {
      totalRecords += Number(calculation.totalRecords || 0);
      
      if (calculation.minerModel === 'S19J_PRO') {
        s19jProTotal = Number(calculation.totalBitcoinMined || 0);
      } else if (calculation.minerModel === 'S9') {
        s9Total = Number(calculation.totalBitcoinMined || 0);
      } else if (calculation.minerModel === 'M20S') {
        m20sTotal = Number(calculation.totalBitcoinMined || 0);
      }
    }
    
    console.log(`\nBitcoin Calculations Analysis:`);
    console.log(`- Total Records: ${totalRecords}`);
    console.log(`- S19J_PRO: ${s19jProTotal.toFixed(8)} BTC`);
    console.log(`- S9: ${s9Total.toFixed(8)} BTC`);
    console.log(`- M20S: ${m20sTotal.toFixed(8)} BTC`);
    
    // Check if the values match
    const volumeMatch = Math.abs(curtailmentVolume - dailySummaryEnergy) < 0.01;
    const paymentMatch = Math.abs(curtailmentPayment - dailySummaryPayment) < 0.01;
    const match = volumeMatch && paymentMatch;
    
    console.log(`\nVerification Result:`);
    console.log(`- Volume Match: ${volumeMatch ? 'YES' : 'NO'}`);
    console.log(`- Payment Match: ${paymentMatch ? 'YES' : 'NO'}`);
    console.log(`- Overall Integrity: ${match ? 'VERIFIED' : 'FAILED'}`);
    
    return {
      curtailmentRecords: {
        total: curtailmentTotal,
        totalVolume: curtailmentVolume,
        totalPayment: curtailmentPayment
      },
      dailySummary: {
        totalCurtailedEnergy: dailySummaryEnergy,
        totalPayment: dailySummaryPayment
      },
      bitcoin: {
        totalRecords,
        S19J_PRO: s19jProTotal,
        S9: s9Total,
        M20S: m20sTotal
      },
      match
    };
  } catch (error) {
    console.error('Error verifying data integrity:', error);
    throw error;
  }
}

async function verifyDates() {
  const datesToCheck = ['2025-03-31'];
  
  for (const date of datesToCheck) {
    await verifyDataIntegrity(date);
  }
}

verifyDates();