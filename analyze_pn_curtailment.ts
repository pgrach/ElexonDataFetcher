/**
 * Physical Notification (PN) and Curtailment Analysis
 * 
 * This script analyzes the relationship between Physical Notification (PN) data
 * and curtailment events to provide insights into potential production versus
 * actual curtailment percentages.
 * 
 * Usage:
 *   npx tsx analyze_pn_curtailment.ts [year-month]
 * 
 * Example:
 *   npx tsx analyze_pn_curtailment.ts 2025-02
 */

import { db } from "./db";
import { curtailmentRecords, physicalNotifications } from "./db/schema";
import { sql, eq, and, between, desc, count } from "drizzle-orm";
import * as fs from "fs";

// Get the target month from command line
const yearMonth = process.argv[2] || "2025-02";

// Helper function to format numbers with commas
function formatNumber(num: number | string, decimals = 0): string {
  const parsedNum = typeof num === 'string' ? parseFloat(num) : num;
  return parsedNum.toLocaleString('en-US', { 
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals
  });
}

// Helper function to calculate percentage
function calculatePercentage(value: number, total: number): string {
  if (total === 0) return "0.00%";
  return ((value / total) * 100).toFixed(2) + "%";
}

// Function to get date range for a month
function getMonthDateRange(yearMonth: string): { startDate: string, endDate: string } {
  const [year, month] = yearMonth.split('-').map(part => parseInt(part));
  const startDate = new Date(year, month - 1, 1);
  const endDate = new Date(year, month, 0);
  
  return {
    startDate: startDate.toISOString().split('T')[0],
    endDate: endDate.toISOString().split('T')[0]
  };
}

async function getTopCurtailedBMUs(startDate: string, endDate: string, limit = 10): Promise<any[]> {
  const results = await db.select({
    farmId: curtailmentRecords.farmId,
    totalCurtailedMWh: sql<number>`SUM(volume)`,
    leadPartyName: curtailmentRecords.leadPartyName,
    recordCount: count()
  })
  .from(curtailmentRecords)
  .where(
    and(
      between(curtailmentRecords.settlementDate, startDate, endDate),
      sql`volume > 0`
    )
  )
  .groupBy(curtailmentRecords.farmId, curtailmentRecords.leadPartyName)
  .orderBy(desc(sql`SUM(volume)`))
  .limit(limit);
  
  return results;
}

async function getBMUAnalysis(farmId: string, startDate: string, endDate: string): Promise<any> {
  // Get PN data for this BMU in the date range
  const pnData = await db.select({
    sumLevelFrom: sql<number>`SUM(level_from)`,
    sumLevelTo: sql<number>`SUM(level_to)`,
    recordCount: count()
  })
  .from(physicalNotifications)
  .where(
    and(
      eq(physicalNotifications.bmUnit, farmId), // We're matching BMU in PN table with farmId in curtailment table
      between(physicalNotifications.settlementDate, startDate, endDate)
    )
  );
  
  // Get curtailment data for this farm in the date range
  const curtailmentData = await db.select({
    totalCurtailedMWh: sql<number>`SUM(volume)`,
    averagePrice: sql<number>`AVG(final_price)`,
    totalPayment: sql<number>`SUM(volume * final_price)`,
    recordCount: count()
  })
  .from(curtailmentRecords)
  .where(
    and(
      eq(curtailmentRecords.farmId, farmId),
      between(curtailmentRecords.settlementDate, startDate, endDate)
    )
  );
  
  // Calculate average PN level across all periods
  const averagePNLevel = pnData[0].recordCount > 0 
    ? (pnData[0].sumLevelFrom + pnData[0].sumLevelTo) / (2 * pnData[0].recordCount) 
    : 0;
  
  // Calculate total potential MWh (based on average PN level * 0.5 hours per settlement period)
  const totalPotentialMWh = averagePNLevel * pnData[0].recordCount * 0.5;
  
  // Calculate curtailment percentage
  const curtailedMWh = curtailmentData[0].totalCurtailedMWh || 0;
  const curtailmentPercentage = totalPotentialMWh > 0 
    ? (curtailedMWh / totalPotentialMWh) * 100 
    : 0;
  
  return {
    farmId, // Using farmId instead of bmUnit
    pnData: {
      averageLevel: averagePNLevel,
      recordCount: pnData[0].recordCount,
      estimatedPotentialMWh: totalPotentialMWh
    },
    curtailmentData: {
      totalCurtailedMWh: curtailedMWh,
      averagePrice: curtailmentData[0].averagePrice || 0,
      totalPayment: curtailmentData[0].totalPayment || 0,
      recordCount: curtailmentData[0].recordCount
    },
    analysis: {
      curtailmentPercentage,
      valuePerMWh: curtailedMWh > 0 ? (curtailmentData[0].totalPayment || 0) / curtailedMWh : 0
    }
  };
}

async function getMonthlyOverview(startDate: string, endDate: string): Promise<any> {
  // Total curtailment for the month
  const curtailmentOverview = await db.select({
    totalCurtailedMWh: sql<number>`SUM(volume)`,
    totalPayment: sql<number>`SUM(volume * final_price)`,
    uniqueBMUs: sql<number>`COUNT(DISTINCT bm_unit)`,
    uniqueLeadParties: sql<number>`COUNT(DISTINCT lead_party_name)`,
    recordCount: count()
  })
  .from(curtailmentRecords)
  .where(
    and(
      between(curtailmentRecords.settlementDate, startDate, endDate),
      sql`volume > 0`
    )
  );
  
  // Total PN data for the month
  const pnOverview = await db.select({
    recordCount: count(),
    uniqueBMUs: sql<number>`COUNT(DISTINCT bm_unit)`
  })
  .from(physicalNotifications)
  .where(
    between(physicalNotifications.settlementDate, startDate, endDate)
  );
  
  return {
    curtailment: {
      totalCurtailedMWh: curtailmentOverview[0].totalCurtailedMWh || 0,
      totalPayment: curtailmentOverview[0].totalPayment || 0,
      uniqueBMUs: curtailmentOverview[0].uniqueBMUs || 0,
      uniqueLeadParties: curtailmentOverview[0].uniqueLeadParties || 0,
      recordCount: curtailmentOverview[0].recordCount || 0,
      averagePaymentPerMWh: curtailmentOverview[0].totalCurtailedMWh > 0 
        ? curtailmentOverview[0].totalPayment / curtailmentOverview[0].totalCurtailedMWh 
        : 0
    },
    physicalNotifications: {
      recordCount: pnOverview[0].recordCount || 0,
      uniqueBMUs: pnOverview[0].uniqueBMUs || 0,
      dataCompleteness: pnOverview[0].uniqueBMUs > 0 
        ? pnOverview[0].uniqueBMUs / (curtailmentOverview[0].uniqueBMUs || 1) * 100 
        : 0
    }
  };
}

async function main() {
  console.log(`\n=== Physical Notification & Curtailment Analysis ===`);
  console.log(`Analyzing data for: ${yearMonth}`);
  
  // Get date range for the target month
  const { startDate, endDate } = getMonthDateRange(yearMonth);
  console.log(`Date range: ${startDate} to ${endDate}`);
  
  // Get monthly overview
  console.log(`\nFetching monthly overview...`);
  const overview = await getMonthlyOverview(startDate, endDate);
  
  // Get top curtailed BMUs
  console.log(`Fetching top curtailed BMUs...`);
  const topBMUs = await getTopCurtailedBMUs(startDate, endDate, 10);
  
  // Analyze each top BMU
  console.log(`Analyzing individual BMUs...`);
  const bmuAnalyses: any[] = [];
  
  for (const bmu of topBMUs) {
    console.log(`- Analyzing ${bmu.farmId} (${bmu.leadPartyName || 'Unknown'})...`);
    const analysis = await getBMUAnalysis(bmu.farmId, startDate, endDate);
    bmuAnalyses.push({
      ...analysis,
      leadPartyName: bmu.leadPartyName
    });
  }
  
  // Print summary
  console.log(`\n=== Monthly Overview (${yearMonth}) ===`);
  console.log(`Total Curtailed Energy: ${formatNumber(overview.curtailment.totalCurtailedMWh, 2)} MWh`);
  console.log(`Total Curtailment Payment: £${formatNumber(overview.curtailment.totalPayment, 2)}`);
  console.log(`Average Payment Rate: £${formatNumber(overview.curtailment.averagePaymentPerMWh, 2)}/MWh`);
  console.log(`Unique BMUs Curtailed: ${overview.curtailment.uniqueBMUs}`);
  console.log(`Unique Lead Parties: ${overview.curtailment.uniqueLeadParties}`);
  console.log(`PN Data Coverage: ${overview.physicalNotifications.uniqueBMUs} BMUs (${overview.physicalNotifications.dataCompleteness.toFixed(1)}% of curtailed BMUs)`);
  console.log(`PN Records: ${formatNumber(overview.physicalNotifications.recordCount)}`);
  
  // Print BMU analysis
  console.log(`\n=== Top 10 Curtailed BMUs Analysis ===`);
  console.log(`BMU ID | Lead Party | Potential (MWh) | Curtailed (MWh) | Curtailed % | Avg Price (£/MWh)`);
  console.log(`----- | ---------- | --------------- | --------------- | ----------- | --------------`);
  
  for (const bmu of bmuAnalyses) {
    console.log(
      `${bmu.bmUnit} | ` +
      `${bmu.leadPartyName || 'Unknown'} | ` +
      `${formatNumber(bmu.pnData.estimatedPotentialMWh, 1)} | ` +
      `${formatNumber(bmu.curtailmentData.totalCurtailedMWh, 1)} | ` +
      `${bmu.analysis.curtailmentPercentage.toFixed(1)}% | ` +
      `£${formatNumber(bmu.analysis.valuePerMWh, 2)}`
    );
  }
  
  // Save detailed report to file
  const report = {
    overview,
    topBMUs: bmuAnalyses
  };
  
  const filename = `pn_curtailment_analysis_${yearMonth}.json`;
  await fs.promises.writeFile(filename, JSON.stringify(report, null, 2));
  console.log(`\nDetailed analysis saved to ${filename}`);
}

// Run the analysis
main()
  .then(() => {
    console.log('\nAnalysis completed successfully');
    process.exit(0);
  })
  .catch(error => {
    console.error('Error during analysis:', error);
    process.exit(1);
  });