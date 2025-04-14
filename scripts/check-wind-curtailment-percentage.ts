/**
 * Wind Curtailment Percentage Checker
 * 
 * This script checks and displays the relationship between wind generation and curtailment
 * for a specific date, showing the correct percentage displays.
 * 
 * Run with: npx tsx scripts/check-wind-curtailment-percentage.ts YYYY-MM-DD
 */

import { getWindGenerationDataForDate, hasWindDataForDate } from '../server/services/windGenerationService';
import { db } from '../db';
import { sql } from 'drizzle-orm';
import { curtailmentRecords } from '../db/schema';
import { eq } from 'drizzle-orm';
import { isValidDateString } from '../server/utils/dates';

// Get date from command line argument
const TARGET_DATE = process.argv[2];

/**
 * Check wind curtailment percentages for a specific date
 */
async function checkWindCurtailmentPercentage(date: string) {
  try {
    if (!date || !isValidDateString(date)) {
      console.error('Please provide a valid date in YYYY-MM-DD format');
      process.exit(1);
    }
    
    console.log(`\n=== Wind Curtailment Check for ${date} ===\n`);
    
    // Step 1: Check if wind data exists
    const hasWindData = await hasWindDataForDate(date);
    
    if (!hasWindData) {
      console.log(`No wind generation data found for ${date}`);
      console.log('Please run process-wind-data-range.ts first to fetch wind generation data.');
      return;
    }
    
    // Step 2: Get curtailment data
    const curtailmentStats = await db
      .select({
        recordCount: sql<number>`COUNT(*)`,
        periodCount: sql<number>`COUNT(DISTINCT ${curtailmentRecords.settlementPeriod})`,
        farmCount: sql<number>`COUNT(DISTINCT ${curtailmentRecords.farmId})`,
        totalVolume: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
        totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date));
    
    const curtailedVolume = Number(curtailmentStats[0]?.totalVolume || 0);
    
    if (curtailedVolume === 0) {
      console.log(`No curtailment records found for ${date}`);
      console.log('Cannot calculate percentages without curtailment data.');
      return;
    }
    
    // Step 3: Get wind generation data
    const windData = await getWindGenerationDataForDate(date);
    
    if (windData.length === 0) {
      console.log(`No wind generation data found for ${date} despite hasWindData returning true.`);
      console.log('This suggests a database inconsistency. Please check the data.');
      return;
    }
    
    // Step 4: Calculate totals
    console.log(`Curtailment Records: ${curtailmentStats[0]?.recordCount || 0}`);
    console.log(`Settlement Periods with Curtailment: ${curtailmentStats[0]?.periodCount || 0}`);
    console.log(`Farms with Curtailment: ${curtailmentStats[0]?.farmCount || 0}`);
    console.log(`Total Curtailed Volume: ${curtailedVolume.toFixed(2)} MWh`);
    console.log(`Total Curtailment Payment: £${Math.abs(Number(curtailmentStats[0]?.totalPayment || 0)).toFixed(2)}`);
    
    console.log(`\nWind Generation Records: ${windData.length}`);
    
    // Calculate total wind generation
    let totalGeneration = 0;
    for (const record of windData) {
      totalGeneration += parseFloat(record.totalWind);
    }
    
    console.log(`Total Wind Generation: ${totalGeneration.toFixed(2)} MWh`);
    
    // Step 5: Calculate and display percentages
    const totalPotential = totalGeneration + curtailedVolume;
    const curtailmentPercentage = (curtailedVolume / totalPotential) * 100;
    const generationPercentage = 100 - curtailmentPercentage;
    
    console.log(`\nWind Farm Percentages for ${date}:`);
    console.log(`Actual Generation: ${generationPercentage.toFixed(2)}%`);
    console.log(`Curtailed Volume: ${curtailmentPercentage.toFixed(2)}%`);
    
    // Step 6: Display period breakdown if significant curtailment
    if (curtailmentPercentage > 10) {
      console.log('\nSignificant curtailment detected. Top curtailment periods:');
      
      const topPeriods = await db
        .select({
          settlementPeriod: curtailmentRecords.settlementPeriod,
          recordCount: sql<number>`COUNT(*)`,
          totalVolume: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
          totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
        })
        .from(curtailmentRecords)
        .where(eq(curtailmentRecords.settlementDate, date))
        .groupBy(curtailmentRecords.settlementPeriod)
        .orderBy(sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))` as any, 'desc')
        .limit(5);
      
      for (const period of topPeriods) {
        console.log(`Period ${period.settlementPeriod}: ${Number(period.totalVolume).toFixed(2)} MWh (${period.recordCount} records)`);
      }
    }
    
    console.log(`\n=== Check Complete for ${date} ===`);
    
  } catch (error) {
    console.error('Error checking wind curtailment percentages:', error);
    process.exit(1);
  }
}

// Run the check with command line arg
checkWindCurtailmentPercentage(TARGET_DATE);