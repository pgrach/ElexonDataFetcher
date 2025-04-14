/**
 * Wind Generation Data Processing Script for Date Range
 * 
 * This script fetches, processes, and stores wind generation data for a specified date range
 * to fix incorrect displaying of "Actual Generation: 100.0% Curtailed Volume: 0.0%"
 * 
 * Run with: npx tsx scripts/process-wind-data-range.ts 2025-04-12 2025-04-14
 */

import { processDateRange, getWindGenerationDataForDate, hasWindDataForDate } from '../server/services/windGenerationService';
import { logger } from '../server/utils/logger';
import { db } from '../db';
import { sql } from 'drizzle-orm';
import { curtailmentRecords } from '../db/schema';
import { eq, and, between } from 'drizzle-orm';
import { isValidDateString, formatDate } from '../server/utils/dates';
import { addDays, parseISO, format } from 'date-fns';

// Get dates from command line arguments
const START_DATE = process.argv[2] || '';
const END_DATE = process.argv[3] || START_DATE;

/**
 * Validate date inputs
 */
function validateDates(startDate: string, endDate: string): boolean {
  if (!isValidDateString(startDate) || !isValidDateString(endDate)) {
    console.error('Invalid date format. Use YYYY-MM-DD format.');
    return false;
  }
  
  const start = new Date(startDate);
  const end = new Date(endDate);
  
  if (start > end) {
    console.error('Start date must be before or equal to end date');
    return false;
  }
  
  return true;
}

/**
 * Main function to process wind generation data for a date range
 */
async function processWindDataRange(startDate: string, endDate: string) {
  try {
    // Validate dates
    if (!validateDates(startDate, endDate)) {
      process.exit(1);
    }
    
    console.log(`\n=== Processing Wind Generation Data for ${startDate} to ${endDate} ===\n`);
    
    // Step 1: Fetch curtailment data for comparison
    const curtailmentStats = await db
      .select({
        date: curtailmentRecords.settlementDate,
        recordCount: sql<number>`COUNT(*)`,
        periodCount: sql<number>`COUNT(DISTINCT ${curtailmentRecords.settlementPeriod})`,
        farmCount: sql<number>`COUNT(DISTINCT ${curtailmentRecords.farmId})`,
        totalVolume: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
        totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(
        and(
          between(
            curtailmentRecords.settlementDate, 
            startDate, 
            endDate
          )
        )
      )
      .groupBy(curtailmentRecords.settlementDate)
      .orderBy(curtailmentRecords.settlementDate);
    
    console.log(`Found curtailment data for ${curtailmentStats.length} days`);
    
    // No dates found, exit early
    if (curtailmentStats.length === 0) {
      console.log(`No curtailment data found for the date range ${startDate} to ${endDate}`);
      return;
    }
    
    // Step 2: Check which dates already have wind data
    let datesToProcess: string[] = [];
    let skippedDates: string[] = [];
    
    // Create array of all dates in range
    const start = parseISO(startDate);
    const end = parseISO(endDate);
    let current = start;
    
    while (current <= end) {
      const formattedDate = format(current, 'yyyy-MM-dd');
      
      // Only process dates that have curtailment data
      const hasCurtailmentData = curtailmentStats.some(stat => stat.date === formattedDate);
      
      if (hasCurtailmentData) {
        // Check if wind data already exists
        const hasWindData = await hasWindDataForDate(formattedDate);
        
        if (hasWindData) {
          console.log(`Clearing existing wind data for ${formattedDate}`);
          await db.execute(sql`DELETE FROM wind_generation_data WHERE settlement_date = ${formattedDate}::date`);
          console.log(`Existing wind generation data cleared for ${formattedDate}`);
        }
        
        datesToProcess.push(formattedDate);
      } else {
        skippedDates.push(formattedDate);
      }
      
      current = addDays(current, 1);
    }
    
    console.log(`\nDates to process: ${datesToProcess.join(', ')}`);
    console.log(`Dates skipped (no curtailment data): ${skippedDates.join(', ') || 'None'}`);
    
    // Step 3: Process wind generation data for all dates at once
    console.log('\nProcessing wind generation data from Elexon...');
    const recordsProcessed = await processDateRange(startDate, endDate);
    console.log(`Processed ${recordsProcessed} wind generation records for date range`);
    
    // Step 4: Show results for each date
    for (const date of datesToProcess) {
      try {
        const curtailmentData = curtailmentStats.find(stat => stat.date === date);
        
        if (!curtailmentData) continue;
        
        const windData = await getWindGenerationDataForDate(date);
        
        console.log(`\n--- ${date} ---`);
        console.log(`Curtailment: ${curtailmentData.recordCount} records, ${Number(curtailmentData.totalVolume).toFixed(2)} MWh`);
        console.log(`Wind generation: ${windData.length} records`);
        
        if (windData.length > 0) {
          // Calculate total wind generation
          let totalGeneration = 0;
          for (const record of windData) {
            totalGeneration += parseFloat(record.totalWind);
          }
          
          console.log(`Total wind generation: ${totalGeneration.toFixed(2)} MWh`);
          
          // Calculate curtailment percentage
          const curtailedVolume = Number(curtailmentData.totalVolume || 0);
          const curtailmentPercentage = (curtailedVolume / (totalGeneration + curtailedVolume)) * 100;
          
          console.log(`Updated percentages: Actual Generation: ${(100 - curtailmentPercentage).toFixed(2)}%, Curtailed Volume: ${curtailmentPercentage.toFixed(2)}%`);
        } else {
          console.log('No wind generation data retrieved after processing');
        }
      } catch (error) {
        console.error(`Error processing results for ${date}:`, error);
      }
    }
    
    console.log(`\n=== Wind Data Processing Complete for ${startDate} to ${endDate} ===`);
    
  } catch (error) {
    console.error('Error processing wind generation data:', error);
    process.exit(1);
  }
}

// Run the processing with command line args
processWindDataRange(START_DATE, END_DATE);