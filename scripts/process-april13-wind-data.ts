/**
 * Wind Generation Data Processing Script for 2025-04-13
 * 
 * This script fetches, processes, and stores wind generation data for April 13, 2025
 * to fix incorrect displaying of "Actual Generation: 100.0% Curtailed Volume: 0.0%"
 * 
 * Run with: npx tsx scripts/process-april13-wind-data.ts
 */

import { processSingleDate, getWindGenerationDataForDate, hasWindDataForDate } from '../server/services/windGenerationService';
import { logger } from '../server/utils/logger';
import { db } from '../db';
import { sql } from 'drizzle-orm';
import { curtailmentRecords } from '../db/schema';
import { eq } from 'drizzle-orm';

// The date to process
const TARGET_DATE = '2025-04-13';

/**
 * Main function to process wind generation data
 */
async function processWindData() {
  try {
    console.log(`\n=== Processing Wind Generation Data for ${TARGET_DATE} ===\n`);
    
    // Step 1: Check if data already exists
    const hasData = await hasWindDataForDate(TARGET_DATE);
    
    if (hasData) {
      console.log(`Wind generation data already exists for ${TARGET_DATE}`);
      console.log('Clearing existing data for reprocessing...');
      
      // Remove existing data
      await db.execute(sql`DELETE FROM wind_generation_data WHERE settlement_date = ${TARGET_DATE}::date`);
      console.log('Existing wind generation data cleared successfully.');
    }
    
    // Step 2: Fetch curtailment data for comparison
    const curtailmentStats = await db
      .select({
        recordCount: sql<number>`COUNT(*)`,
        periodCount: sql<number>`COUNT(DISTINCT ${curtailmentRecords.settlementPeriod})`,
        farmCount: sql<number>`COUNT(DISTINCT ${curtailmentRecords.farmId})`,
        totalVolume: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
        totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log(`Curtailment data for ${TARGET_DATE}:`);
    console.log(`Records: ${curtailmentStats[0]?.recordCount || 0}`);
    console.log(`Settlement Periods: ${curtailmentStats[0]?.periodCount || 0}`);
    console.log(`Total Volume: ${Number(curtailmentStats[0]?.totalVolume || 0).toFixed(2)} MWh`);
    console.log(`Total Payment: £${Number(curtailmentStats[0]?.totalPayment || 0).toFixed(2)}`);
    
    // Step 3: Process wind generation data
    console.log('\nProcessing wind generation data from Elexon...');
    const recordsProcessed = await processSingleDate(TARGET_DATE);
    console.log(`Processed ${recordsProcessed} wind generation records for ${TARGET_DATE}`);
    
    // Step 4: Verify the results
    const windData = await getWindGenerationDataForDate(TARGET_DATE);
    console.log(`\nRetrieved ${windData.length} wind generation records for ${TARGET_DATE}`);
    
    if (windData.length > 0) {
      // Calculate total wind generation
      let totalGeneration = 0;
      for (const record of windData) {
        totalGeneration += parseFloat(record.totalWind);
      }
      
      console.log(`Total wind generation: ${totalGeneration.toFixed(2)} MWh`);
      
      // Calculate curtailment percentage
      const curtailedVolume = Number(curtailmentStats[0]?.totalVolume || 0);
      const curtailmentPercentage = (curtailedVolume / (totalGeneration + curtailedVolume)) * 100;
      
      console.log('\nUpdated percentages:');
      console.log(`Actual Generation: ${(100 - curtailmentPercentage).toFixed(2)}%`);
      console.log(`Curtailed Volume: ${curtailmentPercentage.toFixed(2)}%`);
      
      // Print some sample data
      console.log('\nSample Wind Generation Data:');
      const sampleSize = Math.min(5, windData.length);
      for (let i = 0; i < sampleSize; i++) {
        const record = windData[i];
        console.log(`Period ${record.settlementPeriod}: ${record.totalWind} MWh (Onshore: ${record.windOnshore} MWh, Offshore: ${record.windOffshore} MWh)`);
      }
    } else {
      console.log('No wind generation data retrieved after processing');
    }
    
    console.log(`\n=== Wind Data Processing Complete for ${TARGET_DATE} ===`);
    
  } catch (error) {
    console.error('Error processing wind generation data:', error);
    process.exit(1);
  }
}

// Run the processing
processWindData();