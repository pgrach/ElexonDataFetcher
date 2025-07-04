import { db } from '../db/index.js';
import { curtailmentRecords } from '../db/schema.js';
import { fetchBidsOffers } from '../server/services/elexon.js';
import { eq, and } from 'drizzle-orm';

interface IngestionResult {
  date: string;
  totalRecordsIngested: number;
  periodsProcessed: number[];
  totalVolume: number;
  totalPayment: number;
  errors: string[];
}

async function ingestMissingData(date: string): Promise<IngestionResult> {
  console.log(`\n=== COMPREHENSIVE INGESTION FOR ${date} ===`);
  
  const result: IngestionResult = {
    date,
    totalRecordsIngested: 0,
    periodsProcessed: [],
    totalVolume: 0,
    totalPayment: 0,
    errors: []
  };

  // Process all 48 settlement periods
  for (let period = 1; period <= 48; period++) {
    try {
      console.log(`Processing period ${period}...`);
      
      // First check if we already have data for this period
      const existingRecords = await db.select()
        .from(curtailmentRecords)
        .where(
          and(
            eq(curtailmentRecords.settlementDate, date),
            eq(curtailmentRecords.settlementPeriod, period)
          )
        );

      // Fetch API data for this period
      const apiData = await fetchBidsOffers(date, period);
      
      if (apiData.length === 0) {
        // No data for this period, skip
        continue;
      }

      const newRecords = apiData.length - existingRecords.length;
      
      if (newRecords > 0) {
        // Transform API data to database format
        const recordsToInsert = apiData.map(record => ({
          settlementDate: date,
          settlementPeriod: period,
          farmId: record.id, // Use id as farmId
          leadPartyName: record.leadPartyName,
          volume: record.volume.toString(),
          payment: (Math.abs(record.volume) * Math.abs(record.originalPrice)).toString(),
          originalPrice: record.originalPrice.toString(),
          finalPrice: record.finalPrice.toString(),
          soFlag: record.soFlag,
          cadlFlag: record.cadlFlag,
          createdAt: new Date()
        }));

        // Insert records in batches
        const batchSize = 100;
        for (let i = 0; i < recordsToInsert.length; i += batchSize) {
          const batch = recordsToInsert.slice(i, i + batchSize);
          await db.insert(curtailmentRecords).values(batch);
        }

        // Calculate totals for this period
        const periodVolume = apiData.reduce((sum, record) => sum + Math.abs(record.volume), 0);
        const periodPayment = apiData.reduce((sum, record) => sum + (Math.abs(record.volume) * Math.abs(record.originalPrice)), 0);
        
        result.totalRecordsIngested += recordsToInsert.length;
        result.periodsProcessed.push(period);
        result.totalVolume += periodVolume;
        result.totalPayment += periodPayment;
        
        console.log(`Period ${period}: Ingested ${recordsToInsert.length} records (${periodVolume.toFixed(2)} MWh, £${periodPayment.toFixed(2)})`);
      } else {
        console.log(`Period ${period}: No new records needed (${existingRecords.length} already exist)`);
      }
      
    } catch (error) {
      const errorMsg = `Error processing period ${period}: ${error}`;
      console.error(errorMsg);
      result.errors.push(errorMsg);
    }
  }

  return result;
}

async function updateDailySummary(date: string, totalVolume: number, totalPayment: number) {
  console.log(`\nUpdating daily summary for ${date}...`);
  
  // Check if daily summary exists
  const { dailySummaries } = await import('../db/schema.js');
  
  const existingSummary = await db.select()
    .from(dailySummaries)
    .where(eq(dailySummaries.summaryDate, date));

  if (existingSummary.length > 0) {
    // Update existing summary
    await db.update(dailySummaries)
      .set({
        totalCurtailedEnergy: totalVolume.toString(),
        totalPayment: totalPayment.toString(),
        lastUpdated: new Date()
      })
      .where(eq(dailySummaries.summaryDate, date));
    console.log(`Updated existing daily summary`);
  } else {
    // Create new summary
    await db.insert(dailySummaries).values({
      summaryDate: date,
      totalCurtailedEnergy: totalVolume.toString(),
      totalPayment: totalPayment.toString(),
      createdAt: new Date(),
      lastUpdated: new Date()
    });
    console.log(`Created new daily summary`);
  }
}

async function main() {
  try {
    const result = await ingestMissingData('2025-06-30');
    
    console.log('\n=== INGESTION RESULTS ===');
    console.log(`Date: ${result.date}`);
    console.log(`Total Records Ingested: ${result.totalRecordsIngested}`);
    console.log(`Periods Processed: ${result.periodsProcessed.length}`);
    console.log(`Total Volume: ${result.totalVolume.toFixed(2)} MWh`);
    console.log(`Total Payment: £${result.totalPayment.toFixed(2)}`);
    
    if (result.errors.length > 0) {
      console.log('\n=== ERRORS ===');
      result.errors.forEach(error => console.error(error));
    }
    
    if (result.totalRecordsIngested > 0) {
      // Update daily summary
      await updateDailySummary(result.date, result.totalVolume, result.totalPayment);
      
      console.log('\n✅ INGESTION COMPLETE');
      console.log(`Successfully ingested ${result.totalRecordsIngested} records from ${result.periodsProcessed.length} periods`);
    } else {
      console.log('\n✅ NO ACTION REQUIRED');
      console.log('All data is already complete');
    }
    
  } catch (error) {
    console.error('Ingestion failed:', error);
  }
}

main();