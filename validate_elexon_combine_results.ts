/**
 * Combine Results from All Batches
 * 
 * This script combines the results from the three batch files
 * to create a comprehensive validation report for all 48 periods.
 */

import fs from 'fs';

interface BatchResult {
  apiData: {
    totalVolume: number;
    totalPayment: number;
    recordCount: number;
    periodCount: number;
    periods: Record<string, number>;
  };
  dbData: {
    totalVolume: number;
    totalPayment: number;
    recordCount: number;
    periodCount: number;
  };
  differences: {
    volumeDiff: number;
    paymentDiff: number;
    recordDiff: number;
    volumePercentDiff: number;
    missingInDb: number[];
    missingInApi: number[];
  };
}

interface CombinedResults {
  apiData: {
    totalVolume: number;
    totalPayment: number;
    recordCount: number;
    periodCount: number;
    periodsWithData: number[];
  };
  dbData: {
    totalVolume: number;
    totalPayment: number;
    recordCount: number;
    periodCount: number;
    periodsWithData: number[];
  };
  differences: {
    volumeDiff: number;
    paymentDiff: number;
    recordDiff: number;
    volumePercentDiff: number;
    missingInDb: number[];
    missingInApi: number[];
  };
}

async function combineResults(): Promise<void> {
  try {
    console.log("Combining results from all three batches...");
    
    // Load results from each batch
    const batch1ResultsRaw = await fs.promises.readFile('batch1_results.json', 'utf8');
    const batch2ResultsRaw = await fs.promises.readFile('batch2_results.json', 'utf8');
    const batch3ResultsRaw = await fs.promises.readFile('batch3_results.json', 'utf8');
    
    const batch1Results: BatchResult = JSON.parse(batch1ResultsRaw);
    const batch2Results: BatchResult = JSON.parse(batch2ResultsRaw);
    const batch3Results: BatchResult = JSON.parse(batch3ResultsRaw);
    
    // Combine API data
    const combinedApiVolume = 
      batch1Results.apiData.totalVolume + 
      batch2Results.apiData.totalVolume + 
      batch3Results.apiData.totalVolume;
      
    const combinedApiPayment = 
      batch1Results.apiData.totalPayment + 
      batch2Results.apiData.totalPayment + 
      batch3Results.apiData.totalPayment;
      
    const combinedApiRecordCount = 
      batch1Results.apiData.recordCount + 
      batch2Results.apiData.recordCount + 
      batch3Results.apiData.recordCount;
      
    const combinedApiPeriodCount = 
      batch1Results.apiData.periodCount + 
      batch2Results.apiData.periodCount + 
      batch3Results.apiData.periodCount;
    
    // Combine DB data
    const combinedDbVolume = 
      batch1Results.dbData.totalVolume + 
      batch2Results.dbData.totalVolume + 
      batch3Results.dbData.totalVolume;
      
    const combinedDbPayment = 
      batch1Results.dbData.totalPayment + 
      batch2Results.dbData.totalPayment + 
      batch3Results.dbData.totalPayment;
      
    const combinedDbRecordCount = 
      batch1Results.dbData.recordCount + 
      batch2Results.dbData.recordCount + 
      batch3Results.dbData.recordCount;
      
    const combinedDbPeriodCount = 
      batch1Results.dbData.periodCount + 
      batch2Results.dbData.periodCount + 
      batch3Results.dbData.periodCount;
      
    // Combine missing periods lists
    const combinedMissingInDb = [
      ...batch1Results.differences.missingInDb,
      ...batch2Results.differences.missingInDb,
      ...batch3Results.differences.missingInDb
    ];
    
    const combinedMissingInApi = [
      ...batch1Results.differences.missingInApi,
      ...batch2Results.differences.missingInApi,
      ...batch3Results.differences.missingInApi
    ];
    
    // Calculate combined differences
    const combinedVolumeDiff = Math.abs(combinedApiVolume - combinedDbVolume);
    const combinedPaymentDiff = Math.abs(combinedApiPayment - Math.abs(combinedDbPayment));
    const combinedRecordDiff = Math.abs(combinedApiRecordCount - combinedDbRecordCount);
    
    let combinedVolumePercentDiff = 0;
    if (combinedApiVolume > 0) {
      combinedVolumePercentDiff = (combinedVolumeDiff / combinedApiVolume) * 100;
    }
    
    // Get periods with data
    const apiPeriodsWithData: number[] = [];
    
    // Helper function to add periods from a batch to the combined list
    const addApiPeriods = (batchResults: BatchResult) => {
      for (const periodStr in batchResults.apiData.periods) {
        const period = parseInt(periodStr);
        if (!apiPeriodsWithData.includes(period)) {
          apiPeriodsWithData.push(period);
        }
      }
    };
    
    addApiPeriods(batch1Results);
    addApiPeriods(batch2Results);
    addApiPeriods(batch3Results);
    
    // Sort periods for better readability
    apiPeriodsWithData.sort((a, b) => a - b);
    combinedMissingInDb.sort((a, b) => a - b);
    combinedMissingInApi.sort((a, b) => a - b);
    
    // Create combined results object
    const combinedResults: CombinedResults = {
      apiData: {
        totalVolume: combinedApiVolume,
        totalPayment: combinedApiPayment,
        recordCount: combinedApiRecordCount,
        periodCount: combinedApiPeriodCount,
        periodsWithData: apiPeriodsWithData
      },
      dbData: {
        totalVolume: combinedDbVolume,
        totalPayment: combinedDbPayment,
        recordCount: combinedDbRecordCount,
        periodCount: combinedDbPeriodCount,
        periodsWithData: Array.from({ length: 48 }, (_, i) => i + 1).filter(p => !combinedMissingInApi.includes(p))
      },
      differences: {
        volumeDiff: combinedVolumeDiff,
        paymentDiff: combinedPaymentDiff,
        recordDiff: combinedRecordDiff,
        volumePercentDiff: combinedVolumePercentDiff,
        missingInDb: combinedMissingInDb,
        missingInApi: combinedMissingInApi
      }
    };
    
    // Write combined results to file
    await fs.promises.writeFile(
      'complete_validation_results.json', 
      JSON.stringify(combinedResults, null, 2)
    );
    
    // Print summary to console
    console.log("\n=== COMPLETE VALIDATION RESULTS ===");
    console.log("\nAPI Data:");
    console.log(`  Total Volume: ${combinedApiVolume.toFixed(2)} MWh`);
    console.log(`  Total Payment: £${combinedApiPayment.toFixed(2)}`);
    console.log(`  Total Records: ${combinedApiRecordCount}`);
    console.log(`  Periods with Data: ${combinedApiPeriodCount}`);
    
    console.log("\nDatabase Data:");
    console.log(`  Total Volume: ${combinedDbVolume.toFixed(2)} MWh`);
    console.log(`  Total Payment: £${combinedDbPayment.toFixed(2)}`);
    console.log(`  Total Records: ${combinedDbRecordCount}`);
    console.log(`  Periods with Data: ${combinedDbPeriodCount}`);
    
    console.log("\nDifferences:");
    console.log(`  Volume Difference: ${combinedVolumeDiff.toFixed(2)} MWh`);
    console.log(`  Payment Difference: £${combinedPaymentDiff.toFixed(2)}`);
    console.log(`  Record Count Difference: ${combinedRecordDiff}`);
    
    if (combinedApiVolume > 0) {
      console.log(`  Volume Percent Difference: ${combinedVolumePercentDiff.toFixed(2)}%`);
      
      if (combinedVolumePercentDiff > 1) {
        console.log("\n⚠️ MAJOR DISCREPANCY DETECTED");
        console.log(`Data in database differs from Elexon API by ${combinedVolumePercentDiff.toFixed(2)}%`);
        console.log("Consider reingesting data from the Elexon API for 2025-04-01");
      } else {
        console.log("\n✓ Validation Passed");
        console.log(`Data difference is within acceptable threshold (${combinedVolumePercentDiff.toFixed(2)}%)`);
      }
    }
    
    if (combinedMissingInDb.length > 0) {
      console.log(`\nPeriods in API but missing in DB: ${combinedMissingInDb.join(', ')}`);
    }
    
    if (combinedMissingInApi.length > 0) {
      console.log(`\nPeriods in DB but missing in API: ${combinedMissingInApi.join(', ')}`);
    }
    
    console.log("\nFull results written to complete_validation_results.json");
    
  } catch (error) {
    console.error("Error combining results:", error);
  }
}

async function main(): Promise<void> {
  try {
    await combineResults();
    process.exit(0);
  } catch (error) {
    console.error('Script failed:', error);
    process.exit(1);
  }
}

main();