/**
 * Clear Database Tables Script
 * 
 * This script provides functions to clear specific database tables or start fresh.
 * Use with caution as this will delete data permanently.
 */

import { db } from "@db";
import { 
  curtailmentRecords, 
  dailySummaries, 
  monthlySummaries, 
  yearlySummaries, 
  historicalBitcoinCalculations,
  bitcoinDailySummaries,
  bitcoinMonthlySummaries,
  bitcoinYearlySummaries,
  windGenerationData,
  ingestionProgress
} from "@db/schema";
import { eq, and, sql, gte, lte } from "drizzle-orm";
import { isValidDateString } from "../utils/dates";
import { format, parseISO, isValid } from 'date-fns';

/**
 * Interface for clear operation results
 */
interface ClearResult {
  operation: string;
  table: string;
  recordsDeleted: number;
  condition?: string;
}

/**
 * Clear curtailment records for a specific date range
 */
export async function clearCurtailmentRecords(
  startDate?: string, 
  endDate?: string
): Promise<ClearResult> {
  console.log(`Clearing curtailment records${startDate ? ` from ${startDate}` : ''}${endDate ? ` to ${endDate}` : ''}`);
  
  // For counting purposes, first count the records that match our criteria
  let countQuery = db.select({ count: sql<number>`count(*)` }).from(curtailmentRecords);
  
  // Apply date filters to count query if provided
  if (startDate && endDate) {
    countQuery = countQuery.where(
      and(
        gte(curtailmentRecords.settlementDate, startDate),
        lte(curtailmentRecords.settlementDate, endDate)
      )
    );
  } else if (startDate) {
    countQuery = countQuery.where(gte(curtailmentRecords.settlementDate, startDate));
  } else if (endDate) {
    countQuery = countQuery.where(lte(curtailmentRecords.settlementDate, endDate));
  }
  
  const countResult = await countQuery;
  const recordCount = Number(countResult[0]?.count || 0);
  
  // Now perform the delete operation
  if (startDate && endDate) {
    await db.delete(curtailmentRecords).where(
      and(
        gte(curtailmentRecords.settlementDate, startDate),
        lte(curtailmentRecords.settlementDate, endDate)
      )
    );
  } else if (startDate) {
    await db.delete(curtailmentRecords).where(gte(curtailmentRecords.settlementDate, startDate));
  } else if (endDate) {
    await db.delete(curtailmentRecords).where(lte(curtailmentRecords.settlementDate, endDate));
  } else {
    await db.delete(curtailmentRecords);
  }
  
  console.log(`Deleted ${recordCount} curtailment records`);
  
  return {
    operation: 'delete',
    table: 'curtailment_records',
    recordsDeleted: recordCount,
    condition: startDate || endDate ? `date range: ${startDate || 'any'} to ${endDate || 'any'}` : 'all records'
  };
}

/**
 * Clear Bitcoin calculations for a specific date range
 */
export async function clearBitcoinCalculations(
  startDate?: string, 
  endDate?: string,
  minerModel?: string
): Promise<ClearResult> {
  console.log(`Clearing Bitcoin calculations${startDate ? ` from ${startDate}` : ''}${endDate ? ` to ${endDate}` : ''}${minerModel ? ` for ${minerModel}` : ''}`);
  
  // Build conditions for the query
  let conditions = [];
  
  // Apply date filters if provided
  if (startDate && endDate) {
    conditions.push(
      and(
        gte(historicalBitcoinCalculations.settlementDate, startDate),
        lte(historicalBitcoinCalculations.settlementDate, endDate)
      )
    );
  } else if (startDate) {
    conditions.push(gte(historicalBitcoinCalculations.settlementDate, startDate));
  } else if (endDate) {
    conditions.push(lte(historicalBitcoinCalculations.settlementDate, endDate));
  }
  
  // Apply miner model filter if provided
  if (minerModel) {
    conditions.push(eq(historicalBitcoinCalculations.minerModel, minerModel));
  }
  
  // For counting purposes, first count the records that match our criteria
  let countQuery = db.select({ count: sql<number>`count(*)` }).from(historicalBitcoinCalculations);
  
  // Apply all conditions to count query if any exist
  if (conditions.length > 0) {
    if (conditions.length === 1) {
      countQuery = countQuery.where(conditions[0]);
    } else {
      countQuery = countQuery.where(and(...conditions));
    }
  }
  
  const countResult = await countQuery;
  const recordCount = Number(countResult[0]?.count || 0);
  
  // Now perform the delete operation with the same conditions
  if (conditions.length > 0) {
    if (conditions.length === 1) {
      await db.delete(historicalBitcoinCalculations).where(conditions[0]);
    } else {
      await db.delete(historicalBitcoinCalculations).where(and(...conditions));
    }
  } else {
    await db.delete(historicalBitcoinCalculations);
  }
  
  console.log(`Deleted ${recordCount} Bitcoin calculations`);
  
  return {
    operation: 'delete',
    table: 'historical_bitcoin_calculations',
    recordsDeleted: recordCount,
    condition: `${startDate || endDate ? `date range: ${startDate || 'any'} to ${endDate || 'any'}` : 'all records'}${minerModel ? `, miner: ${minerModel}` : ''}`
  };
}

/**
 * Clear all summary tables for a fresh start
 */
export async function clearAllSummaryTables(): Promise<ClearResult[]> {
  console.log('Clearing all summary tables...');
  
  const results: ClearResult[] = [];
  
  // Clear daily summaries
  const dailyResult = await db.delete(dailySummaries);
  results.push({
    operation: 'delete',
    table: 'daily_summaries',
    recordsDeleted: 0 // Cannot count without an ID column
  });
  
  // Clear monthly summaries
  const monthlyResult = await db.delete(monthlySummaries);
  results.push({
    operation: 'delete',
    table: 'monthly_summaries',
    recordsDeleted: 0 // Cannot count without an ID column
  });
  
  // Clear yearly summaries
  const yearlyResult = await db.delete(yearlySummaries);
  results.push({
    operation: 'delete',
    table: 'yearly_summaries',
    recordsDeleted: 0 // Cannot count without an ID column
  });
  
  // Clear Bitcoin daily summaries
  await db.delete(bitcoinDailySummaries);
  results.push({
    operation: 'delete',
    table: 'bitcoin_daily_summaries',
    recordsDeleted: 0 // Cannot count without an ID column
  });
  
  // Clear Bitcoin monthly summaries
  await db.delete(bitcoinMonthlySummaries);
  results.push({
    operation: 'delete',
    table: 'bitcoin_monthly_summaries',
    recordsDeleted: 0 // Cannot count without an ID column
  });
  
  // Clear Bitcoin yearly summaries
  await db.delete(bitcoinYearlySummaries);
  results.push({
    operation: 'delete',
    table: 'bitcoin_yearly_summaries',
    recordsDeleted: 0 // Cannot count without an ID column
  });
  
  // Clear ingestion progress
  await db.delete(ingestionProgress);
  results.push({
    operation: 'delete',
    table: 'ingestion_progress',
    recordsDeleted: 0 // Cannot count without an ID column
  });
  
  console.log('All summary tables cleared successfully');
  
  return results;
}

/**
 * Clear wind generation data for a specific date range
 */
export async function clearWindGenerationData(
  startDate?: string, 
  endDate?: string
): Promise<ClearResult> {
  console.log(`Clearing wind generation data${startDate ? ` from ${startDate}` : ''}${endDate ? ` to ${endDate}` : ''}`);
  
  // For counting purposes, first count the records that match our criteria
  let countQuery = db.select({ count: sql<number>`count(*)` }).from(windGenerationData);
  
  // Apply date filters to count query if provided
  if (startDate && endDate) {
    countQuery = countQuery.where(
      and(
        gte(windGenerationData.settlementDate, startDate),
        lte(windGenerationData.settlementDate, endDate)
      )
    );
  } else if (startDate) {
    countQuery = countQuery.where(gte(windGenerationData.settlementDate, startDate));
  } else if (endDate) {
    countQuery = countQuery.where(lte(windGenerationData.settlementDate, endDate));
  }
  
  const countResult = await countQuery;
  const recordCount = Number(countResult[0]?.count || 0);
  
  // Now perform the delete operation
  if (startDate && endDate) {
    await db.delete(windGenerationData).where(
      and(
        gte(windGenerationData.settlementDate, startDate),
        lte(windGenerationData.settlementDate, endDate)
      )
    );
  } else if (startDate) {
    await db.delete(windGenerationData).where(gte(windGenerationData.settlementDate, startDate));
  } else if (endDate) {
    await db.delete(windGenerationData).where(lte(windGenerationData.settlementDate, endDate));
  } else {
    await db.delete(windGenerationData);
  }
  
  console.log(`Deleted ${recordCount} wind generation records`);
  
  return {
    operation: 'delete',
    table: 'wind_generation_data',
    recordsDeleted: recordCount,
    condition: startDate || endDate ? `date range: ${startDate || 'any'} to ${endDate || 'any'}` : 'all records'
  };
}

/**
 * Clear all data for a completely fresh start
 */
export async function clearAllData(): Promise<ClearResult[]> {
  console.log('\n===== CLEARING ALL DATA - THIS WILL DELETE EVERYTHING =====');
  console.log('This operation will remove all data from all tables for a fresh start');
  
  const results: ClearResult[] = [];
  
  // First clear all summary tables
  const summaryResults = await clearAllSummaryTables();
  results.push(...summaryResults);
  
  // Clear historical Bitcoin calculations
  const bitcoinResult = await clearBitcoinCalculations();
  results.push(bitcoinResult);
  
  // Clear curtailment records
  const curtailmentResult = await clearCurtailmentRecords();
  results.push(curtailmentResult);
  
  // Clear wind generation data
  const windResult = await clearWindGenerationData();
  results.push(windResult);
  
  console.log('\n===== ALL DATA CLEARED SUCCESSFULLY =====');
  
  return results;
}

// Only run the script directly if it's the main module
if (require.main === module) {
  (async () => {
    try {
      console.log('Starting database clear operation...');
      
      // Extract command line arguments
      const args = process.argv.slice(2);
      const operation = args[0]; // clear-all, clear-curtailment, clear-bitcoin, clear-summaries, clear-wind
      const startDate = args[1]; // YYYY-MM-DD (optional)
      const endDate = args[2];   // YYYY-MM-DD (optional)
      const minerModel = args[3]; // Miner model (optional, for clear-bitcoin only)
      
      // Validate dates if provided
      if (startDate && !isValidDateString(startDate)) {
        console.error(`Invalid start date format: ${startDate}. Use YYYY-MM-DD format.`);
        process.exit(1);
      }
      
      if (endDate && !isValidDateString(endDate)) {
        console.error(`Invalid end date format: ${endDate}. Use YYYY-MM-DD format.`);
        process.exit(1);
      }
      
      // Execute the requested operation
      switch (operation) {
        case 'clear-all':
          await clearAllData();
          break;
          
        case 'clear-curtailment':
          await clearCurtailmentRecords(startDate, endDate);
          break;
          
        case 'clear-bitcoin':
          await clearBitcoinCalculations(startDate, endDate, minerModel);
          break;
          
        case 'clear-summaries':
          await clearAllSummaryTables();
          break;
          
        case 'clear-wind':
          await clearWindGenerationData(startDate, endDate);
          break;
          
        default:
          console.error(`
Unknown operation: ${operation}
Usage: 
  npm run tsx server/scripts/clear_database_tables.ts <operation> [startDate] [endDate] [minerModel]

Operations:
  clear-all           - Clear all data from all tables
  clear-curtailment   - Clear curtailment records (optional date range)
  clear-bitcoin       - Clear Bitcoin calculations (optional date range and miner model)
  clear-summaries     - Clear all summary tables
  clear-wind          - Clear wind generation data (optional date range)

Examples:
  npm run tsx server/scripts/clear_database_tables.ts clear-curtailment 2025-01-01 2025-03-31
  npm run tsx server/scripts/clear_database_tables.ts clear-bitcoin 2025-01-01 2025-03-31 S19J_PRO
  npm run tsx server/scripts/clear_database_tables.ts clear-summaries
`);
          process.exit(1);
      }
      
      console.log('Database clear operation completed successfully');
      process.exit(0);
    } catch (error) {
      console.error('Error during database clear operation:', error);
      process.exit(1);
    }
  })();
}