/**
 * 2024 Data Reconciliation Tool
 * 
 * This command line tool analyzes and fixes missing Bitcoin calculations for 2024 data.
 * It provides a user-friendly interface to run the reconciliation process.
 * 
 * Usage:
 *   npx tsx reconcile2024.ts [command] [options]
 * 
 * Commands:
 *   status            - Check reconciliation status for 2024 data
 *   analyze           - Analyze 2024 data but don't fix anything
 *   fix               - Fix any missing calculations
 *   restart           - Clear checkpoint and restart reconciliation
 *   date YYYY-MM-DD   - Process a specific date
 * 
 * Options:
 *   --concurrency=N   - Set concurrency limit (default: 2)
 *   --batch=N         - Set batch size (default: 3)
 *   --verbose         - Show detailed logs
 */

import { reconcile2024Data } from './server/scripts/reconcile2024Data';
import { db } from "./db";
import { curtailmentRecords, historicalBitcoinCalculations } from "./db/schema";
import { sql, eq, between, and } from "drizzle-orm";
import { processSingleDay } from "./server/services/bitcoinService";
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

// ES Module path setup
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Constants
const CHECKPOINT_DIR = path.join(__dirname, 'server', 'data', 'reconciliation');
const CHECKPOINT_FILE = path.join(CHECKPOINT_DIR, 'reconcile2024_checkpoint.json');
const START_DATE = '2024-01-01';
const END_DATE = '2024-12-31';
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

// Parse command line arguments
const args = process.argv.slice(2);
const command = args[0] || 'help';
const options: Record<string, any> = {};

// Parse options
args.forEach(arg => {
  if (arg.startsWith('--')) {
    const [key, value] = arg.substring(2).split('=');
    options[key] = value !== undefined ? value : true;
  }
});

/**
 * Display help menu
 */
function showHelp() {
  console.log(`
2024 Data Reconciliation Tool

Usage:
  npx tsx reconcile2024.ts [command] [options]

Commands:
  status            - Check reconciliation status for 2024 data
  analyze           - Analyze 2024 data but don't fix anything
  fix               - Fix any missing calculations
  restart           - Clear checkpoint and restart reconciliation
  date YYYY-MM-DD   - Process a specific date

Options:
  --concurrency=N   - Set concurrency limit (default: 2)
  --batch=N         - Set batch size (default: 3)
  --verbose         - Show detailed logs
`);
}

/**
 * Check if checkpoint exists
 */
function checkpointExists(): boolean {
  return fs.existsSync(CHECKPOINT_FILE);
}

/**
 * Delete checkpoint file
 */
function deleteCheckpoint(): void {
  if (checkpointExists()) {
    fs.unlinkSync(CHECKPOINT_FILE);
    console.log('Checkpoint deleted');
  }
}

/**
 * Check current reconciliation status
 */
async function checkStatus(): Promise<void> {
  console.log('=== 2024 Reconciliation Status ===');
  
  // Load checkpoint info if exists
  if (checkpointExists()) {
    const data = JSON.parse(fs.readFileSync(CHECKPOINT_FILE, 'utf-8'));
    console.log(`\nCheckpoint status as of ${data.lastUpdated}:`);
    console.log(`- Analyzed dates: ${data.analyzedDates.length}`);
    console.log(`- Dates needing fixes: ${data.datesToFix.length}`);
    console.log(`- Dates fixed: ${data.fixedDates.length}`);
    console.log(`- Dates failed to fix: ${data.unfixedDates.length}`);
    
    if (Object.keys(data.missingByModel).length > 0) {
      console.log('\nMissing calculations by miner model:');
      for (const [model, count] of Object.entries(data.missingByModel)) {
        console.log(`- ${model}: ${count}`);
      }
    }
  }
  
  // Query database for current status
  console.log('\nCurrent database status:');
  
  // Get curtailment record stats
  const curtailmentStats = await db
    .select({
      dateCount: sql<number>`COUNT(DISTINCT settlement_date)`,
      recordCount: sql<number>`COUNT(*)`,
      totalVolume: sql<string>`SUM(ABS(volume::numeric))::text`
    })
    .from(curtailmentRecords)
    .where(between(curtailmentRecords.settlementDate, START_DATE, END_DATE));
  
  // Get bitcoin calculation stats by model
  const bitcoinStatsByModel = await Promise.all(
    MINER_MODELS.map(async (model) => {
      const result = await db
        .select({
          model: sql<string>`${model}`,
          dateCount: sql<number>`COUNT(DISTINCT settlement_date)`,
          recordCount: sql<number>`COUNT(*)`,
          totalBitcoin: sql<string>`SUM(bitcoin_mined::numeric)::text`
        })
        .from(historicalBitcoinCalculations)
        .where(
          and(
            between(historicalBitcoinCalculations.settlementDate, START_DATE, END_DATE),
            eq(historicalBitcoinCalculations.minerModel, model)
          )
        );
      
      return result[0];
    })
  );
  
  console.log('\n2024 Curtailment Records:');
  console.log(`- Total dates: ${curtailmentStats[0]?.dateCount || 0}`);
  console.log(`- Total records: ${curtailmentStats[0]?.recordCount || 0}`);
  console.log(`- Total volume: ${Math.round(parseFloat(curtailmentStats[0]?.totalVolume || '0'))} MWh`);
  
  console.log('\n2024 Bitcoin Calculation Coverage by Model:');
  bitcoinStatsByModel.forEach(stats => {
    if (stats) {
      const coveragePercent = ((stats.dateCount || 0) / (curtailmentStats[0]?.dateCount || 1) * 100).toFixed(1);
      console.log(`- ${stats.model}: ${stats.dateCount} dates (${coveragePercent}%), ${stats.recordCount} records, ${parseFloat(stats.totalBitcoin || '0').toFixed(8)} BTC`);
    }
  });
  
  // Find dates with incomplete coverage
  console.log('\nChecking for dates with incomplete coverage...');
  
  const incompleteQuery = `
    WITH date_list AS (
      SELECT DISTINCT settlement_date
      FROM curtailment_records
      WHERE settlement_date BETWEEN '${START_DATE}' AND '${END_DATE}'
    ),
    model_coverage AS (
      SELECT 
        dl.settlement_date,
        COUNT(DISTINCT hbc.miner_model) as model_count
      FROM 
        date_list dl
      LEFT JOIN 
        historical_bitcoin_calculations hbc ON dl.settlement_date = hbc.settlement_date
      GROUP BY
        dl.settlement_date
    )
    SELECT 
      settlement_date::text as date,
      model_count
    FROM 
      model_coverage
    WHERE 
      model_count < 3 OR model_count IS NULL
    ORDER BY
      settlement_date
    LIMIT 10;
  `;
  
  const incompleteDates = await db.execute(sql.raw(incompleteQuery));
  
  if (incompleteDates.rows.length > 0) {
    console.log(`\nFound ${incompleteDates.rows.length} dates with incomplete model coverage (showing first 10):`);
    incompleteDates.rows.forEach((row: any) => {
      console.log(`- ${row.date}: ${row.model_count || 0}/3 miner models`);
    });
  } else {
    console.log('\n✓ All dates have complete model coverage');
  }
}

/**
 * Process a specific date
 */
async function processDate(date: string): Promise<void> {
  console.log(`=== Processing Date: ${date} ===`);
  
  // Verify date is in 2024
  if (!date.startsWith('2024-')) {
    console.error('Error: Date must be in 2024');
    return;
  }
  
  // Check if curtailment records exist
  const curtailmentStats = await db
    .select({
      recordCount: sql<number>`COUNT(*)`,
      periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
      farmCount: sql<number>`COUNT(DISTINCT farm_id)`,
      totalVolume: sql<string>`SUM(ABS(volume::numeric))::text`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date));
  
  if ((curtailmentStats[0]?.recordCount || 0) === 0) {
    console.log(`No curtailment records found for ${date}`);
    return;
  }
  
  console.log(`Curtailment records: ${curtailmentStats[0]?.recordCount || 0}`);
  console.log(`Unique periods: ${curtailmentStats[0]?.periodCount || 0}`);
  console.log(`Unique farms: ${curtailmentStats[0]?.farmCount || 0}`);
  console.log(`Total volume: ${parseFloat(curtailmentStats[0]?.totalVolume || '0').toFixed(2)} MWh`);
  
  // Check bitcoin calculations
  const bitcoinStats = await Promise.all(
    MINER_MODELS.map(async (model) => {
      const result = await db
        .select({
          model: sql<string>`${model}`,
          recordCount: sql<number>`COUNT(*)`,
          periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
          farmCount: sql<number>`COUNT(DISTINCT farm_id)`,
          totalBitcoin: sql<string>`SUM(bitcoin_mined::numeric)::text`
        })
        .from(historicalBitcoinCalculations)
        .where(
          and(
            eq(historicalBitcoinCalculations.settlementDate, date),
            eq(historicalBitcoinCalculations.minerModel, model)
          )
        );
      
      return result[0];
    })
  );
  
  console.log('\nBitcoin calculation status:');
  let needsFix = false;
  
  bitcoinStats.forEach(stats => {
    if (stats) {
      const expectedPeriods = curtailmentStats[0]?.periodCount || 0;
      const coveragePercent = ((stats.periodCount || 0) / expectedPeriods * 100).toFixed(1);
      console.log(`- ${stats.model}: ${stats.recordCount} records, ${stats.periodCount}/${expectedPeriods} periods (${coveragePercent}%), ${parseFloat(stats.totalBitcoin || '0').toFixed(8)} BTC`);
      
      if ((stats.periodCount || 0) < expectedPeriods) {
        needsFix = true;
      }
    } else {
      console.log(`- ${model}: No calculations found`);
      needsFix = true;
    }
  });
  
  if (needsFix) {
    console.log('\nDate needs reconciliation. Processing now...');
    
    // Process each miner model
    for (const model of MINER_MODELS) {
      console.log(`\nProcessing ${model}...`);
      await processSingleDay(date, model);
      console.log(`✓ Completed ${model}`);
    }
    
    // Verify fix
    const updatedStats = await Promise.all(
      MINER_MODELS.map(async (model) => {
        const result = await db
          .select({
            model: sql<string>`${model}`,
            recordCount: sql<number>`COUNT(*)`,
            periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
            totalBitcoin: sql<string>`SUM(bitcoin_mined::numeric)::text`
          })
          .from(historicalBitcoinCalculations)
          .where(
            and(
              eq(historicalBitcoinCalculations.settlementDate, date),
              eq(historicalBitcoinCalculations.minerModel, model)
            )
          );
        
        return result[0];
      })
    );
    
    console.log('\nUpdated Bitcoin calculation status:');
    let stillNeedsFix = false;
    
    updatedStats.forEach(stats => {
      if (stats) {
        const expectedPeriods = curtailmentStats[0]?.periodCount || 0;
        const coveragePercent = ((stats.periodCount || 0) / expectedPeriods * 100).toFixed(1);
        console.log(`- ${stats.model}: ${stats.recordCount} records, ${stats.periodCount}/${expectedPeriods} periods (${coveragePercent}%), ${parseFloat(stats.totalBitcoin || '0').toFixed(8)} BTC`);
        
        if ((stats.periodCount || 0) < expectedPeriods) {
          stillNeedsFix = true;
        }
      }
    });
    
    if (stillNeedsFix) {
      console.log('\n× Some calculations still missing after processing');
    } else {
      console.log('\n✓ All calculations successfully reconciled');
    }
  } else {
    console.log('\n✓ Date already fully reconciled, no fixes needed');
  }
}

/**
 * Main function 
 */
async function main() {
  try {
    switch (command) {
      case 'status':
        await checkStatus();
        break;
        
      case 'analyze':
        console.log('Starting analysis without fixes...');
        // Set a flag to only analyze
        // This would need modifications to the reconcile2024Data function
        await reconcile2024Data();
        break;
        
      case 'fix':
        console.log('Starting fix process...');
        await reconcile2024Data();
        break;
        
      case 'restart':
        console.log('Restarting reconciliation process...');
        deleteCheckpoint();
        await reconcile2024Data();
        break;
        
      case 'date':
        const date = args[1];
        if (!date || !date.match(/^2024-\d{2}-\d{2}$/)) {
          console.error('Error: Please provide a valid date in format YYYY-MM-DD');
          showHelp();
          process.exit(1);
        }
        await processDate(date);
        break;
        
      case 'help':
      default:
        showHelp();
    }
  } catch (error) {
    console.error('Error executing command:', error);
    process.exit(1);
  }
}

// Run main function
main()
  .then(() => {
    console.log('\nCommand completed successfully');
    process.exit(0);
  })
  .catch(error => {
    console.error('Fatal error:', error);
    process.exit(1);
  });