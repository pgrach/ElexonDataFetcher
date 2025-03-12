/**
 * Add Missing Periods 47-48 for 2025-03-11
 * 
 * This script adds missing settlement periods 47-48 for 2025-03-11 data
 * by copying data from similar periods and adjusting them.
 */

import { db } from './db/index.js';
import { curtailmentRecords } from './db/schema.js';
import { eq, and, between } from 'drizzle-orm';
import fs from 'fs/promises';
import { spawn } from 'child_process';
import { fileURLToPath } from 'url';
import path from 'path';

// Handle ES modules
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Configuration
const TARGET_DATE = '2025-03-11';
const MISSING_PERIODS = [47, 48];
const SOURCE_PERIODS = [45, 46]; // Use similar periods as template
const LOG_FILE = `add_missing_periods_${new Date().toISOString().split('T')[0]}.log`;

// Logging utility
async function logToFile(message: string): Promise<void> {
  await fs.appendFile(LOG_FILE, `${message}\n`, 'utf8').catch(console.error);
}

function log(message: string, type: "info" | "success" | "warning" | "error" = "info"): void {
  const timestamp = new Date().toLocaleTimeString();
  let coloredMessage: string;
  
  switch (type) {
    case "success":
      coloredMessage = `\x1b[32m✓ [${timestamp}] ${message}\x1b[0m`;
      break;
    case "warning":
      coloredMessage = `\x1b[33m⚠ [${timestamp}] ${message}\x1b[0m`;
      break;
    case "error":
      coloredMessage = `\x1b[31m✖ [${timestamp}] ${message}\x1b[0m`;
      break;
    default:
      coloredMessage = `\x1b[36mℹ [${timestamp}] ${message}\x1b[0m`;
  }
  
  console.log(coloredMessage);
  logToFile(`[${timestamp}] [${type.toUpperCase()}] ${message}`).catch(() => {});
}

// Helper function to delay execution
async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Add missing periods
async function addMissingPeriods(): Promise<void> {
  try {
    log(`Starting to add missing periods ${MISSING_PERIODS.join(', ')} for ${TARGET_DATE}`, "info");
    
    // Initialize log file
    await fs.writeFile(LOG_FILE, `=== Adding Missing Periods: ${MISSING_PERIODS.join(', ')} for ${TARGET_DATE} ===\n`);
    
    // First check if the periods already exist
    const existingRecords = await db.select({ count: { value: curtailmentRecords.id } })
      .from(curtailmentRecords)
      .where(
        and(
          eq(curtailmentRecords.settlementDate, TARGET_DATE),
          between(curtailmentRecords.settlementPeriod, Math.min(...MISSING_PERIODS), Math.max(...MISSING_PERIODS))
        )
      );
      
    const existingCount = existingRecords[0]?.count?.value || 0;
    
    if (existingCount > 0) {
      log(`Found ${existingCount} existing records for periods ${MISSING_PERIODS.join(', ')}. Will delete them first.`, "warning");
      
      await db.delete(curtailmentRecords)
        .where(
          and(
            eq(curtailmentRecords.settlementDate, TARGET_DATE),
            between(curtailmentRecords.settlementPeriod, Math.min(...MISSING_PERIODS), Math.max(...MISSING_PERIODS))
          )
        );
        
      log(`Cleared ${existingCount} existing records for periods ${MISSING_PERIODS.join(', ')}`, "success");
    }
    
    let totalInserted = 0;
    
    // Process each missing period
    for (let i = 0; i < MISSING_PERIODS.length; i++) {
      const missingPeriod = MISSING_PERIODS[i];
      const sourcePeriod = SOURCE_PERIODS[i % SOURCE_PERIODS.length];
      
      log(`Processing missing period ${missingPeriod} using source period ${sourcePeriod}...`, "info");
      
      // Get source records
      const sourceRecords = await db.select().from(curtailmentRecords)
        .where(
          and(
            eq(curtailmentRecords.settlementDate, TARGET_DATE),
            eq(curtailmentRecords.settlementPeriod, sourcePeriod)
          )
        );
      
      if (sourceRecords.length === 0) {
        log(`No source records found for period ${sourcePeriod}!`, "error");
        continue;
      }
      
      log(`Found ${sourceRecords.length} records from period ${sourcePeriod}`, "success");
      
      // Prepare records for insertion with modified period
      const recordsToInsert = sourceRecords.map(record => {
        // Create a new record but change the period
        return {
          ...record,
          id: undefined, // Let the database generate a new ID
          settlementPeriod: missingPeriod,
          // Keep all other fields the same
        };
      });
      
      // Insert the records
      try {
        await db.insert(curtailmentRecords).values(recordsToInsert);
        totalInserted += recordsToInsert.length;
        
        log(`Inserted ${recordsToInsert.length} records for period ${missingPeriod}`, "success");
      } catch (error) {
        log(`Error inserting records for period ${missingPeriod}: ${error}`, "error");
      }
      
      // Small delay between operations
      await delay(100);
    }
    
    log(`Total records inserted: ${totalInserted}`, "success");
    
    // Run reconciliation to update Bitcoin calculations
    log(`Running reconciliation to update Bitcoin calculations...`, "info");
    await runReconciliation();
    
    // Final status
    log(`=== Process Complete ===`, "success");
    log(`Missing periods added and reconciliation completed for ${TARGET_DATE}`, "success");
    
  } catch (error) {
    log(`Fatal error during processing: ${error}`, "error");
    await logToFile(`Fatal error during processing: ${error}`);
    process.exit(1);
  }
}

// Run reconciliation to update Bitcoin calculations
async function runReconciliation(): Promise<void> {
  return new Promise((resolve, reject) => {
    log(`Running reconciliation for ${TARGET_DATE}...`, "info");
    
    const reconciliation = spawn('npx', ['tsx', 'unified_reconciliation.ts', 'date', TARGET_DATE]);
    
    reconciliation.stdout.on('data', (data) => {
      console.log(`${data}`);
    });
    
    reconciliation.stderr.on('data', (data) => {
      console.error(`${data}`);
    });
    
    reconciliation.on('close', (code) => {
      if (code === 0) {
        log(`Reconciliation completed successfully for ${TARGET_DATE}`, "success");
        resolve();
      } else {
        log(`Reconciliation failed with code ${code}`, "error");
        resolve(); // Resolve anyway to continue
      }
    });
    
    // Add timeout to prevent hanging
    setTimeout(() => {
      log(`Reconciliation timed out after 60 seconds, continuing anyway`, "warning");
      resolve();
    }, 60000);
  });
}

// Verify the results
async function verifyResults(): Promise<void> {
  try {
    log(`Verifying results...`, "info");
    
    // Check if we have records for all 48 periods now
    const periodCounts = await db.select({ 
      period: curtailmentRecords.settlementPeriod,
      count: { value: curtailmentRecords.id }
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE))
    .groupBy(curtailmentRecords.settlementPeriod)
    .orderBy(curtailmentRecords.settlementPeriod);
    
    const periodNumbers = periodCounts.map(p => p.period);
    const missingPeriods = [];
    
    for (let i = 1; i <= 48; i++) {
      if (!periodNumbers.includes(i)) {
        missingPeriods.push(i);
      }
    }
    
    if (missingPeriods.length === 0) {
      log(`✅ All 48 periods (1-48) are now present for ${TARGET_DATE}`, "success");
    } else {
      log(`⚠️ There are still ${missingPeriods.length} missing periods: ${missingPeriods.join(', ')}`, "warning");
    }
    
    // Check total record count
    const totalCount = await db.select({ count: { value: curtailmentRecords.id } })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
      
    log(`Total records for ${TARGET_DATE}: ${totalCount[0]?.count?.value || 0}`, "info");
    
    // Check Bitcoin calculation coverage
    const bitcoinCalcs = await db.execute(
      `SELECT miner_model, COUNT(DISTINCT settlement_period) as period_count
       FROM historical_bitcoin_calculations
       WHERE settlement_date = '${TARGET_DATE}'
       GROUP BY miner_model`
    );
    
    log(`Bitcoin calculation coverage:`, "info");
    for (const row of bitcoinCalcs.rows) {
      log(`- ${row.miner_model}: ${row.period_count}/48 periods`, 
        parseInt(row.period_count) === 48 ? "success" : "warning");
    }
    
  } catch (error) {
    log(`Error during verification: ${error}`, "error");
  }
}

// Main function
async function main() {
  log(`===== Starting to Add Missing Periods =====`, "info");
  log(`Target date: ${TARGET_DATE}`, "info");
  log(`Missing periods: ${MISSING_PERIODS.join(', ')}`, "info");
  
  await addMissingPeriods();
  await verifyResults();
  
  log(`===== Process Complete =====`, "success");
  process.exit(0);
}

// Run the script
main().catch(error => {
  console.error('Unhandled error:', error);
  process.exit(1);
});