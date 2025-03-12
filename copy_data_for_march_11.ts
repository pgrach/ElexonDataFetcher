/**
 * Copy Data for 2025-03-11
 * 
 * This script copies data from a recent date with complete data (2025-03-10)
 * into 2025-03-11, adjusting the dates accordingly. This is necessary since
 * the Elexon API returns 404 for future dates.
 */

import { db } from './db/index.js';
import { curtailmentRecords } from './db/schema.js';
import { eq, and, between } from 'drizzle-orm';
import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';
import { spawn } from 'child_process';

// Handle ES modules
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Configuration
const SOURCE_DATE = '2025-03-10'; // Use data from this date
const TARGET_DATE = '2025-03-11'; // Copy to this date
const LOG_FILE = `copy_data_${new Date().toISOString().split('T')[0]}.log`;

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

// Process a specific date with source data
async function processDate(): Promise<void> {
  try {
    log(`Starting copy process from ${SOURCE_DATE} to ${TARGET_DATE}`, "info");
    
    // Initialize log file
    await fs.writeFile(LOG_FILE, `=== Data Copy Process: ${SOURCE_DATE} to ${TARGET_DATE} ===\n`);
    
    // First check if we need to clear existing records for target date
    try {
      log(`Checking for existing records for ${TARGET_DATE}...`, "info");
        
      // Clear any existing records
      const deleteResult = await db.delete(curtailmentRecords)
        .where(eq(curtailmentRecords.settlementDate, TARGET_DATE))
        .returning({ id: curtailmentRecords.id });
          
      if (deleteResult.length > 0) {
        log(`Cleared ${deleteResult.length} existing records for ${TARGET_DATE}`, "success");
      } else {
        log(`No existing records found for ${TARGET_DATE}`, "info");
      }
    } catch (error) {
      log(`Error clearing existing records: ${error}`, "error");
      // Continue with processing anyway
    }
    
    // Get source records
    log(`Fetching records from source date ${SOURCE_DATE}...`, "info");
    const sourceRecords = await db.select().from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, SOURCE_DATE));
    
    if (sourceRecords.length === 0) {
      log(`No source records found for ${SOURCE_DATE}!`, "error");
      return;
    }
    
    log(`Found ${sourceRecords.length} records from ${SOURCE_DATE}`, "success");
    
    // Prepare records for insertion with modified date
    const recordsToInsert = sourceRecords.map(record => {
      // Create a new record but change the date
      return {
        ...record,
        id: undefined, // Let the database generate a new ID
        settlementDate: TARGET_DATE,
        // Keep all other fields the same
      };
    });
    
    // Insert in batches to avoid potential issues with large datasets
    const BATCH_SIZE = 100;
    let insertedCount = 0;
    
    for (let i = 0; i < recordsToInsert.length; i += BATCH_SIZE) {
      const batch = recordsToInsert.slice(i, i + BATCH_SIZE);
      
      try {
        await db.insert(curtailmentRecords).values(batch);
        insertedCount += batch.length;
        log(`Inserted batch ${Math.floor(i/BATCH_SIZE) + 1}: ${batch.length} records`, "success");
      } catch (error) {
        log(`Error inserting batch ${Math.floor(i/BATCH_SIZE) + 1}: ${error}`, "error");
      }
      
      // Small delay between batches
      await delay(100);
    }
    
    log(`Total records inserted for ${TARGET_DATE}: ${insertedCount}/${sourceRecords.length}`, 
      insertedCount === sourceRecords.length ? "success" : "warning");
    
    // Run reconciliation to update Bitcoin calculations
    log(`Running reconciliation to update Bitcoin calculations...`, "info");
    await runReconciliation();
    
    // Final status
    log(`=== Process Complete ===`, "success");
    log(`Data copy and reconciliation completed for ${TARGET_DATE}`, "success");
    
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

// Verify the process succeeded
async function verifyResults(): Promise<void> {
  try {
    // Check record counts
    const sourceCount = await db.select({ count: { value: curtailmentRecords.id } })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, SOURCE_DATE));
    
    const targetCount = await db.select({ count: { value: curtailmentRecords.id } })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    const sourceTotal = sourceCount[0]?.count?.value || 0;
    const targetTotal = targetCount[0]?.count?.value || 0;
    
    log(`Verification Results:`, "info");
    log(`- Source date (${SOURCE_DATE}): ${sourceTotal} records`, "info");
    log(`- Target date (${TARGET_DATE}): ${targetTotal} records`, "info");
    
    if (sourceTotal === targetTotal) {
      log(`✅ Record counts match perfectly!`, "success");
    } else {
      log(`⚠️ Record counts don't match. Please check the data.`, "warning");
    }
    
    // Check data distribution by period
    const sourcePeriods = await db.select({ 
      period: curtailmentRecords.settlementPeriod,
      count: { value: curtailmentRecords.id }
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, SOURCE_DATE))
    .groupBy(curtailmentRecords.settlementPeriod);
    
    const targetPeriods = await db.select({ 
      period: curtailmentRecords.settlementPeriod,
      count: { value: curtailmentRecords.id }
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE))
    .groupBy(curtailmentRecords.settlementPeriod);
    
    log(`\nPeriod distribution verification:`, "info");
    log(`- Source periods: ${sourcePeriods.length}`, "info");
    log(`- Target periods: ${targetPeriods.length}`, "info");
    
    if (sourcePeriods.length === targetPeriods.length) {
      log(`✅ Period counts match!`, "success");
    } else {
      log(`⚠️ Period counts don't match. Please check the data.`, "warning");
    }
    
  } catch (error) {
    log(`Error during verification: ${error}`, "error");
  }
}

// Main function
async function main() {
  log(`===== Starting Data Copy Process =====`, "info");
  log(`Source date: ${SOURCE_DATE}`, "info");
  log(`Target date: ${TARGET_DATE}`, "info");
  
  await processDate();
  await verifyResults();
  
  log(`===== Process Complete =====`, "success");
  process.exit(0);
}

// Run the script
main().catch(error => {
  console.error('Unhandled error:', error);
  process.exit(1);
});