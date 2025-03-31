/**
 * Missing Day Processor for 2025-03-29
 * 
 * This script is designed to fill the gap in the data for March 29, 2025, which is missing
 * in the curtailment_records table. It uses the existing data processing pipeline but focuses
 * on just this specific date.
 * 
 * Usage:
 *   npx tsx process_missing_day_2025-03-29.ts
 */

import { processDailyCurtailment } from "./server/services/curtailment";
import { db } from "@db";
import { curtailmentRecords, dailySummaries } from "@db/schema";
import { eq, sql } from "drizzle-orm";
import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from 'url';
import { format } from "date-fns";

const MISSING_DATE = "2025-03-29";
const LOG_FILE = `process_missing_day_${MISSING_DATE}.log`;

// Create a logging function that writes to both console and file
async function logToFile(message: string): Promise<void> {
  const timestamp = new Date().toLocaleTimeString();
  const logMessage = `[${timestamp}] ${message}\n`;
  
  console.log(message);
  
  try {
    await fs.appendFile(LOG_FILE, logMessage);
  } catch (error) {
    console.error("Error writing to log file:", error);
  }
}

// Simple logging wrapper with color coding
function log(message: string, type: "info" | "success" | "warning" | "error" = "info"): void {
  const prefix = {
    info: "[INFO]",
    success: "[SUCCESS]",
    warning: "[WARNING]",
    error: "[ERROR]"
  }[type];
  
  logToFile(`${prefix} ${message}`);
}

// Delay function for rate limiting
async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function processMissingDate(): Promise<void> {
  try {
    // Initialize log file
    await fs.writeFile(LOG_FILE, `=== Processing Missing Data: ${MISSING_DATE} ===\n`);
    
    // Check if we already have data for this date
    const existingRecords = await db
      .select({ count: sql<number>`count(*)` })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, MISSING_DATE));
    
    const recordCount = existingRecords[0]?.count || 0;
    
    if (recordCount > 0) {
      log(`Found ${recordCount} existing records for ${MISSING_DATE}. Will clear and reprocess.`, "warning");
    } else {
      log(`No existing records found for ${MISSING_DATE}.`, "info");
    }
    
    // Process the date using the existing pipeline
    log(`Starting processing for ${MISSING_DATE}...`, "info");
    
    try {
      await processDailyCurtailment(MISSING_DATE);
      
      // Verify data was added
      const verificationQuery = await db
        .select({ count: sql<number>`count(*)` })
        .from(curtailmentRecords)
        .where(eq(curtailmentRecords.settlementDate, MISSING_DATE));
      
      const newRecordCount = verificationQuery[0]?.count || 0;
      
      if (newRecordCount > 0) {
        log(`Successfully processed ${MISSING_DATE} with ${newRecordCount} records.`, "success");
        
        // Check daily summary
        const dailySummary = await db.query.dailySummaries.findFirst({
          where: eq(dailySummaries.summaryDate, MISSING_DATE)
        });
        
        if (dailySummary) {
          log(`Daily summary: ${Number(dailySummary.totalCurtailedEnergy).toFixed(2)} MWh, £${Number(dailySummary.totalPayment).toFixed(2)}`, "success");
        } else {
          log(`Warning: No daily summary found for ${MISSING_DATE}.`, "warning");
        }
      } else {
        log(`Warning: No records were added for ${MISSING_DATE}. This might be normal if there was no curtailment on this date.`, "warning");
      }
    } catch (error) {
      log(`Error processing ${MISSING_DATE}: ${error}`, "error");
      throw error;
    }
    
    log(`Processing complete for ${MISSING_DATE}.`, "success");
    
  } catch (error) {
    log(`Fatal error: ${error}`, "error");
    process.exit(1);
  }
}

// Run the script
processMissingDate();