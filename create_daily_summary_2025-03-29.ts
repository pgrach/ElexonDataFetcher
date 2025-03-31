/**
 * Create Daily Summary for 2025-03-29
 * 
 * This script creates the missing daily summary for March 29, 2025 
 * based on the existing curtailment records.
 * 
 * Usage:
 *   npx tsx create_daily_summary_2025-03-29.ts
 */

import { db } from './db';
import fs from 'fs';
import path from 'path';
import { format } from 'date-fns';
import { sql, eq, and, desc } from 'drizzle-orm';
import { curtailmentRecords, dailySummaries } from './db/schema';

async function log(message: string, level: "info" | "error" | "warning" | "success" = "info"): Promise<void> {
  const timestamp = new Date().toISOString();
  const prefix = level === "info" 
    ? "\x1b[37m[INFO]" 
    : level === "error" 
      ? "\x1b[31m[ERROR]" 
      : level === "warning" 
        ? "\x1b[33m[WARNING]" 
        : "\x1b[32m[SUCCESS]";
  
  console.log(`[${timestamp}] ${prefix} ${message}\x1b[0m`);
  
  // Also log to file
  const logDir = path.join(process.cwd(), 'logs');
  const logFile = path.join(logDir, `create_daily_summary_2025-03-29.log`);
  
  if (!fs.existsSync(logDir)) {
    fs.mkdirSync(logDir, { recursive: true });
  }
  
  fs.appendFileSync(
    logFile, 
    `[${timestamp}] [${level.toUpperCase()}] ${message}\n`
  );
}

async function createDailySummary(): Promise<void> {
  try {
    log("=== Starting Daily Summary Creation for 2025-03-29 ===");
    
    // 1. Get the totals from curtailment records
    const totals = await db
      .select({
        recordCount: sql`COUNT(*)`,
        periodCount: sql`COUNT(DISTINCT ${curtailmentRecords.settlementPeriod})`,
        farmCount: sql`COUNT(DISTINCT ${curtailmentRecords.farmId})`,
        totalVolume: sql`SUM(${curtailmentRecords.volume})`,
        totalPayment: sql`SUM(${curtailmentRecords.payment})`
      })
      .from(curtailmentRecords)
      .where(sql`${curtailmentRecords.settlementDate} = ${'2025-03-29'}`);
    
    if (!totals.length || !totals[0].totalVolume || !totals[0].totalPayment) {
      throw new Error("Failed to retrieve totals from curtailment records");
    }
    
    const { recordCount, periodCount, farmCount, totalVolume, totalPayment } = totals[0];
    
    // 2. Check if there's an existing daily summary
    const existingSummary = await db
      .select()
      .from(dailySummaries)
      .where(sql`${dailySummaries.summaryDate} = ${'2025-03-29'}`)
      .limit(1);
    
    if (existingSummary.length > 0) {
      // 3a. Update the existing summary
      log(`Found existing summary for 2025-03-29, updating...`);
      
      await db
        .update(dailySummaries)
        .set({
          totalCurtailedEnergy: totalVolume.toString(),
          totalPayment: totalPayment.toString(),
          lastUpdated: new Date()
        })
        .where(sql`${dailySummaries.summaryDate} = ${'2025-03-29'}`);
      
      log(`Updated daily summary for 2025-03-29`, "success");
    } else {
      // 3b. Create a new daily summary
      log(`No existing summary found for 2025-03-29, creating new record...`);
      
      await db
        .insert(dailySummaries)
        .values({
          summaryDate: '2025-03-29',
          totalCurtailedEnergy: totalVolume.toString(),
          totalPayment: totalPayment.toString(),
          totalWindGeneration: '0',
          windOnshoreGeneration: '0',
          windOffshoreGeneration: '0',
          createdAt: new Date(),
          lastUpdated: new Date()
        });
      
      log(`Created new daily summary for 2025-03-29`, "success");
    }
    
    // 4. Verify the daily summary
    const verifiedSummary = await db
      .select()
      .from(dailySummaries)
      .where(sql`${dailySummaries.summaryDate} = ${'2025-03-29'}`)
      .limit(1);
    
    if (verifiedSummary.length > 0) {
      const summary = verifiedSummary[0];
      log(`Verified daily summary: ${Math.abs(Number(summary.totalCurtailedEnergy)).toFixed(2)} MWh, £${Math.abs(Number(summary.totalPayment)).toFixed(2)}`, "success");
      log(`Record counts from query: ${recordCount} records, ${periodCount} periods, ${farmCount} farms`, "success");
    } else {
      log(`Failed to verify daily summary for 2025-03-29`, "error");
    }
    
    log("=== Daily Summary Creation Complete ===", "success");
  } catch (error) {
    log(`Error in daily summary creation: ${error}`, "error");
    throw error;
  }
}

// Run the function
createDailySummary().catch(error => {
  console.error("Fatal error:", error);
  process.exit(1);
});