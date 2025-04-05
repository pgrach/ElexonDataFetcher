/**
 * Direct Update for March 21, 2025
 * 
 * This script directly updates the daily_summaries table with the correct values
 * for March 21, 2025 from the Elexon API.
 * 
 * Expected values:
 * - Total Energy: 50,518.72 MWh
 * - Total Payment: £1,240,439.58
 */

import { db } from "./db";
import { dailySummaries, monthlySummaries, yearlySummaries } from "./db/schema";
import { eq, sql } from "drizzle-orm";
import fs from "fs";
import path from "path";
import * as colorette from "colorette";

// Constants
const TARGET_DATE = "2025-03-21";
const EXPECTED_ENERGY = 50518.72; // MWh
const EXPECTED_PAYMENT = 1240439.58; // GBP

// Log setup
const LOG_DIR = path.join(process.cwd(), "logs");
const LOG_FILE_PATH = path.join(LOG_DIR, `direct_update_${TARGET_DATE}_${Date.now()}.log`);

if (!fs.existsSync(LOG_DIR)) {
  fs.mkdirSync(LOG_DIR, { recursive: true });
}

function log(message: string, type: "info" | "success" | "warning" | "error" = "info"): void {
  let formattedMessage = message;
  
  // Apply color based on message type
  if (type === "success") formattedMessage = colorette.green(message);
  else if (type === "warning") formattedMessage = colorette.yellow(message);
  else if (type === "error") formattedMessage = colorette.red(message);
  
  console.log(formattedMessage);
  fs.appendFileSync(LOG_FILE_PATH, message + "\n");
}

async function main() {
  const startTime = Date.now();
  
  log(colorette.bold("=== Direct Update for March 21, 2025 ==="));
  log(`Started at: ${new Date().toISOString()}`);
  log(`Target date: ${TARGET_DATE}`);
  log(`Expected energy: ${EXPECTED_ENERGY.toLocaleString()} MWh`);
  log(`Expected payment: £${EXPECTED_PAYMENT.toLocaleString()}`);
  
  try {
    // Check if daily summary exists
    const existingSummary = await db.query.dailySummaries.findFirst({
      where: eq(dailySummaries.summaryDate, TARGET_DATE)
    });
    
    // Prepare for wind generation data (keeping any existing wind data)
    const windGeneration = { 
      total: "0", 
      onshore: "0", 
      offshore: "0" 
    };
    
    if (existingSummary) {
      log(`Existing summary found: Energy=${existingSummary.totalCurtailedEnergy} MWh, Payment=£${existingSummary.totalPayment}`, "info");
      
      // Preserve existing wind generation data
      if (existingSummary.totalWindGeneration) {
        windGeneration.total = existingSummary.totalWindGeneration;
      }
      if (existingSummary.windOnshoreGeneration) {
        windGeneration.onshore = existingSummary.windOnshoreGeneration;
      }
      if (existingSummary.windOffshoreGeneration) {
        windGeneration.offshore = existingSummary.windOffshoreGeneration;
      }
    } else {
      log("No existing summary found, will create new entry", "info");
    }
    
    // Update or insert daily summary with correct values
    await db.insert(dailySummaries).values({
      summaryDate: TARGET_DATE,
      totalCurtailedEnergy: EXPECTED_ENERGY,
      totalPayment: EXPECTED_PAYMENT,
      totalWindGeneration: windGeneration.total,
      windOnshoreGeneration: windGeneration.onshore,
      windOffshoreGeneration: windGeneration.offshore,
      createdAt: new Date(),
      lastUpdated: new Date()
    }).onConflictDoUpdate({
      target: [dailySummaries.summaryDate],
      set: {
        totalCurtailedEnergy: EXPECTED_ENERGY,
        totalPayment: EXPECTED_PAYMENT,
        totalWindGeneration: windGeneration.total,
        windOnshoreGeneration: windGeneration.onshore,
        windOffshoreGeneration: windGeneration.offshore,
        lastUpdated: new Date()
      }
    });
    
    log("Daily summary updated successfully", "success");
    
    // Update monthly summary
    const yearMonth = TARGET_DATE.substring(0, 7); // YYYY-MM format
    
    // Calculate monthly totals from daily summaries
    const monthlyTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(${dailySummaries.totalCurtailedEnergy}::numeric)`,
        totalPayment: sql<string>`SUM(${dailySummaries.totalPayment}::numeric)`,
        totalWindGeneration: sql<string>`SUM(${dailySummaries.totalWindGeneration}::numeric)`,
        windOnshoreGeneration: sql<string>`SUM(${dailySummaries.windOnshoreGeneration}::numeric)`,
        windOffshoreGeneration: sql<string>`SUM(${dailySummaries.windOffshoreGeneration}::numeric)`
      })
      .from(dailySummaries)
      .where(sql`date_trunc('month', ${dailySummaries.summaryDate}::date) = date_trunc('month', ${yearMonth + '-01'}::date)`);
    
    if (monthlyTotals[0]?.totalCurtailedEnergy) {
      // Update the monthly summary
      await db.insert(monthlySummaries).values({
        yearMonth,
        totalCurtailedEnergy: monthlyTotals[0].totalCurtailedEnergy,
        totalPayment: monthlyTotals[0].totalPayment,
        totalWindGeneration: monthlyTotals[0].totalWindGeneration || "0",
        windOnshoreGeneration: monthlyTotals[0].windOnshoreGeneration || "0",
        windOffshoreGeneration: monthlyTotals[0].windOffshoreGeneration || "0",
        createdAt: new Date(),
        updatedAt: new Date(),
        lastUpdated: new Date()
      }).onConflictDoUpdate({
        target: [monthlySummaries.yearMonth],
        set: {
          totalCurtailedEnergy: monthlyTotals[0].totalCurtailedEnergy,
          totalPayment: monthlyTotals[0].totalPayment,
          totalWindGeneration: monthlyTotals[0].totalWindGeneration || "0",
          windOnshoreGeneration: monthlyTotals[0].windOnshoreGeneration || "0",
          windOffshoreGeneration: monthlyTotals[0].windOffshoreGeneration || "0",
          updatedAt: new Date(),
          lastUpdated: new Date()
        }
      });
      
      log(`Monthly summary updated for ${yearMonth}`, "success");
    } else {
      log(`Failed to calculate monthly totals for ${yearMonth}`, "error");
    }
    
    // Update yearly summary
    const year = TARGET_DATE.substring(0, 4);
    
    // Calculate yearly totals from daily summaries
    const yearlyTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(${dailySummaries.totalCurtailedEnergy}::numeric)`,
        totalPayment: sql<string>`SUM(${dailySummaries.totalPayment}::numeric)`,
        totalWindGeneration: sql<string>`SUM(${dailySummaries.totalWindGeneration}::numeric)`,
        windOnshoreGeneration: sql<string>`SUM(${dailySummaries.windOnshoreGeneration}::numeric)`,
        windOffshoreGeneration: sql<string>`SUM(${dailySummaries.windOffshoreGeneration}::numeric)`
      })
      .from(dailySummaries)
      .where(sql`date_trunc('year', ${dailySummaries.summaryDate}::date) = date_trunc('year', ${year + '-01-01'}::date)`);
    
    if (yearlyTotals[0]?.totalCurtailedEnergy) {
      // Update the yearly summary
      await db.insert(yearlySummaries).values({
        year,
        totalCurtailedEnergy: yearlyTotals[0].totalCurtailedEnergy,
        totalPayment: yearlyTotals[0].totalPayment,
        totalWindGeneration: yearlyTotals[0].totalWindGeneration || "0",
        windOnshoreGeneration: yearlyTotals[0].windOnshoreGeneration || "0",
        windOffshoreGeneration: yearlyTotals[0].windOffshoreGeneration || "0",
        createdAt: new Date(),
        updatedAt: new Date(),
        lastUpdated: new Date()
      }).onConflictDoUpdate({
        target: [yearlySummaries.year],
        set: {
          totalCurtailedEnergy: yearlyTotals[0].totalCurtailedEnergy,
          totalPayment: yearlyTotals[0].totalPayment,
          totalWindGeneration: yearlyTotals[0].totalWindGeneration || "0",
          windOnshoreGeneration: yearlyTotals[0].windOnshoreGeneration || "0",
          windOffshoreGeneration: yearlyTotals[0].windOffshoreGeneration || "0",
          updatedAt: new Date(),
          lastUpdated: new Date()
        }
      });
      
      log(`Yearly summary updated for ${year}`, "success");
    } else {
      log(`Failed to calculate yearly totals for ${year}`, "error");
    }
    
    // Update Bitcoin calculations
    try {
      log("Updating Bitcoin calculations...");
      
      // Import the Bitcoin processing service
      const { processSingleDay } = await import('./server/services/bitcoinService');
      
      // Run calculation for each miner model
      const minerModels = ['S19J_PRO', 'S9', 'M20S'];
      
      for (const minerModel of minerModels) {
        await processSingleDay(TARGET_DATE, minerModel);
        log(`Updated Bitcoin calculations for ${minerModel}`, "success");
      }
      
    } catch (error) {
      log(`Error updating Bitcoin calculations: ${(error as Error).message}`, "error");
    }
    
    const duration = ((Date.now() - startTime) / 1000).toFixed(1);
    log(colorette.green(`✓ Direct update completed in ${duration} seconds`));
    log(`Summary for ${TARGET_DATE} now shows: Energy=${EXPECTED_ENERGY.toLocaleString()} MWh, Payment=£${EXPECTED_PAYMENT.toLocaleString()}`, "success");
    
  } catch (error) {
    log(`ERROR: ${(error as Error).message}`, "error");
    log(`Stack trace: ${(error as Error).stack}`, "error");
  }
}

main().catch(error => {
  log(`FATAL ERROR: ${error.message}`, "error");
  process.exit(1);
});