/**
 * Update March 21, 2025 Energy Value (Final Correction)
 * 
 * This script corrects the energy curtailment amount for March 21, 2025
 * to match the EXACT value from the Elexon API: 50,518.72 MWh.
 */

import { db } from "./db";
import { dailySummaries, monthlySummaries, yearlySummaries } from "./db/schema";
import { eq, sql } from "drizzle-orm";
import { green, red, blue, yellow } from "colorette";
import fs from "fs";
import path from "path";

// The correct values from Elexon API (FINAL VALUE)
const CORRECT_ENERGY_AMOUNT = 50518.72; // MWh - Exact Elexon API value
const TARGET_DATE = "2025-03-21";

// Create logs directory if it doesn't exist
const logsDir = path.join(process.cwd(), "logs");
if (!fs.existsSync(logsDir)) {
  fs.mkdirSync(logsDir);
}

// Log file path for this run
const logFilePath = path.join(logsDir, `final_energy_correction_${new Date().toISOString().replace(/:/g, "-")}.log`);

// Log to both console and file
function log(message: string, type: "info" | "success" | "warning" | "error" = "info"): void {
  const timestamp = new Date().toISOString();
  const coloredMessage = type === "info" 
    ? blue(message)
    : type === "success"
    ? green(message)
    : type === "warning"
    ? yellow(message)
    : red(message);
  
  console.log(`${timestamp} - ${coloredMessage}`);
  
  // Also log to file without colors
  fs.appendFileSync(logFilePath, `${timestamp} - [${type.toUpperCase()}] ${message}\n`);
}

async function main() {
  try {
    log(`Starting final energy correction for ${TARGET_DATE}`, "info");
    
    // First, get the current value in the database
    const currentSummary = await db.query.dailySummaries.findFirst({
      where: eq(dailySummaries.summaryDate, TARGET_DATE)
    });
    
    if (!currentSummary) {
      log(`No daily summary found for ${TARGET_DATE}. Nothing to update.`, "error");
      return;
    }
    
    log(`Current energy value in database: ${Number(currentSummary.totalCurtailedEnergy).toFixed(2)} MWh`, "info");
    log(`Correct energy value from Elexon: ${CORRECT_ENERGY_AMOUNT.toFixed(2)} MWh`, "info");
    
    // Update the daily summary with the correct energy amount
    await db.update(dailySummaries)
      .set({ 
        totalCurtailedEnergy: CORRECT_ENERGY_AMOUNT.toString(),
        lastUpdated: new Date()
      })
      .where(eq(dailySummaries.summaryDate, TARGET_DATE));
    
    // Verify the update
    const updatedSummary = await db.query.dailySummaries.findFirst({
      where: eq(dailySummaries.summaryDate, TARGET_DATE)
    });
    
    if (!updatedSummary) {
      log(`Failed to retrieve updated summary for ${TARGET_DATE}`, "error");
      return;
    }
    
    const updatedEnergy = Number(updatedSummary.totalCurtailedEnergy).toFixed(2);
    
    if (updatedEnergy === CORRECT_ENERGY_AMOUNT.toFixed(2)) {
      log(`Successfully updated energy value to ${updatedEnergy} MWh`, "success");
    } else {
      log(`Update may have failed. Current value: ${updatedEnergy} MWh (Expected: ${CORRECT_ENERGY_AMOUNT.toFixed(2)} MWh)`, "warning");
    }
    
    // Update the monthly summary
    const yearMonth = TARGET_DATE.substring(0, 7); // Get YYYY-MM format
    log(`Updating monthly summary for ${yearMonth}`, "info");
    
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
    
    if (!monthlyTotals[0] || !monthlyTotals[0].totalCurtailedEnergy) {
      log('Error: No daily summaries found to create monthly summary', "error");
      return;
    }
    
    // Update the monthly summary
    await db.insert(monthlySummaries).values({
      yearMonth,
      totalCurtailedEnergy: monthlyTotals[0].totalCurtailedEnergy,
      totalPayment: monthlyTotals[0].totalPayment,
      totalWindGeneration: monthlyTotals[0].totalWindGeneration || '0',
      windOnshoreGeneration: monthlyTotals[0].windOnshoreGeneration || '0',
      windOffshoreGeneration: monthlyTotals[0].windOffshoreGeneration || '0',
      createdAt: new Date(),
      updatedAt: new Date(),
      lastUpdated: new Date()
    }).onConflictDoUpdate({
      target: [monthlySummaries.yearMonth],
      set: {
        totalCurtailedEnergy: monthlyTotals[0].totalCurtailedEnergy,
        totalPayment: monthlyTotals[0].totalPayment,
        totalWindGeneration: monthlyTotals[0].totalWindGeneration || '0',
        windOnshoreGeneration: monthlyTotals[0].windOnshoreGeneration || '0',
        windOffshoreGeneration: monthlyTotals[0].windOffshoreGeneration || '0',
        updatedAt: new Date(),
        lastUpdated: new Date()
      }
    });
    
    log(`Monthly summary updated for ${yearMonth}:`, "success");
    log(`- Energy: ${Number(monthlyTotals[0].totalCurtailedEnergy).toFixed(2)} MWh`, "info");
    
    // Update yearly summary
    const year = TARGET_DATE.substring(0, 4);
    log(`Updating yearly summary for ${year}`, "info");
    
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
    
    if (!yearlyTotals[0] || !yearlyTotals[0].totalCurtailedEnergy) {
      log('Error: No daily summaries found to create yearly summary', "error");
      return;
    }
    
    // Update the yearly summary
    await db.insert(yearlySummaries).values({
      year,
      totalCurtailedEnergy: yearlyTotals[0].totalCurtailedEnergy,
      totalPayment: yearlyTotals[0].totalPayment,
      totalWindGeneration: yearlyTotals[0].totalWindGeneration || '0',
      windOnshoreGeneration: yearlyTotals[0].windOnshoreGeneration || '0',
      windOffshoreGeneration: yearlyTotals[0].windOffshoreGeneration || '0',
      createdAt: new Date(),
      updatedAt: new Date(),
      lastUpdated: new Date()
    }).onConflictDoUpdate({
      target: [yearlySummaries.year],
      set: {
        totalCurtailedEnergy: yearlyTotals[0].totalCurtailedEnergy,
        totalPayment: yearlyTotals[0].totalPayment,
        totalWindGeneration: yearlyTotals[0].totalWindGeneration || '0',
        windOnshoreGeneration: yearlyTotals[0].windOnshoreGeneration || '0',
        windOffshoreGeneration: yearlyTotals[0].windOffshoreGeneration || '0',
        updatedAt: new Date(),
        lastUpdated: new Date()
      }
    });
    
    log(`Yearly summary updated for ${year}:`, "success");
    log(`- Energy: ${Number(yearlyTotals[0].totalCurtailedEnergy).toFixed(2)} MWh`, "info");
    
    // Update Bitcoin calculations
    log(`Updating Bitcoin calculations...`, "info");
    const minerModels = ['S19J_PRO', 'S9', 'M20S'];
    const { processSingleDay } = await import('./server/services/bitcoinService');
    
    for (const minerModel of minerModels) {
      await processSingleDay(TARGET_DATE, minerModel);
      log(`- Updated calculations for ${minerModel}`, "info");
    }
    
    log(`Bitcoin calculations updated successfully`, "success");
    
    // Create a record of this update for auditing purposes
    await fs.promises.appendFile(
      path.join(logsDir, 'data_corrections.log'),
      `${new Date().toISOString()} - Final energy correction for ${TARGET_DATE}:\n` + 
      `  Energy: ${Number(currentSummary.totalCurtailedEnergy).toFixed(2)} MWh → ${CORRECT_ENERGY_AMOUNT.toFixed(2)} MWh\n`
    );
    
    log("Energy correction process completed successfully", "success");
    
  } catch (error) {
    log(`Error in update process: ${(error as Error).message}`, "error");
    log((error as Error).stack || "No stack trace available", "error");
  }
}

main().catch(error => {
  log(`Uncaught error in main: ${error.message}`, "error");
  process.exit(1);
});