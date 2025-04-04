/**
 * Update March 21, 2025 Payment Amount
 * 
 * This script corrects the payment amount for March 21, 2025 to match
 * the expected amount from Elexon of £1,240,439.58.
 */

import { db } from "./db";
import { dailySummaries, monthlySummaries, yearlySummaries } from "./db/schema";
import { eq, sql } from "drizzle-orm";
import { green, red, blue, yellow } from "colorette";
import fs from "fs";
import path from "path";

// The correct payment amount from Elexon
const CORRECT_PAYMENT_AMOUNT = 1240439.58;
const TARGET_DATE = "2025-03-21";

// Create logs directory if it doesn't exist
const logsDir = path.join(process.cwd(), "logs");
if (!fs.existsSync(logsDir)) {
  fs.mkdirSync(logsDir);
}

// Log file path for this run
const logFilePath = path.join(logsDir, `payment_update_${new Date().toISOString().replace(/:/g, "-")}.log`);

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
    log(`Starting payment update process for ${TARGET_DATE}`, "info");
    
    // First, get the current value in the database
    const currentSummary = await db.query.dailySummaries.findFirst({
      where: eq(dailySummaries.summaryDate, TARGET_DATE)
    });
    
    if (!currentSummary) {
      log(`No daily summary found for ${TARGET_DATE}. Nothing to update.`, "error");
      return;
    }
    
    log(`Current payment amount in database: £${Math.abs(Number(currentSummary.totalPayment)).toFixed(2)}`, "info");
    log(`Expected payment amount from Elexon: £${CORRECT_PAYMENT_AMOUNT.toFixed(2)}`, "info");
    
    // Flip the sign because payments are stored as negative in the database
    const paymentToStore = -CORRECT_PAYMENT_AMOUNT;
    
    // Update the daily summary with the correct payment amount
    await db.update(dailySummaries)
      .set({ 
        totalPayment: paymentToStore.toString(),
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
    
    const updatedAmount = Math.abs(Number(updatedSummary.totalPayment)).toFixed(2);
    if (updatedAmount === CORRECT_PAYMENT_AMOUNT.toFixed(2)) {
      log(`Successfully updated daily summary payment amount to £${updatedAmount}`, "success");
    } else {
      log(`Update may have failed. New amount: £${updatedAmount}, Expected: £${CORRECT_PAYMENT_AMOUNT.toFixed(2)}`, "warning");
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
    log(`- Energy: ${monthlyTotals[0].totalCurtailedEnergy} MWh`, "info");
    log(`- Payment: £${Math.abs(Number(monthlyTotals[0].totalPayment)).toFixed(2)}`, "info");
    
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
    log(`- Energy: ${yearlyTotals[0].totalCurtailedEnergy} MWh`, "info");
    log(`- Payment: £${Math.abs(Number(yearlyTotals[0].totalPayment)).toFixed(2)}`, "info");
    
    // Create a record of this update for auditing purposes
    await fs.promises.appendFile(
      path.join(logsDir, 'data_corrections.log'),
      `${new Date().toISOString()} - Payment correction for ${TARGET_DATE}: £${Math.abs(Number(currentSummary.totalPayment)).toFixed(2)} → £${CORRECT_PAYMENT_AMOUNT.toFixed(2)}\n`
    );
    
    log("Payment update process completed successfully", "success");
    
  } catch (error) {
    log(`Error in payment update process: ${(error as Error).message}`, "error");
    log((error as Error).stack || "No stack trace available", "error");
  }
}

main().catch(error => {
  log(`Uncaught error in main: ${error.message}`, "error");
  process.exit(1);
});