/**
 * Analyze March 28, 2025 Data
 * 
 * This script will:
 * 1. Check current data for 2025-03-28
 * 2. Compare with expected values
 * 3. Fetch additional data from Elexon API if needed
 */

import { db } from './db';
import { curtailmentRecords, dailySummaries } from './db/schema';
import { eq, sql } from 'drizzle-orm';
import { config } from 'dotenv';
import fs from 'fs';
import path from 'path';

// Load environment variables
config();

const DATE_TO_ANALYZE = '2025-03-28';
const EXPECTED_PAYMENT = 3784089.62; // Expected payment according to user
const LOG_FILE = `analyze_march28_${new Date().toISOString().slice(0, 10)}.log`;

// Set up logging
async function logToFile(message: string): Promise<void> {
  await fs.promises.appendFile(
    path.join(process.cwd(), 'logs', LOG_FILE),
    `${new Date().toISOString()} - ${message}\n`
  );
}

function log(message: string, type: "info" | "success" | "warning" | "error" = "info"): void {
  const timestamp = new Date().toISOString().substring(11, 19);
  
  // Color codes for console
  const colors = {
    info: "\x1b[36m", // Cyan
    success: "\x1b[32m", // Green
    warning: "\x1b[33m", // Yellow
    error: "\x1b[31m", // Red
    reset: "\x1b[0m" // Reset
  };
  
  console.log(`${colors[type]}[${timestamp}] ${message}${colors.reset}`);
  logToFile(`[${type.toUpperCase()}] ${message}`).catch(console.error);
}

async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Load BMU mappings from JSON file
async function loadBmuMappings(): Promise<{
  [key: string]: { leadParty: string }
}> {
  try {
    const data = await fs.promises.readFile('./data/bmu_mapping.json', 'utf8');
    const mappings = JSON.parse(data) as Array<{
      elexonBmUnit: string;
      leadPartyName: string;
      fuelType: string;
    }>;
    
    // Convert to the format we need
    const result: { [key: string]: { leadParty: string } } = {};
    for (const mapping of mappings) {
      result[mapping.elexonBmUnit] = { 
        leadParty: mapping.leadPartyName 
      };
    }
    
    return result;
  } catch (error) {
    log('Error loading BMU mappings file', 'error');
    throw error;
  }
}

async function analyzeCurrentData(): Promise<void> {
  try {
    // Get the current data from curtailment_records
    const currentData = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(ABS(volume)::numeric)`,
        totalPayment: sql<string>`SUM(payment::numeric)`,
        recordCount: sql<number>`COUNT(*)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, DATE_TO_ANALYZE));

    if (!currentData[0]) {
      log(`No data found for ${DATE_TO_ANALYZE}`, 'error');
      return;
    }

    const { totalCurtailedEnergy, totalPayment, recordCount } = currentData[0];
    
    // Get the daily summary
    const dailySummary = await db
      .select()
      .from(dailySummaries)
      .where(eq(dailySummaries.summaryDate, DATE_TO_ANALYZE));

    log(`==== Current Data Analysis for ${DATE_TO_ANALYZE} ====`, 'info');
    log(`Total Curtailed Energy: ${totalCurtailedEnergy} MWh`, 'info');
    log(`Total Payment: £${totalPayment}`, 'info');
    log(`Record Count: ${recordCount}`, 'info');
    
    if (dailySummary.length > 0) {
      log(`Daily Summary - Energy: ${dailySummary[0].totalCurtailedEnergy} MWh`, 'info');
      log(`Daily Summary - Payment: £${dailySummary[0].totalPayment}`, 'info');
    } else {
      log(`No daily summary found for ${DATE_TO_ANALYZE}`, 'warning');
    }
    
    // Analysis of the discrepancy
    const currentPaymentAbs = Math.abs(parseFloat(totalPayment as string));
    const paymentDifference = EXPECTED_PAYMENT - currentPaymentAbs;
    const percentageDifference = (paymentDifference / EXPECTED_PAYMENT) * 100;
    
    log(`==== Discrepancy Analysis ====`, 'info');
    log(`Expected Payment: £${EXPECTED_PAYMENT.toFixed(2)}`, 'info');
    log(`Current Payment: £${currentPaymentAbs.toFixed(2)}`, 'info');
    log(`Difference: £${paymentDifference.toFixed(2)} (${percentageDifference.toFixed(2)}%)`, 
      paymentDifference > 0 ? 'error' : 'success');
    
    // Check for missing periods or farms
    log(`==== Period Analysis ====`, 'info');
    const periodCounts = await db
      .select({
        period: curtailmentRecords.settlementPeriod,
        count: sql<number>`COUNT(*)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, DATE_TO_ANALYZE))
      .groupBy(curtailmentRecords.settlementPeriod)
      .orderBy(curtailmentRecords.settlementPeriod);
    
    // Check for any missing periods (should be 48 periods in a day)
    const periods = periodCounts.map(p => parseInt(p.period as string, 10));
    const missingPeriods = Array.from({ length: 48 }, (_, i) => i + 1)
      .filter(p => !periods.includes(p));
    
    if (missingPeriods.length > 0) {
      log(`Missing Periods: ${missingPeriods.join(', ')}`, 'warning');
    } else {
      log(`All 48 periods are present`, 'success');
    }
    
    // Look for unusual period data
    log(`==== Period Payment Distribution ====`, 'info');
    const periodPayments = await db
      .select({
        period: curtailmentRecords.settlementPeriod,
        payment: sql<string>`SUM(payment::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, DATE_TO_ANALYZE))
      .groupBy(curtailmentRecords.settlementPeriod)
      .orderBy(curtailmentRecords.settlementPeriod);
    
    // Calculate average payment per period and identify outliers
    const payments = periodPayments.map(p => Math.abs(parseFloat(p.payment as string)));
    const avgPayment = payments.reduce((sum, val) => sum + val, 0) / payments.length;
    
    log(`Average Payment per Period: £${avgPayment.toFixed(2)}`, 'info');
    
    // Check for periods with unusually low payments
    const lowPaymentPeriods = periodPayments
      .filter(p => Math.abs(parseFloat(p.payment as string)) < avgPayment * 0.5)
      .map(p => p.period);
    
    if (lowPaymentPeriods.length > 0) {
      log(`Periods with unusually low payments: ${lowPaymentPeriods.join(', ')}`, 'warning');
    }
    
    // Check for lead parties
    log(`==== Lead Party Analysis ====`, 'info');
    const bmuMappings = await loadBmuMappings();
    
    const leadPartyData = await db
      .select({
        leadParty: curtailmentRecords.leadPartyName,
        count: sql<number>`COUNT(*)`,
        totalPayment: sql<string>`SUM(payment::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, DATE_TO_ANALYZE))
      .groupBy(curtailmentRecords.leadPartyName)
      .orderBy(sql`ABS(SUM(payment::numeric)) DESC`);
    
    const leadParties = new Map<string, { count: number, payment: number }>();
    
    for (const record of leadPartyData) {
      if (!record.leadParty) {
        log(`Record with null lead party found`, 'warning');
        continue;
      }
      
      const leadParty = record.leadParty;
      const current = leadParties.get(leadParty) || { count: 0, payment: 0 };
      
      leadParties.set(leadParty, {
        count: current.count + record.count,
        payment: current.payment + Math.abs(parseFloat(record.totalPayment as string))
      });
    }
    
    log(`Top 10 Lead Parties by Payment:`, 'info');
    Array.from(leadParties.entries())
      .sort((a, b) => b[1].payment - a[1].payment)
      .slice(0, 10)
      .forEach(([party, data], index) => {
        log(`${index + 1}. ${party}: £${data.payment.toFixed(2)} (${data.count} records)`, 'info');
      });
    
    // Summary and recommendations
    log(`\n==== Summary ====`, 'info');
    
    if (paymentDifference > 0) {
      const missingAmount = paymentDifference;
      log(`Missing approximately £${missingAmount.toFixed(2)} in payments`, 'error');
      log(`This could be due to:`, 'info');
      log(`1. Missing records for certain periods`, 'info');
      log(`2. Missing records for specific farms or lead parties`, 'info');
      log(`3. Incomplete data fetch from Elexon API`, 'info');
      
      log(`\n==== Recommendations ====`, 'info');
      log(`1. Verify the Elexon API data for ${DATE_TO_ANALYZE} is complete`, 'info');
      log(`2. Check if any specific periods need to be re-fetched`, 'info');
      log(`3. Compare with other dates to identify missing lead parties`, 'info');
    } else {
      log(`No significant payment discrepancy detected`, 'success');
    }
    
  } catch (error) {
    log(`Error analyzing data: ${error}`, 'error');
    throw error;
  }
}

// Main function
async function main() {
  try {
    log(`Starting analysis for ${DATE_TO_ANALYZE}`, 'info');
    await analyzeCurrentData();
    log(`Analysis complete`, 'success');
  } catch (error) {
    log(`Failed to complete analysis: ${error}`, 'error');
  } finally {
    process.exit(0);
  }
}

// Run the analysis
main();