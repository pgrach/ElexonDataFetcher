/**
 * Compare March 28 Data with a Reference Date
 * 
 * This script will:
 * 1. Check for missing lead parties or farms between March 28 and a reference date
 * 2. Show payment differences
 * 3. Identify potential causes of the discrepancy
 */

import { db } from './db';
import { curtailmentRecords } from './db/schema';
import { eq, sql, and, desc } from 'drizzle-orm';
import fs from 'fs';
import path from 'path';

const TARGET_DATE = '2025-03-28';
const REFERENCE_DATE = '2025-03-29'; // Using March 29 as reference since it's correct
const LOG_FILE = `compare_march28_${new Date().toISOString().slice(0, 10)}.log`;

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

async function compareLeadParties(): Promise<void> {
  try {
    log(`Comparing lead parties between ${TARGET_DATE} and ${REFERENCE_DATE}...`, 'info');
    
    // Get lead parties from target date
    const targetParties = await db
      .select({
        leadParty: curtailmentRecords.leadPartyName,
        count: sql<number>`COUNT(*)`,
        totalPayment: sql<string>`SUM(payment::numeric)`,
        totalEnergy: sql<string>`SUM(ABS(volume)::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE))
      .groupBy(curtailmentRecords.leadPartyName)
      .orderBy(sql`ABS(SUM(payment::numeric)) DESC`);
    
    // Get lead parties from reference date
    const referenceParties = await db
      .select({
        leadParty: curtailmentRecords.leadPartyName,
        count: sql<number>`COUNT(*)`,
        totalPayment: sql<string>`SUM(payment::numeric)`,
        totalEnergy: sql<string>`SUM(ABS(volume)::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, REFERENCE_DATE))
      .groupBy(curtailmentRecords.leadPartyName)
      .orderBy(sql`ABS(SUM(payment::numeric)) DESC`);
    
    // Convert to maps for easier comparison
    const targetMap = new Map(targetParties.map(p => [p.leadParty, p]));
    const referenceMap = new Map(referenceParties.map(p => [p.leadParty, p]));
    
    // Find parties in reference but not in target
    const missingParties = [];
    for (const [name, data] of referenceMap.entries()) {
      if (name && !targetMap.has(name)) {
        missingParties.push({
          name,
          count: data.count,
          payment: Math.abs(parseFloat(data.totalPayment as string)),
          energy: parseFloat(data.totalEnergy as string)
        });
      }
    }
    
    // Find parties with significant payment differences
    const diffParties = [];
    for (const [name, targetData] of targetMap.entries()) {
      if (!name) continue;
      
      const referenceData = referenceMap.get(name);
      if (referenceData) {
        const targetPayment = Math.abs(parseFloat(targetData.totalPayment as string));
        const referencePayment = Math.abs(parseFloat(referenceData.totalPayment as string));
        
        // If the difference is more than 20%
        const percentDiff = Math.abs((targetPayment - referencePayment) / referencePayment) * 100;
        if (percentDiff > 20 && Math.abs(targetPayment - referencePayment) > 10000) {
          diffParties.push({
            name,
            targetPayment,
            referencePayment,
            diff: targetPayment - referencePayment,
            percentDiff
          });
        }
      }
    }
    
    // Calculate total payments
    const totalTargetPayment = targetParties.reduce((sum, p) => sum + Math.abs(parseFloat(p.totalPayment as string)), 0);
    const totalReferencePayment = referenceParties.reduce((sum, p) => sum + Math.abs(parseFloat(p.totalPayment as string)), 0);
    
    log(`\n==== Payment Totals ====`, 'info');
    log(`${TARGET_DATE}: £${totalTargetPayment.toFixed(2)}`, 'info');
    log(`${REFERENCE_DATE}: £${totalReferencePayment.toFixed(2)}`, 'info');
    log(`Difference: £${(totalReferencePayment - totalTargetPayment).toFixed(2)}`, 'info');
    log(`Percentage: ${((totalReferencePayment - totalTargetPayment) / totalReferencePayment * 100).toFixed(2)}%`, 'info');
    
    // Log missing parties
    if (missingParties.length > 0) {
      log(`\n==== Lead Parties in ${REFERENCE_DATE} but missing from ${TARGET_DATE} ====`, 'warning');
      let totalMissingPayment = 0;
      
      missingParties.sort((a, b) => b.payment - a.payment);
      for (const party of missingParties) {
        log(`${party.name}: £${party.payment.toFixed(2)} (${party.count} records, ${party.energy.toFixed(2)} MWh)`, 'warning');
        totalMissingPayment += party.payment;
      }
      
      log(`\nTotal missing payment from these parties: £${totalMissingPayment.toFixed(2)}`, 'error');
      log(`This accounts for ${(totalMissingPayment / (totalReferencePayment - totalTargetPayment) * 100).toFixed(2)}% of the total difference`, 'info');
      
      // List farms in these missing parties 
      log(`\n==== Top Farms from Missing Lead Parties (based on ${REFERENCE_DATE}) ====`, 'info');
      for (const party of missingParties.slice(0, 5)) { // Top 5 missing parties
        const farms = await db
          .select({
            farmId: curtailmentRecords.farmId,
            count: sql<number>`COUNT(*)`,
            totalPayment: sql<string>`SUM(payment::numeric)`,
            totalEnergy: sql<string>`SUM(ABS(volume)::numeric)`
          })
          .from(curtailmentRecords)
          .where(
            and(
              eq(curtailmentRecords.settlementDate, REFERENCE_DATE),
              eq(curtailmentRecords.leadPartyName, party.name)
            )
          )
          .groupBy(curtailmentRecords.farmId)
          .orderBy(sql`ABS(SUM(payment::numeric)) DESC`);
        
        if (farms.length > 0) {
          log(`\n${party.name} Farms:`, 'info');
          farms.forEach(f => {
            const payment = Math.abs(parseFloat(f.totalPayment as string));
            log(`- ${f.farmId}: £${payment.toFixed(2)} (${f.count} records, ${parseFloat(f.totalEnergy as string).toFixed(2)} MWh)`, 'info');
          });
        }
      }
    } else {
      log(`\nNo lead parties missing between dates`, 'success');
    }
    
    // Log parties with big differences
    if (diffParties.length > 0) {
      log(`\n==== Lead Parties with Significant Payment Differences ====`, 'warning');
      diffParties.sort((a, b) => b.diff - a.diff);
      
      for (const party of diffParties) {
        log(`${party.name}:`, 'warning');
        log(`  ${TARGET_DATE}: £${party.targetPayment.toFixed(2)}`, 'info');
        log(`  ${REFERENCE_DATE}: £${party.referencePayment.toFixed(2)}`, 'info');
        log(`  Difference: £${party.diff.toFixed(2)} (${party.percentDiff.toFixed(2)}%)`, party.diff > 0 ? 'success' : 'error');
      }
    } else {
      log(`\nNo significant payment differences found for common lead parties`, 'success');
    }
    
    log(`\n==== Top 10 Lead Parties by Payment for ${TARGET_DATE} ====`, 'info');
    targetParties
      .filter(p => p.leadParty) // Filter out null lead parties
      .slice(0, 10)
      .forEach(p => {
        const payment = Math.abs(parseFloat(p.totalPayment as string));
        log(`${p.leadParty}: £${payment.toFixed(2)} (${p.count} records, ${parseFloat(p.totalEnergy as string).toFixed(2)} MWh)`, 'info');
      });
      
    log(`\n==== Top 10 Lead Parties by Payment for ${REFERENCE_DATE} ====`, 'info');
    referenceParties
      .filter(p => p.leadParty) // Filter out null lead parties
      .slice(0, 10)
      .forEach(p => {
        const payment = Math.abs(parseFloat(p.totalPayment as string));
        log(`${p.leadParty}: £${payment.toFixed(2)} (${p.count} records, ${parseFloat(p.totalEnergy as string).toFixed(2)} MWh)`, 'info');
      });
      
    // Check for Seagreen Wind Energy Limited - a major lead party in our analysis earlier
    log(`\n==== Detailed Comparison of Critical Lead Parties ====`, 'info');
    const criticalParties = ['Seagreen Wind Energy Limited', 'Vattenfall Wind Power Ltd', 'Beatrice Offshore Windfarm Ltd'];
    
    for (const party of criticalParties) {
      const targetData = targetMap.get(party);
      const referenceData = referenceMap.get(party);
      
      if (targetData && referenceData) {
        const targetPayment = Math.abs(parseFloat(targetData.totalPayment as string));
        const referencePayment = Math.abs(parseFloat(referenceData.totalPayment as string));
        const paymentDiff = referencePayment - targetPayment;
        
        log(`\n${party}:`, 'info');
        log(`  ${TARGET_DATE}: £${targetPayment.toFixed(2)} (${targetData.count} records)`, 'info');
        log(`  ${REFERENCE_DATE}: £${referencePayment.toFixed(2)} (${referenceData.count} records)`, 'info');
        log(`  Difference: £${paymentDiff.toFixed(2)} (${(paymentDiff / referencePayment * 100).toFixed(2)}%)`, 'info');
        
        // Get farms for this lead party on both dates
        log(`  Farms for ${TARGET_DATE}:`, 'info');
        const targetFarms = await db
          .select({
            farmId: curtailmentRecords.farmId,
            count: sql<number>`COUNT(*)`,
            totalPayment: sql<string>`SUM(payment::numeric)`
          })
          .from(curtailmentRecords)
          .where(
            and(
              eq(curtailmentRecords.settlementDate, TARGET_DATE),
              eq(curtailmentRecords.leadPartyName, party)
            )
          )
          .groupBy(curtailmentRecords.farmId)
          .orderBy(sql`ABS(SUM(payment::numeric)) DESC`);
          
        for (const farm of targetFarms) {
          const payment = Math.abs(parseFloat(farm.totalPayment as string));
          log(`    ${farm.farmId}: £${payment.toFixed(2)} (${farm.count} records)`, 'info');
        }
        
        log(`  Farms for ${REFERENCE_DATE}:`, 'info');
        const referenceFarms = await db
          .select({
            farmId: curtailmentRecords.farmId,
            count: sql<number>`COUNT(*)`,
            totalPayment: sql<string>`SUM(payment::numeric)`
          })
          .from(curtailmentRecords)
          .where(
            and(
              eq(curtailmentRecords.settlementDate, REFERENCE_DATE),
              eq(curtailmentRecords.leadPartyName, party)
            )
          )
          .groupBy(curtailmentRecords.farmId)
          .orderBy(sql`ABS(SUM(payment::numeric)) DESC`);
          
        for (const farm of referenceFarms) {
          const payment = Math.abs(parseFloat(farm.totalPayment as string));
          log(`    ${farm.farmId}: £${payment.toFixed(2)} (${farm.count} records)`, 'info');
        }
      } else if (!targetData && referenceData) {
        const payment = Math.abs(parseFloat(referenceData.totalPayment as string));
        log(`\n${party}: MISSING from ${TARGET_DATE}, has £${payment.toFixed(2)} on ${REFERENCE_DATE}`, 'error');
      } else if (targetData && !referenceData) {
        const payment = Math.abs(parseFloat(targetData.totalPayment as string));
        log(`\n${party}: Present on ${TARGET_DATE} with £${payment.toFixed(2)}, but missing from ${REFERENCE_DATE}`, 'warning');
      }
    }
  } catch (error) {
    log(`Error comparing lead parties: ${error}`, 'error');
    throw error;
  }
}

// Main function
async function main() {
  try {
    log(`Starting comparison between ${TARGET_DATE} and ${REFERENCE_DATE}`, 'info');
    await compareLeadParties();
    log(`Comparison complete`, 'success');
  } catch (error) {
    log(`Error in comparison process: ${error}`, 'error');
  } finally {
    process.exit(0);
  }
}

// Run the script
main();