/**
 * Fetch Missing Elexon API Data for 2025-03-28
 * 
 * This script will:
 * 1. Check for missing lead parties compared to other days
 * 2. Attempt to fetch any missing data from Elexon API
 * 3. Update the database with the retrieved data
 */

import { db } from './db';
import { curtailmentRecords, dailySummaries, monthlySummaries, yearlySummaries } from './db/schema';
import { eq, sql, ne, and, gte, lte } from 'drizzle-orm';
import { config } from 'dotenv';
import fs from 'fs';
import path from 'path';
import axios from 'axios';

// Load environment variables
config();

const DATE_TO_FIX = '2025-03-28';
const LOG_FILE = `fetch_missing_march28_${new Date().toISOString().slice(0, 10)}.log`;
const ELEXON_API_KEY = process.env.ELEXON_API_KEY;

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

async function findMissingLeadParties(): Promise<Set<string>> {
  try {
    log(`Finding missing lead parties for ${DATE_TO_FIX}...`, 'info');
    
    // Get lead parties present on March 29 (the day after)
    const nextDayParties = await db
      .select({
        leadParty: curtailmentRecords.leadPartyName,
        count: sql<number>`COUNT(*)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, '2025-03-29'))
      .groupBy(curtailmentRecords.leadPartyName);
    
    // Get lead parties present on the target day
    const targetDayParties = await db
      .select({
        leadParty: curtailmentRecords.leadPartyName,
        count: sql<number>`COUNT(*)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, DATE_TO_FIX))
      .groupBy(curtailmentRecords.leadPartyName);
    
    const nextDayPartyNames = new Set(nextDayParties.map(p => p.leadParty));
    const targetDayPartyNames = new Set(targetDayParties.map(p => p.leadParty));
    
    // Find parties present on March 29 but not on March 28
    const missingParties = new Set<string>();
    for (const party of nextDayPartyNames) {
      if (party && !targetDayPartyNames.has(party)) {
        missingParties.add(party);
      }
    }
    
    if (missingParties.size > 0) {
      log(`Found ${missingParties.size} lead parties missing on ${DATE_TO_FIX}:`, 'warning');
      for (const party of missingParties) {
        log(`- ${party}`, 'warning');
      }
    } else {
      log(`No missing lead parties found between days.`, 'success');
    }
    
    return missingParties;
  } catch (error) {
    log(`Error finding missing lead parties: ${error}`, 'error');
    throw error;
  }
}

async function fetchElexonData(settlementDate: string): Promise<any> {
  try {
    log(`Fetching Elexon data for ${settlementDate}...`, 'info');
    
    const url = `https://data.elexon.co.uk/bmrs/api/v1/datasets/PHYBMDATA/date/${settlementDate}`;
    
    const response = await axios.get(url, {
      headers: {
        'Accept': 'application/json',
        'Cache-Control': 'no-cache'
      }
    });
    
    if (response.status !== 200) {
      throw new Error(`Elexon API returned status code ${response.status}`);
    }
    
    return response.data;
  } catch (error) {
    log(`Error fetching Elexon data: ${error}`, 'error');
    throw error;
  }
}

// Load BMU mappings from JSON file
async function loadBmuMappingsArray(): Promise<Array<{
  elexonBmUnit: string;
  leadPartyName: string;
  fuelType: string;
}>> {
  try {
    const data = await fs.promises.readFile('./data/bmu_mapping.json', 'utf8');
    return JSON.parse(data);
  } catch (error) {
    log('Error loading BMU mappings file', 'error');
    throw error;
  }
}

async function processElexonData(data: any, missingLeadParties: Set<string>): Promise<number> {
  try {
    log(`Processing Elexon data...`, 'info');
    
    const bmuMappings = await loadBmuMappingsArray();
    const mappedLeadParties = new Map<string, string>();
    
    // Create a map of BMU ID to lead party name
    for (const mapping of bmuMappings) {
      mappedLeadParties.set(mapping.elexonBmUnit, mapping.leadPartyName);
    }
    
    // Get BMU IDs for missing lead parties
    const missingPartyBmus = new Set<string>();
    for (const [bmuId, leadParty] of mappedLeadParties.entries()) {
      if (missingLeadParties.has(leadParty)) {
        missingPartyBmus.add(bmuId);
      }
    }
    
    if (missingPartyBmus.size === 0) {
      log(`No BMU IDs found for missing lead parties.`, 'warning');
      return 0;
    }
    
    log(`Found ${missingPartyBmus.size} BMU IDs for missing lead parties.`, 'info');
    
    // Process the data and insert records
    let insertedCount = 0;
    const records = data.data || [];
    
    for (const record of records) {
      const bmuId = record.bmUnit;
      
      // Check if this BMU ID belongs to a missing lead party
      if (!missingPartyBmus.has(bmuId)) {
        continue;
      }
      
      const leadPartyName = mappedLeadParties.get(bmuId);
      if (!leadPartyName) {
        continue;
      }
      
      // Check if already exists
      const existingRecord = await db
        .select({ id: curtailmentRecords.id })
        .from(curtailmentRecords)
        .where(
          and(
            eq(curtailmentRecords.settlementDate, DATE_TO_FIX),
            eq(curtailmentRecords.settlementPeriod, record.settlementPeriod.toString()),
            eq(curtailmentRecords.farmId, bmuId)
          )
        )
        .limit(1);
      
      if (existingRecord.length > 0) {
        continue; // Skip if already exists
      }
      
      // Calculate payment
      const volume = parseFloat(record.quantity);
      const originalPrice = parseFloat(record.originalBidOffer);
      const finalPrice = parseFloat(record.bidOfferPrice);
      const payment = volume * finalPrice;
      
      // Insert the record
      await db.insert(curtailmentRecords).values({
        settlementDate: DATE_TO_FIX,
        settlementPeriod: record.settlementPeriod,
        farmId: bmuId,
        leadPartyName,
        volume: volume.toString(),
        payment: payment.toString(),
        originalPrice: originalPrice.toString(),
        finalPrice: finalPrice.toString(),
        soFlag: record.soFlag,
        cadlFlag: record.cadlFlag,
        createdAt: new Date()
      });
      
      insertedCount++;
      
      log(`Inserted record for ${bmuId} (${leadPartyName}) for period ${record.settlementPeriod}`, 'success');
    }
    
    log(`Inserted ${insertedCount} new records from Elexon data.`, 'success');
    
    return insertedCount;
  } catch (error) {
    log(`Error processing Elexon data: ${error}`, 'error');
    throw error;
  }
}

async function updateSummaries(): Promise<void> {
  try {
    log(`Updating summaries for ${DATE_TO_FIX}...`, 'info');
    
    // Get the correct totals from curtailment_records
    const correctedTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(ABS(volume)::numeric)`,
        totalPayment: sql<string>`SUM(payment::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, DATE_TO_FIX));

    if (!correctedTotals[0]?.totalCurtailedEnergy) {
      log(`No curtailment records found for ${DATE_TO_FIX}, skipping`, 'warning');
      return;
    }

    const totalCurtailedEnergy = correctedTotals[0].totalCurtailedEnergy;
    const totalPayment = correctedTotals[0].totalPayment;

    // Get current values for comparison
    const currentSummary = await db
      .select()
      .from(dailySummaries)
      .where(eq(dailySummaries.summaryDate, DATE_TO_FIX));

    log("Current Summary vs Updated Values:", 'info');
    log(`- Energy: ${currentSummary[0]?.totalCurtailedEnergy || 'N/A'} => ${totalCurtailedEnergy} MWh`, 'info');
    log(`- Payment: £${currentSummary[0]?.totalPayment || 'N/A'} => £${totalPayment}`, 'info');

    // Update the daily summary
    await db.insert(dailySummaries).values({
      summaryDate: DATE_TO_FIX,
      totalCurtailedEnergy,
      totalPayment,
      lastUpdated: new Date()
    }).onConflictDoUpdate({
      target: [dailySummaries.summaryDate],
      set: {
        totalCurtailedEnergy,
        totalPayment,
        lastUpdated: new Date()
      }
    });
    
    log(`Daily summary for ${DATE_TO_FIX} updated successfully`, 'success');
    
    // Update monthly and yearly summaries
    log(`Updating monthly and yearly summaries...`, 'info');
    
    const yearMonth = DATE_TO_FIX.substring(0, 7);
    const year = DATE_TO_FIX.substring(0, 4);
    
    // Update monthly summary
    const monthlyTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(${dailySummaries.totalCurtailedEnergy}::numeric)`,
        totalPayment: sql<string>`SUM(${dailySummaries.totalPayment}::numeric)`
      })
      .from(dailySummaries)
      .where(sql`date_trunc('month', ${dailySummaries.summaryDate}::date) = date_trunc('month', ${yearMonth + '-01'}::date)`);

    if (!monthlyTotals[0]?.totalCurtailedEnergy) {
      log(`No daily summaries found for ${yearMonth}, skipping monthly update`, 'warning');
    } else {
      await db.insert(monthlySummaries).values({
        yearMonth,
        totalCurtailedEnergy: monthlyTotals[0].totalCurtailedEnergy,
        totalPayment: monthlyTotals[0].totalPayment,
        updatedAt: new Date()
      }).onConflictDoUpdate({
        target: [monthlySummaries.yearMonth],
        set: {
          totalCurtailedEnergy: monthlyTotals[0].totalCurtailedEnergy,
          totalPayment: monthlyTotals[0].totalPayment,
          updatedAt: new Date()
        }
      });
      
      log(`Monthly summary for ${yearMonth} updated successfully`, 'success');
    }
    
    // Update yearly summary
    const yearlyTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(${dailySummaries.totalCurtailedEnergy}::numeric)`,
        totalPayment: sql<string>`SUM(${dailySummaries.totalPayment}::numeric)`
      })
      .from(dailySummaries)
      .where(sql`date_trunc('year', ${dailySummaries.summaryDate}::date) = date_trunc('year', ${year + '-01-01'}::date)`);

    if (!yearlyTotals[0]?.totalCurtailedEnergy) {
      log(`No daily summaries found for ${year}, skipping yearly update`, 'warning');
    } else {
      await db.insert(yearlySummaries).values({
        year,
        totalCurtailedEnergy: yearlyTotals[0].totalCurtailedEnergy,
        totalPayment: yearlyTotals[0].totalPayment,
        updatedAt: new Date()
      }).onConflictDoUpdate({
        target: [yearlySummaries.year],
        set: {
          totalCurtailedEnergy: yearlyTotals[0].totalCurtailedEnergy,
          totalPayment: yearlyTotals[0].totalPayment,
          updatedAt: new Date()
        }
      });
      
      log(`Yearly summary for ${year} updated successfully`, 'success');
    }
    
    log(`All summaries updated successfully`, 'success');
  } catch (error) {
    log(`Error updating summaries: ${error}`, 'error');
    throw error;
  }
}

// Main function
async function main() {
  try {
    log(`Starting process to fetch missing data for ${DATE_TO_FIX}`, 'info');
    
    // Find missing lead parties
    const missingLeadParties = await findMissingLeadParties();
    
    if (missingLeadParties.size === 0) {
      log(`No missing lead parties identified. Looking for other data issues...`, 'info');
    }
    
    // Fetch Elexon data
    const elexonData = await fetchElexonData(DATE_TO_FIX);
    
    // Process the data
    const insertedCount = await processElexonData(elexonData, missingLeadParties);
    
    if (insertedCount > 0) {
      // Update summaries
      await updateSummaries();
      
      log(`Successfully added ${insertedCount} missing records and updated summaries`, 'success');
    } else {
      log(`No new records added. The discrepancy might be due to other factors.`, 'warning');
    }
    
    log(`Process completed successfully`, 'success');
  } catch (error) {
    log(`Error in main process: ${error}`, 'error');
  } finally {
    process.exit(0);
  }
}

// Run the script
main();