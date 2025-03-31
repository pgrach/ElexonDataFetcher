/**
 * Ingest Missing Periods for 2025-03-29
 * 
 * This script is designed to fetch and process the missing settlement periods 43 and 44
 * for March 29, 2025 (21:00-22:00 hour), which are missing from the database.
 * 
 * Usage:
 *   npx tsx ingest_missing_periods_2025-03-29.ts
 */

import { db } from "./db";
import { curtailmentRecords } from "./db/schema";
import fs from "fs";
import path from "path";
import axios from "axios";
import { eq, and, sql } from "drizzle-orm";
import { InsertCurtailmentRecord } from "./db/schema";

// Constants
const TARGET_DATE = "2025-03-29";
const MISSING_PERIODS = [43, 44];
const LOG_FILE = "ingest_missing_periods_2025-03-29.log";
const API_BASE_URL = "https://api.bmreports.com/BMRS/B1630/v1";

// API parameters
const API_PARAMS = {
  APIKey: process.env.ELEXON_API_KEY || "d3k23h9p7jxwq1l",
  SettlementDate: TARGET_DATE,
  ServiceType: "xml"
};

// Create log directory if it doesn't exist
const logDir = path.join(process.cwd(), "logs");
if (!fs.existsSync(logDir)) {
  fs.mkdirSync(logDir, { recursive: true });
}

// BMU to farm mapping
interface BmuMapping {
  BMU_ID: string;
  FARM_ID: string;
  LEAD_PARTY_NAME: string;
  [key: string]: string;
}

// Logging utility
async function log(message: string, level: "info" | "error" | "warning" | "success" = "info"): Promise<void> {
  const timestamp = new Date().toISOString();
  const formattedMessage = `[${timestamp}] [${level.toUpperCase()}] ${message}`;
  
  // Log to console with color
  const colors = {
    info: "\x1b[36m", // Cyan
    error: "\x1b[31m", // Red
    warning: "\x1b[33m", // Yellow
    success: "\x1b[32m" // Green
  };
  console.log(`${colors[level]}${formattedMessage}\x1b[0m`);
  
  // Log to file
  const logPath = path.join(logDir, LOG_FILE);
  fs.appendFileSync(logPath, formattedMessage + "\n");
}

// Delay utility
async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Load BMU mappings from JSON file
async function loadBmuMappings(): Promise<Map<string, BmuMapping>> {
  try {
    const data = fs.readFileSync("./data/bmu_mapping.json", "utf8");
    const bmuData: BmuMapping[] = JSON.parse(data);
    
    // Create a map for faster lookups
    const bmuMap = new Map<string, BmuMapping>();
    for (const bmu of bmuData) {
      bmuMap.set(bmu.BMU_ID, bmu);
    }
    
    await log(`Loaded ${bmuMap.size} BMU mappings from file`, "success");
    return bmuMap;
  } catch (error) {
    await log(`Error loading BMU mappings: ${error}`, "error");
    throw error;
  }
}

// Parse XML response from Elexon API
function parseXmlResponse(xmlData: string): any[] {
  try {
    // Extract data points using regex for simplicity
    // In a production environment, use a proper XML parser
    const dataItems: any[] = [];
    const regex = /<item>([\s\S]*?)<\/item>/g;
    let match;
    
    while ((match = regex.exec(xmlData)) !== null) {
      const itemContent = match[1];
      const record: any = {};
      
      // Extract each field
      const fields = [
        { name: "settlementDate", regex: /<settlementDate>(.*?)<\/settlementDate>/},
        { name: "settlementPeriod", regex: /<settlementPeriod>(.*?)<\/settlementPeriod>/},
        { name: "bmuId", regex: /<bmuId>(.*?)<\/bmuId>/},
        { name: "volumeQty", regex: /<volumeQty>(.*?)<\/volumeQty>/},
        { name: "cashflowQty", regex: /<cashflowQty>(.*?)<\/cashflowQty>/},
        { name: "soFlag", regex: /<soFlag>(.*?)<\/soFlag>/},
        { name: "cadlFlag", regex: /<cadlFlag>(.*?)<\/cadlFlag>/},
        { name: "priceCost", regex: /<priceCost>(.*?)<\/priceCost>/},
        { name: "acceptanceNumber", regex: /<acceptanceNumber>(.*?)<\/acceptanceNumber>/},
        { name: "amendmentFlag", regex: /<amendmentFlag>(.*?)<\/amendmentFlag>/},
        { name: "amendmentNumber", regex: /<amendmentNumber>(.*?)<\/amendmentNumber>/},
        { name: "storageFlag", regex: /<storageFlag>(.*?)<\/storageFlag>/}
      ];
      
      fields.forEach(field => {
        const fieldMatch = field.regex.exec(itemContent);
        if (fieldMatch) {
          // Convert to proper type
          if (field.name === "settlementPeriod") {
            record[field.name] = parseInt(fieldMatch[1]);
          } else if (field.name === "volumeQty" || field.name === "cashflowQty" || field.name === "priceCost") {
            record[field.name] = parseFloat(fieldMatch[1]);
          } else {
            record[field.name] = fieldMatch[1];
          }
        }
      });
      
      dataItems.push(record);
    }
    
    return dataItems;
  } catch (error) {
    throw new Error(`Error parsing XML: ${error}`);
  }
}

// Process data and save to database
async function processData(data: any[], bmuMap: Map<string, BmuMapping>): Promise<void> {
  const recordsToInsert: InsertCurtailmentRecord[] = [];
  
  for (const item of data) {
    // Skip records that aren't for our target periods
    if (!MISSING_PERIODS.includes(item.settlementPeriod)) {
      continue;
    }
    
    // Look up BMU mapping
    const bmuMapping = bmuMap.get(item.bmuId);
    if (!bmuMapping) {
      await log(`No mapping found for BMU ID: ${item.bmuId}`, "warning");
      continue;
    }
    
    // Only interested in curtailment records with negative volume
    if (item.volumeQty >= 0) {
      continue;
    }
    
    // Create record
    recordsToInsert.push({
      settlementDate: item.settlementDate, // This is already a properly formatted date string
      settlementPeriod: item.settlementPeriod,
      farmId: bmuMapping.FARM_ID,
      leadPartyName: bmuMapping.LEAD_PARTY_NAME,
      volume: item.volumeQty.toString(),
      payment: Math.abs(item.cashflowQty).toString(),
      originalPrice: item.priceCost.toString(),
      finalPrice: item.priceCost.toString(),
      soFlag: item.soFlag === "Y",
      cadlFlag: item.cadlFlag === "Y",
      createdAt: new Date()
    });
  }
  
  if (recordsToInsert.length === 0) {
    await log(`No valid curtailment records found for periods ${MISSING_PERIODS.join(", ")}`, "warning");
    return;
  }
  
  try {
    // Insert records in batches
    await db.insert(curtailmentRecords).values(recordsToInsert);
    await log(`Successfully inserted ${recordsToInsert.length} curtailment records`, "success");
    
    // Log a breakdown of the inserted records
    const periodGroups = new Map<number, { count: number, volume: number, payment: number }>();
    
    for (const record of recordsToInsert) {
      if (!periodGroups.has(record.settlementPeriod)) {
        periodGroups.set(record.settlementPeriod, { count: 0, volume: 0, payment: 0 });
      }
      
      const group = periodGroups.get(record.settlementPeriod)!;
      group.count++;
      group.volume += Math.abs(parseFloat(record.volume));
      group.payment += parseFloat(record.payment);
    }
    
    for (const [period, stats] of periodGroups.entries()) {
      await log(`Period ${period}: ${stats.count} records, ${stats.volume.toFixed(2)} MWh, £${stats.payment.toFixed(2)} payment`, "info");
    }
  } catch (error) {
    await log(`Error inserting records: ${error}`, "error");
    throw error;
  }
}

// Fetch data from Elexon API
async function fetchElexonData(): Promise<any[]> {
  try {
    await log(`Fetching data from Elexon API for date ${TARGET_DATE}`, "info");
    
    const response = await axios.get(API_BASE_URL, { params: API_PARAMS });
    
    if (response.status !== 200) {
      throw new Error(`API returned status ${response.status}: ${response.statusText}`);
    }
    
    const xmlData = response.data;
    await log(`Successfully fetched data from Elexon API`, "success");
    
    // Parse XML response
    const parsedData = parseXmlResponse(xmlData);
    await log(`Parsed ${parsedData.length} records from API response`, "info");
    
    return parsedData;
  } catch (error) {
    await log(`Error fetching data from API: ${error}`, "error");
    throw error;
  }
}

// Main function
async function main(): Promise<void> {
  try {
    await log(`\n=== Starting Ingest Process for Missing Periods on ${TARGET_DATE} ===\n`, "info");
    
    // Check if records already exist
    const existingRecords = await db
      .select({ count: sql<number>`COUNT(*)` })
      .from(curtailmentRecords)
      .where(
        and(
          eq(curtailmentRecords.settlementDate, TARGET_DATE),
          eq(curtailmentRecords.settlementPeriod, MISSING_PERIODS[0])
        )
      );
    
    if (existingRecords[0]?.count > 0) {
      await log(`Records already exist for period ${MISSING_PERIODS[0]} on ${TARGET_DATE}. Cleaning up before reinserting.`, "warning");
      
      // Delete existing records for these periods
      for (const period of MISSING_PERIODS) {
        await db
          .delete(curtailmentRecords)
          .where(
            and(
              eq(curtailmentRecords.settlementDate, TARGET_DATE),
              eq(curtailmentRecords.settlementPeriod, period)
            )
          );
      }
      
      await log(`Deleted existing records for periods ${MISSING_PERIODS.join(", ")}`, "info");
    }
    
    // Load BMU mappings
    const bmuMap = await loadBmuMappings();
    
    // Fetch data from Elexon API
    const data = await fetchElexonData();
    
    // Process data and save to database
    await processData(data, bmuMap);
    
    // Verify the data was inserted correctly
    const insertedRecords = await db
      .select({
        period: curtailmentRecords.settlementPeriod,
        count: sql<number>`COUNT(*)`,
        totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
        totalPayment: sql<string>`SUM(payment::numeric)`
      })
      .from(curtailmentRecords)
      .where(
        and(
          eq(curtailmentRecords.settlementDate, TARGET_DATE),
          sql`settlement_period IN (${MISSING_PERIODS.join(',')})`
        )
      )
      .groupBy(curtailmentRecords.settlementPeriod);
    
    if (insertedRecords.length > 0) {
      await log(`\nVerification Results:`, "success");
      
      let totalVolume = 0;
      let totalPayment = 0;
      
      for (const record of insertedRecords) {
        await log(`Period ${record.period}: ${record.count} records, ${Number(record.totalVolume).toFixed(2)} MWh, £${Number(record.totalPayment).toFixed(2)} payment`, "success");
        totalVolume += Number(record.totalVolume);
        totalPayment += Number(record.totalPayment);
      }
      
      await log(`\nTotal for hour 21: ${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)} payment`, "success");
    } else {
      await log(`No records found after insertion. Something went wrong.`, "error");
    }
    
    await log(`\n=== Completed Ingest Process for Missing Periods on ${TARGET_DATE} ===\n`, "success");
  } catch (error) {
    await log(`Unhandled error: ${error}`, "error");
    process.exit(1);
  }
}

// Run the script
main()
  .then(() => {
    console.log(`\nProcess completed. See ${LOG_FILE} for details.`);
    process.exit(0);
  })
  .catch(error => {
    console.error(`\nFatal error: ${error}`);
    process.exit(1);
  });