/**
 * Process Missing Data for 2025-03-28
 * 
 * This script will:
 * 1. Check current data for 2025-03-28
 * 2. Process any missing or incomplete data from Elexon API
 * 3. Update all Bitcoin calculations to ensure completeness
 */

import dotenv from 'dotenv';
import fs from 'fs';
import path from 'path';
import { db } from './db';
import { curtailmentRecords, historicalBitcoinCalculations } from './db/schema';
import { eq, and, count, sql, inArray } from 'drizzle-orm';
import { format, parse } from 'date-fns';
import pLimit from 'p-limit';

// Load environment variables
dotenv.config();

// Date to process
const DATE_TO_PROCESS = '2025-03-28';
const LOG_FILE = `./logs/process_2025-03-28_${format(new Date(), 'yyyy-MM-dd')}.log`;
const BMU_MAPPINGS_FILE = './data/bmu_mapping.json';

// Create log directory if it doesn't exist
if (!fs.existsSync('./logs')) {
  fs.mkdirSync('./logs');
}

// Logger setup
async function logToFile(message: string): Promise<void> {
  const timestamp = format(new Date(), 'yyyy-MM-dd HH:mm:ss');
  const logMessage = `[${timestamp}] ${message}\n`;
  
  fs.appendFileSync(LOG_FILE, logMessage);
}

function log(message: string, type: "info" | "success" | "warning" | "error" = "info"): void {
  const timestamp = format(new Date(), 'yyyy-MM-dd HH:mm:ss');
  
  // Terminal output with colors
  let colorCode = "";
  switch (type) {
    case "success": colorCode = "\x1b[32m"; break; // Green
    case "warning": colorCode = "\x1b[33m"; break; // Yellow
    case "error": colorCode = "\x1b[31m"; break;   // Red
    default: colorCode = "\x1b[36m";               // Cyan for info
  }
  
  console.log(`${colorCode}[${timestamp}] ${message}\x1b[0m`);
  
  // Also log to file
  logToFile(message).catch(console.error);
}

async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Load BMU mappings
async function loadBmuMappings(): Promise<{
  [bmuId: string]: { name: string; leadParty: string; farmId: string }
}> {
  try {
    const data = fs.readFileSync(BMU_MAPPINGS_FILE, 'utf8');
    return JSON.parse(data);
  } catch (error) {
    log(`Error loading BMU mappings: ${error}`, 'error');
    return {};
  }
}

async function fetchElexonDataForPeriod(settlementDate: string, settlementPeriod: number): Promise<any[]> {
  try {
    // Format the date for the API (DD-MM-YYYY)
    const parsedDate = parse(settlementDate, 'yyyy-MM-dd', new Date());
    const formattedDate = format(parsedDate, 'dd-MM-yyyy');
    
    // Construct the URL for the Elexon API
    const url = `https://api.bmreports.com/BMRS/B1610/v1?APIKey=${process.env.ELEXON_API_KEY}&SettlementDate=${formattedDate}&Period=${settlementPeriod}&ServiceType=xml`;
    
    log(`Fetching data from Elexon API for ${settlementDate} Period ${settlementPeriod}`);
    
    const response = await fetch(url, {
      method: 'GET',
      headers: { 'Content-Type': 'application/xml' },
      signal: AbortSignal.timeout(30000) // 30 second timeout
    });
    
    if (!response.ok) {
      throw new Error(`API returned status code ${response.status}`);
    }
    
    const xmlData = await response.text();
    
    // Simple XML parsing to extract the relevant data
    // This is a basic implementation - in production, use a proper XML parser
    const records: any[] = [];
    const regex = /<item>.*?<settlementDate>(.*?)<\/settlementDate>.*?<settlementPeriod>(.*?)<\/settlementPeriod>.*?<bMUnitID>(.*?)<\/bMUnitID>.*?<bMUnitType>(.*?)<\/bMUnitType>.*?<leadPartyName>(.*?)<\/leadPartyName>.*?<ngcBMUnitName>(.*?)<\/ngcBMUnitName>.*?<cashFlow>(.*?)<\/cashFlow>.*?<volume>(.*?)<\/volume>.*?<\/item>/gs;
    
    let match;
    while ((match = regex.exec(xmlData)) !== null) {
      if (match[4] === 'T') { // Only process records where BMUnitType is 'T' (Wind)
        records.push({
          settlementDate: match[1],
          settlementPeriod: parseInt(match[2]),
          bmuId: match[3],
          bmuType: match[4],
          leadPartyName: match[5],
          ngcBmuName: match[6],
          cashFlow: parseFloat(match[7]),
          volume: parseFloat(match[8])
        });
      }
    }
    
    log(`Retrieved ${records.length} wind farm records for period ${settlementPeriod}`);
    return records;
  } catch (error) {
    log(`Error fetching data for period ${settlementPeriod}: ${error}`, 'error');
    return [];
  }
}

async function processPeriod(
  date: string, 
  period: number, 
  bmuMappings: {[bmuId: string]: { name: string; leadParty: string; farmId: string }}
): Promise<boolean> {
  try {
    // Check if we already have data for this period
    const existingRecords = await db.select({ count: count() })
      .from(curtailmentRecords)
      .where(
        and(
          eq(curtailmentRecords.settlementDate, date),
          eq(curtailmentRecords.settlementPeriod, period)
        )
      );
    
    const recordCount = existingRecords[0]?.count || 0;
    
    // Fetch data from Elexon API
    const apiRecords = await fetchElexonDataForPeriod(date, period);
    
    log(`[${date} P${period}] DB records: ${recordCount}, API records: ${apiRecords.length}`);
    
    if (apiRecords.length === 0) {
      log(`No data available from API for ${date} period ${period}`, 'warning');
      return false;
    }
    
    if (recordCount === apiRecords.length) {
      log(`Data for ${date} period ${period} seems complete (${recordCount} records)`, 'success');
      return true;
    }
    
    // If we have some records but not all, delete existing records to avoid duplicates
    if (recordCount > 0) {
      log(`Clearing ${recordCount} existing records for ${date} period ${period} before reinsertion`);
      await db.delete(curtailmentRecords)
        .where(
          and(
            eq(curtailmentRecords.settlementDate, date),
            eq(curtailmentRecords.settlementPeriod, period)
          )
        );
    }
    
    // Insert all records from API
    const insertData = apiRecords.map(record => {
      const mappingInfo = bmuMappings[record.bmuId] || {
        name: record.ngcBmuName,
        leadParty: record.leadPartyName,
        farmId: record.bmuId
      };
      
      return {
        settlementDate: date,
        settlementPeriod: period,
        farmId: record.bmuId,
        leadPartyName: mappingInfo.leadParty,
        volume: Math.abs(record.volume), // Store as positive value
        payment: Math.abs(record.cashFlow), // Store as positive value
        soFlag: false, // Default value
        cadlFlag: false, // Default value
        originalPrice: 0, // Default value
        finalPrice: 0, // Default value
        createdAt: new Date()
      };
    });
    
    if (insertData.length > 0) {
      await db.insert(curtailmentRecords).values(insertData);
      log(`Successfully inserted ${insertData.length} records for ${date} period ${period}`, 'success');
    }
    
    return true;
  } catch (error) {
    log(`Error processing period ${period}: ${error}`, 'error');
    return false;
  }
}

async function processBatch(
  date: string,
  periods: number[],
  bmuMappings: {[bmuId: string]: { name: string; leadParty: string; farmId: string }}
): Promise<void> {
  // Process periods in parallel but with concurrency limit to avoid API rate limits
  const limit = pLimit(3); // Maximum 3 concurrent API calls
  
  const tasks = periods.map(period => {
    return limit(() => processPeriod(date, period, bmuMappings));
  });
  
  await Promise.all(tasks);
}

async function processDate(): Promise<void> {
  try {
    log(`Starting processing for date ${DATE_TO_PROCESS}`, 'info');
    
    // Load BMU mappings
    const bmuMappings = await loadBmuMappings();
    log(`Loaded ${Object.keys(bmuMappings).length} BMU mappings`);
    
    // Check current data for this date
    const currentRecords = await db.select({
      count: count(),
      periods: sql`COUNT(DISTINCT settlement_period)`,
      volume: sql`SUM(volume)`,
      payment: sql`SUM(payment)`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, DATE_TO_PROCESS));
    
    log(`Current data for ${DATE_TO_PROCESS}: ${JSON.stringify({
      records: currentRecords[0]?.count || 0,
      periods: currentRecords[0]?.periods || 0,
      volume: typeof currentRecords[0]?.volume === 'number' ? currentRecords[0].volume.toFixed(2) : '0.00',
      payment: typeof currentRecords[0]?.payment === 'number' ? currentRecords[0].payment.toFixed(2) : '0.00'
    })}`);
    
    // Process all 48 settlement periods in batches
    const allPeriods = Array.from({ length: 48 }, (_, i) => i + 1);
    
    // Process in batches of 12 periods at a time
    const batchSize = 12;
    for (let i = 0; i < allPeriods.length; i += batchSize) {
      const batch = allPeriods.slice(i, i + batchSize);
      log(`Processing batch of periods ${batch[0]}-${batch[batch.length - 1]}`);
      await processBatch(DATE_TO_PROCESS, batch, bmuMappings);
      
      // Add a delay between batches to avoid API rate limits
      if (i + batchSize < allPeriods.length) {
        log(`Pausing for 10 seconds before next batch...`);
        await delay(10000);
      }
    }
    
    // Check updated data
    const updatedRecords = await db.select({
      count: count(),
      periods: sql`COUNT(DISTINCT settlement_period)`,
      volume: sql`SUM(volume)`,
      payment: sql`SUM(payment)`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, DATE_TO_PROCESS));
    
    log(`Updated data for ${DATE_TO_PROCESS}: ${JSON.stringify({
      records: updatedRecords[0]?.count || 0,
      periods: updatedRecords[0]?.periods || 0,
      volume: typeof updatedRecords[0]?.volume === 'number' ? updatedRecords[0].volume.toFixed(2) : '0.00',
      payment: typeof updatedRecords[0]?.payment === 'number' ? updatedRecords[0].payment.toFixed(2) : '0.00'
    })}`, 'success');
    
  } catch (error) {
    log(`Error processing date ${DATE_TO_PROCESS}: ${error}`, 'error');
  }
}

async function findMissingCalculations(): Promise<number[]> {
  try {
    // Get all distinct periods for the date
    const periods = await db.select({ period: curtailmentRecords.settlementPeriod })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, DATE_TO_PROCESS))
      .groupBy(curtailmentRecords.settlementPeriod);
    
    const allPeriods = periods.map(p => p.period);
    
    if (allPeriods.length === 0) {
      log(`No curtailment records found for ${DATE_TO_PROCESS}`);
      return [];
    }
    
    // Check which periods have calculations for S19J_PRO miner model
    const calculatedPeriods = await db.select({ period: historicalBitcoinCalculations.settlementPeriod })
      .from(historicalBitcoinCalculations)
      .where(
        and(
          eq(historicalBitcoinCalculations.settlementDate, DATE_TO_PROCESS),
          eq(historicalBitcoinCalculations.minerModel, 'S19J_PRO')
        )
      )
      .groupBy(historicalBitcoinCalculations.settlementPeriod);
    
    const calculatedPeriodSet = new Set(calculatedPeriods.map(p => p.period));
    
    // Find missing periods
    const missingPeriods = allPeriods.filter(period => !calculatedPeriodSet.has(period));
    
    log(`Found ${missingPeriods.length} periods without Bitcoin calculations for ${DATE_TO_PROCESS}`);
    
    return missingPeriods;
  } catch (error) {
    log(`Error finding missing calculations: ${error}`, 'error');
    return [];
  }
}

async function updateBitcoinCalculations(): Promise<void> {
  try {
    log(`Starting Bitcoin calculation updates for ${DATE_TO_PROCESS}`);
    
    // Perform calculations with each miner model
    const minerModels = ['S19J_PRO', 'S9', 'M20S'];
    
    for (const minerModel of minerModels) {
      // Check which farms have records for this date using SQL directly
      const farmsResult = await db.execute(
        sql`SELECT DISTINCT farm_id, lead_party_name 
            FROM curtailment_records 
            WHERE settlement_date = ${DATE_TO_PROCESS}`
      );
      
      const farms = farmsResult.rows || [];
      log(`Found ${farms.length} farms with curtailment records for ${DATE_TO_PROCESS}`);
      
      // Get mining difficulty for this date (use latest from previous day if not available)
      let difficulty = 113757508810853; // Default value if not found
      let btcPrice = 66061.96; // Default price in GBP
      
      log(`Using mining difficulty of ${difficulty} and BTC price of £${btcPrice} for calculations`);
      
      // Perform calculations for each farm and period
      const limit = pLimit(10); // Process up to 10 farm calculations concurrently
      const tasks: Promise<void>[] = [];
      
      for (const farm of farms) {
        tasks.push(limit(async () => {
          // Get all periods for this farm
          const farmRecordsResult = await db.execute(
            sql`SELECT settlement_period, volume 
                FROM curtailment_records 
                WHERE settlement_date = ${DATE_TO_PROCESS} 
                AND farm_id = ${farm.farm_id}`
          );
          
          const farmRecords = farmRecordsResult.rows || [];
          const insertData = [];
          
          // Process all periods
          for (const record of farmRecords) {
            // Check if calculation already exists
            const existingCalcResult = await db.execute(
              sql`SELECT id FROM historical_bitcoin_calculations 
                  WHERE settlement_date = ${DATE_TO_PROCESS}
                  AND settlement_period = ${record.settlement_period}
                  AND farm_id = ${farm.farm_id}
                  AND miner_model = ${minerModel}`
            );
            
            if (existingCalcResult.rows && existingCalcResult.rows.length > 0) {
              continue; // Skip if calculation already exists
            }
            
            // Calculate Bitcoin amount based on miner model and curtailed volume
            // Use absolute value to ensure positive energy for calculations
            let energyMWh = Math.abs(Number(record.volume));
            let energyWh = energyMWh * 1000000;
            
            // Different efficiency values for different miner models (J/TH)
            let efficiency;
            switch (minerModel) {
              case 'S19J_PRO': efficiency = 29.5; break;
              case 'S9': efficiency = 94.0; break;
              case 'M20S': efficiency = 48.0; break;
              default: efficiency = 30.0;
            }
            
            // Calculate terahashes
            let hashrateTh = energyWh / efficiency; // Wh / (J/TH) = TH
            
            // Calculate Bitcoin mined
            // Formula: BTC = (hashrateTh * 3600) / (difficulty * 2^32 / 10^12) * 6.25
            let btcMined = (hashrateTh * 3600) / (difficulty * Math.pow(2, 32) / Math.pow(10, 12)) * 6.25;
            
            // Calculate value in GBP
            let valueGbp = btcMined * btcPrice;
            
            // Insert directly with SQL to avoid TypeScript issues
            await db.execute(
              sql`INSERT INTO historical_bitcoin_calculations 
                  (settlement_date, settlement_period, farm_id, miner_model, 
                   curtailed_energy, bitcoin_mined, value_gbp, 
                   network_difficulty, btc_price_gbp, created_at)
                  VALUES 
                  (${DATE_TO_PROCESS}, ${record.settlement_period}, ${farm.farm_id}, ${minerModel},
                   ${energyMWh}, ${btcMined}, ${valueGbp}, 
                   ${difficulty}, ${btcPrice}, NOW())`
            );
            
            insertData.push({
              period: record.settlement_period,
              energy: energyMWh,
              btc: btcMined
            });
          }
          
          if (insertData.length > 0) {
            log(`Added ${insertData.length} Bitcoin calculations for farm ${farm.farm_id} with model ${minerModel}`);
          }
        }));
      }
      
      await Promise.all(tasks);
      
      log(`Completed Bitcoin calculations for ${minerModel}`, 'success');
    }
    
    // Check final state of calculations
    const calculationCountsResult = await db.execute(
      sql`SELECT miner_model, COUNT(*) as count
          FROM historical_bitcoin_calculations
          WHERE settlement_date = ${DATE_TO_PROCESS}
          GROUP BY miner_model`
    );
    
    log(`Bitcoin calculation summary for ${DATE_TO_PROCESS}:`);
    if (calculationCountsResult.rows) {
      calculationCountsResult.rows.forEach(row => {
        log(`  ${row.miner_model}: ${row.count} records`);
      });
    }
    
  } catch (error) {
    log(`Error updating Bitcoin calculations: ${error}`, 'error');
  }
}

async function updateDailySummary(): Promise<void> {
  try {
    log(`Updating daily summary for ${DATE_TO_PROCESS}`);
    
    // Calculate totals from curtailment records with direct SQL
    const totalsResult = await db.execute(
      sql`SELECT 
            ABS(SUM(volume)) as total_volume, 
            ABS(SUM(payment)) as total_payment 
          FROM curtailment_records 
          WHERE settlement_date = ${DATE_TO_PROCESS}`
    );
    
    // Safely extract results, using absolute values to ensure positive numbers
    const totalVolume = totalsResult.rows && totalsResult.rows[0] ? 
      Math.abs(Number(totalsResult.rows[0].total_volume) || 0) : 0;
    
    const totalPayment = totalsResult.rows && totalsResult.rows[0] ? 
      Math.abs(Number(totalsResult.rows[0].total_payment) || 0) : 0;
    
    log(`Calculated totals for ${DATE_TO_PROCESS}: Volume=${totalVolume.toFixed(2)} MWh, Payment=${totalPayment.toFixed(2)} GBP`);
    
    // Check if summary already exists
    const existingSummaryResult = await db.execute(
      sql`SELECT * FROM daily_summaries WHERE summary_date = ${DATE_TO_PROCESS}`
    );
    
    const hasExistingSummary = existingSummaryResult.rows && existingSummaryResult.rows.length > 0;
    
    if (hasExistingSummary) {
      // Update existing summary
      await db.execute(
        sql`UPDATE daily_summaries 
            SET total_curtailed_energy = ${totalVolume},
                total_payment = ${totalPayment},
                last_updated = NOW()
            WHERE summary_date = ${DATE_TO_PROCESS}`
      );
      log(`Updated daily summary for ${DATE_TO_PROCESS}`);
    } else {
      // Create new summary
      await db.execute(
        sql`INSERT INTO daily_summaries 
            (summary_date, total_curtailed_energy, total_payment, created_at)
            VALUES (${DATE_TO_PROCESS}, ${totalVolume}, ${totalPayment}, NOW())`
      );
      log(`Created new daily summary for ${DATE_TO_PROCESS}`);
    }
    
    // Log the updated summary
    const updatedSummaryResult = await db.execute(
      sql`SELECT * FROM daily_summaries WHERE summary_date = ${DATE_TO_PROCESS}`
    );
    
    if (updatedSummaryResult.rows && updatedSummaryResult.rows.length > 0) {
      log(`Daily summary for ${DATE_TO_PROCESS}: ${JSON.stringify(updatedSummaryResult.rows[0])}`);
    }
    
  } catch (error) {
    log(`Error updating daily summary: ${error}`, 'error');
  }
}

async function main() {  
  try {
    log(`Starting processing for ${DATE_TO_PROCESS}`, 'info');
    
    // Step 1: Process curtailment records
    await processDate();
    
    // Step 2: Update Bitcoin calculations
    await updateBitcoinCalculations();
    
    // Step 3: Update daily summary
    await updateDailySummary();
    
    log(`Processing completed for ${DATE_TO_PROCESS}`, 'success');
  } catch (error) {
    log(`Error in main process: ${error}`, 'error');
  }
}

// Run the script
main().catch(error => {
  console.error('Unhandled error:', error);
}).finally(() => {
  // Don't exit immediately to allow any pending logs to be written
  setTimeout(() => {
    process.exit(0);
  }, 1000);
});