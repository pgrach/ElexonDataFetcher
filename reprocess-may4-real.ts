/**
 * May 4th Data Reprocessing Script
 * 
 * This script reprocesses all data for May 4th, 2025 using direct 
 * API calls to Elexon to fetch authentic pricing and curtailment data.
 * 
 * Usage:
 *   npx tsx reprocess-may4-real.ts
 */

import axios from 'axios';
import { db } from './db';
import { 
  curtailmentRecords, 
  insertCurtailmentRecordSchema,
  historicalBitcoinCalculations,
  insertHistoricalBitcoinCalculationSchema,
  bitcoinDailySummaries,
  insertBitcoinDailySummarySchema
} from './db/schema';
import fs from 'fs';
import path from 'path';
import { format } from 'date-fns';
import { eq, and } from 'drizzle-orm';

// Create a logger instance
import { createLogger, format as logFormat, transports } from 'winston';

const logger = createLogger({
  level: 'info',
  format: logFormat.combine(
    logFormat.timestamp(),
    logFormat.printf(({ level, message, timestamp }) => {
      return `[${timestamp}] [${level.toUpperCase()}] ${message}`;
    })
  ),
  transports: [
    new transports.Console(),
    new transports.File({ 
      filename: `reprocess-may4-real-${format(new Date(), 'yyyy-MM-dd')}.log` 
    })
  ]
});

// Path to BMU mapping
const BMU_MAPPING_PATH = './data/bmu_mapping.json';

// Constants
const TARGET_DATE = '2025-05-04';
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];
const DIFFICULTY = 119116256505723; // Use the current difficulty value

// Bitcoin miner efficiency (J/TH)
const MINER_EFFICIENCY = {
  'S19J_PRO': 27.5,
  'S9': 97.0,
  'M20S': 52.0,
};

/**
 * Get unified wind farm IDs from mapping file
 */
async function getWindFarmIds(): Promise<Set<string>> {
  try {
    const data = fs.readFileSync(BMU_MAPPING_PATH, 'utf8');
    const mapping = JSON.parse(data);
    const windFarmIds = new Set<string>();
    
    for (const entry of mapping) {
      if (entry.bmuId) {
        windFarmIds.add(entry.bmuId);
      }
    }
    
    logger.info(`Loaded ${windFarmIds.size} wind farm BMU IDs from mapping file`);
    return windFarmIds;
  } catch (error) {
    logger.error(`Error loading wind farm IDs: ${error}`);
    throw error;
  }
}

/**
 * Fetch data from Elexon API with retries
 */
async function fetchFromElexon(url: string): Promise<any> {
  const MAX_RETRIES = 3;
  let retries = 0;
  
  while (retries < MAX_RETRIES) {
    try {
      logger.info(`Fetching from Elexon API: ${url}`);
      const response = await axios.get(url, { timeout: 10000 });
      if (response.status === 200 && response.data) {
        return response.data;
      }
      throw new Error(`Failed to get data, status: ${response.status}`);
    } catch (error) {
      retries++;
      logger.error(`Attempt ${retries}/${MAX_RETRIES} failed: ${error}`);
      if (retries === MAX_RETRIES) {
        throw error;
      }
      // Exponential backoff
      await new Promise(resolve => setTimeout(resolve, 1000 * retries * retries));
    }
  }
}

/**
 * Fetch bids and offers data for a specific period
 */
async function fetchBidsOffers(date: string, period: number): Promise<any[]> {
  try {
    // Format the date for Elexon API (YYYY-MM-DD)
    const formattedDate = date;
    
    // Construct the API URL
    const url = `https://api.bmreports.com/BMRS/DISBSAD/v1?APIKey=live&SettlementDate=${formattedDate}&Period=${period}&ServiceType=xml`;
    
    const response = await fetchFromElexon(url);
    
    // Parse the XML response
    if (typeof response === 'string') {
      logger.error('Received string response instead of parsed XML');
      return [];
    }
    
    // Extract the relevant data from the response
    const items = response?.response?.responseBody?.responseList?.item || [];
    return Array.isArray(items) ? items : [items];
  } catch (error) {
    logger.error(`Error fetching bids and offers for period ${period}: ${error}`);
    return [];
  }
}

/**
 * Process curtailment data for May 4th, 2025
 */
async function processCurtailmentData(): Promise<number> {
  let insertedRecords = 0;
  
  try {
    // Get the set of wind farm IDs
    const windFarmIds = await getWindFarmIds();
    
    // Clear any existing records for the date
    const deleted = await db.delete(curtailmentRecords)
      .where(eq(curtailmentRecords.settlement_date, TARGET_DATE))
      .execute();
    
    logger.info(`Deleted ${deleted} existing curtailment records for ${TARGET_DATE}`);
    
    // Process each settlement period
    for (let period = 1; period <= 48; period++) {
      logger.info(`Processing period ${period}...`);
      
      // Fetch bids and offers data
      const records = await fetchBidsOffers(TARGET_DATE, period);
      
      if (!records || records.length === 0) {
        logger.warn(`No data found for period ${period}`);
        continue;
      }
      
      logger.info(`Retrieved ${records.length} records for period ${period}`);
      
      // Filter and process the records
      const curtailmentRecordsToInsert = [];
      
      for (const record of records) {
        // Check if the record is from a wind farm
        if (!windFarmIds.has(record.bMUnitID)) {
          continue;
        }
        
        // Check for curtailment criteria: negative volume AND (soFlag OR cadlFlag)
        const volume = parseFloat(record.acceptedVolume || 0);
        if (volume >= 0) {
          continue; // Skip if not negative volume
        }
        
        const soFlag = record.sOFlag === 'Y';
        const cadlFlag = record.cADLFlag === 'Y';
        
        if (!soFlag && !cadlFlag) {
          continue; // Skip if neither flag is set
        }
        
        // Get the price
        const price = parseFloat(record.acceptedPrice || 0);
        
        // Calculate the payment: volume * price
        // Note: volume is negative, so payment might be negative if price is positive
        // For curtailment, we need to ensure payment is a positive value
        const payment = Math.abs(volume) * Math.abs(price);
        
        // Prepare the record for insertion
        curtailmentRecordsToInsert.push({
          settlement_date: TARGET_DATE,
          settlement_period: period,
          farm_id: record.bMUnitID,
          volume: Math.abs(volume), // Store as positive
          payment: payment,
          original_price: price,
          final_price: price,
          so_flag: soFlag,
          cadl_flag: cadlFlag,
          created_at: new Date(),
          lead_party_name: record.leadParty || 'Unknown'
        });
      }
      
      // Insert the curtailment records
      if (curtailmentRecordsToInsert.length > 0) {
        const inserted = await db.insert(curtailmentRecords)
          .values(curtailmentRecordsToInsert)
          .execute();
        
        insertedRecords += curtailmentRecordsToInsert.length;
        logger.info(`Inserted ${curtailmentRecordsToInsert.length} curtailment records for period ${period}`);
      }
    }
    
    logger.info(`Total curtailment records inserted: ${insertedRecords}`);
    return insertedRecords;
  } catch (error) {
    logger.error(`Error processing curtailment data: ${error}`);
    throw error;
  }
}

/**
 * Calculate Bitcoin mining for a specific curtailment record
 */
async function calculateBitcoinMining(recordId: number, minerModel: string): Promise<void> {
  try {
    // Get the curtailment record
    const record = await db.query.curtailmentRecords.findFirst({
      where: eq(curtailmentRecords.id, recordId)
    });
    
    if (!record) {
      logger.error(`Curtailment record ${recordId} not found`);
      return;
    }
    
    // Convert MWh to kWh
    const energyKwh = record.volume * 1000;
    
    // Get miner efficiency
    const efficiency = MINER_EFFICIENCY[minerModel];
    if (!efficiency) {
      logger.error(`Unknown miner model: ${minerModel}`);
      return;
    }
    
    // Calculate hash rate (TH)
    const hashRate = energyKwh / efficiency;
    
    // Calculate expected Bitcoin (BTC)
    // Formula: hashRate * 6.25 * 30 minutes / (difficulty * 2^32 / 600)
    const expectedBitcoin = (hashRate * 6.25 * 30 * 600) / (DIFFICULTY * Math.pow(2, 32));
    
    // Insert the calculation
    await db.insert(historicalBitcoinCalculations)
      .values({
        settlement_date: record.settlement_date,
        settlement_period: record.settlement_period,
        farm_id: record.farm_id,
        miner_model: minerModel,
        bitcoin_mined: expectedBitcoin,
        difficulty: DIFFICULTY,
        calculated_at: new Date()
      })
      .execute();
    
    logger.info(`Calculated Bitcoin mining for record ${recordId}, miner ${minerModel}: ${expectedBitcoin} BTC`);
  } catch (error) {
    logger.error(`Error calculating Bitcoin mining for record ${recordId}: ${error}`);
  }
}

/**
 * Process Bitcoin calculations for all curtailment records
 */
async function processBitcoinCalculations(): Promise<void> {
  try {
    // Clear existing Bitcoin calculations for the date
    await db.delete(historicalBitcoinCalculations)
      .where(eq(historicalBitcoinCalculations.settlement_date, TARGET_DATE))
      .execute();
    
    logger.info(`Deleted existing Bitcoin calculations for ${TARGET_DATE}`);
    
    // Get all curtailment records for the date
    const records = await db.query.curtailmentRecords.findMany({
      where: eq(curtailmentRecords.settlement_date, TARGET_DATE)
    });
    
    logger.info(`Processing Bitcoin calculations for ${records.length} curtailment records`);
    
    // Process each record with each miner model
    for (const record of records) {
      for (const minerModel of MINER_MODELS) {
        await calculateBitcoinMining(record.id, minerModel);
      }
    }
    
    logger.info('Bitcoin calculations completed');
  } catch (error) {
    logger.error(`Error processing Bitcoin calculations: ${error}`);
    throw error;
  }
}

/**
 * Create daily summary for Bitcoin calculations
 */
async function createBitcoinDailySummary(): Promise<void> {
  try {
    // Delete existing summaries for the date
    await db.delete(bitcoinDailySummaries)
      .where(eq(bitcoinDailySummaries.summary_date, TARGET_DATE))
      .execute();
    
    logger.info(`Deleted existing Bitcoin daily summaries for ${TARGET_DATE}`);
    
    // Create summaries for each miner model
    for (const minerModel of MINER_MODELS) {
      // Get total Bitcoin mined for the day with this miner model
      const result = await db
        .select({
          totalBitcoin: db.fn.sum(historicalBitcoinCalculations.bitcoin_mined)
        })
        .from(historicalBitcoinCalculations)
        .where(
          and(
            eq(historicalBitcoinCalculations.settlement_date, TARGET_DATE),
            eq(historicalBitcoinCalculations.miner_model, minerModel)
          )
        )
        .execute();
      
      const totalBitcoin = result[0]?.totalBitcoin || 0;
      
      // Insert the summary
      await db.insert(bitcoinDailySummaries)
        .values({
          summary_date: TARGET_DATE,
          miner_model: minerModel,
          bitcoin_mined: totalBitcoin,
          created_at: new Date(),
          updated_at: new Date()
        })
        .execute();
      
      logger.info(`Created Bitcoin daily summary for ${TARGET_DATE} and miner ${minerModel}: ${totalBitcoin} BTC`);
    }
  } catch (error) {
    logger.error(`Error creating Bitcoin daily summaries: ${error}`);
    throw error;
  }
}

/**
 * Update daily summary for curtailment records
 */
async function updateDailySummary(): Promise<void> {
  try {
    // Calculate totals
    const curtailmentResult = await db
      .select({
        totalEnergy: db.fn.sum(curtailmentRecords.volume),
        totalPayment: db.fn.sum(curtailmentRecords.payment)
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlement_date, TARGET_DATE))
      .execute();
    
    const totalEnergy = curtailmentResult[0]?.totalEnergy || 0;
    const totalPayment = curtailmentResult[0]?.totalPayment || 0;
    
    // Get wind generation totals
    const windResult = await db.execute(
      `SELECT 
        SUM(total_wind) as total_wind_generation,
        SUM(wind_onshore) as wind_onshore_generation,
        SUM(wind_offshore) as wind_offshore_generation
      FROM wind_generation_data
      WHERE settlement_date = '${TARGET_DATE}'`
    );
    
    const totalWindGeneration = windResult.rows[0]?.total_wind_generation || 0;
    const windOnshoreGeneration = windResult.rows[0]?.wind_onshore_generation || 0;
    const windOffshoreGeneration = windResult.rows[0]?.wind_offshore_generation || 0;
    
    // Check if a summary already exists
    const existingSummary = await db.execute(
      `SELECT * FROM daily_summaries WHERE summary_date = '${TARGET_DATE}'`
    );
    
    if (existingSummary.rows.length > 0) {
      // Update existing summary
      await db.execute(
        `UPDATE daily_summaries
        SET
          total_curtailed_energy = ${totalEnergy},
          total_payment = ${totalPayment},
          total_wind_generation = ${totalWindGeneration},
          wind_onshore_generation = ${windOnshoreGeneration},
          wind_offshore_generation = ${windOffshoreGeneration},
          last_updated = NOW()
        WHERE summary_date = '${TARGET_DATE}'`
      );
      
      logger.info(`Updated daily summary for ${TARGET_DATE}`);
    } else {
      // Insert new summary
      await db.execute(
        `INSERT INTO daily_summaries (
          summary_date,
          total_curtailed_energy,
          total_payment,
          created_at,
          total_wind_generation,
          wind_onshore_generation,
          wind_offshore_generation,
          last_updated
        ) VALUES (
          '${TARGET_DATE}',
          ${totalEnergy},
          ${totalPayment},
          NOW(),
          ${totalWindGeneration},
          ${windOnshoreGeneration},
          ${windOffshoreGeneration},
          NOW()
        )`
      );
      
      logger.info(`Created daily summary for ${TARGET_DATE}`);
    }
  } catch (error) {
    logger.error(`Error updating daily summary: ${error}`);
    throw error;
  }
}

/**
 * Update monthly summary for May 2025
 */
async function updateMonthlySummary(): Promise<void> {
  try {
    // Get the year and month
    const yearMonth = TARGET_DATE.substring(0, 7);
    
    // Calculate totals for the month
    const result = await db.execute(
      `SELECT 
        SUM(total_curtailed_energy) as total_energy,
        SUM(total_payment) as total_payment,
        SUM(total_wind_generation) as total_wind,
        SUM(wind_onshore_generation) as onshore_wind,
        SUM(wind_offshore_generation) as offshore_wind
      FROM daily_summaries
      WHERE EXTRACT(YEAR FROM summary_date) = ${TARGET_DATE.substring(0, 4)}
      AND EXTRACT(MONTH FROM summary_date) = ${TARGET_DATE.substring(5, 7)}`
    );
    
    const totalEnergy = result.rows[0]?.total_energy || 0;
    const totalPayment = result.rows[0]?.total_payment || 0;
    const totalWind = result.rows[0]?.total_wind || 0;
    const onshoreWind = result.rows[0]?.onshore_wind || 0;
    const offshoreWind = result.rows[0]?.offshore_wind || 0;
    
    // Check if a monthly summary exists
    const existingSummary = await db.execute(
      `SELECT * FROM monthly_summaries WHERE year_month = '${yearMonth}'`
    );
    
    if (existingSummary.rows.length > 0) {
      // Update existing summary
      await db.execute(
        `UPDATE monthly_summaries
        SET
          total_curtailed_energy = ${totalEnergy},
          total_payment = ${totalPayment},
          total_wind_generation = ${totalWind},
          wind_onshore_generation = ${onshoreWind},
          wind_offshore_generation = ${offshoreWind},
          updated_at = NOW(),
          last_updated = NOW()
        WHERE year_month = '${yearMonth}'`
      );
      
      logger.info(`Updated monthly summary for ${yearMonth}`);
    } else {
      // Insert new summary
      await db.execute(
        `INSERT INTO monthly_summaries (
          year_month,
          total_curtailed_energy,
          total_payment,
          created_at,
          updated_at,
          total_wind_generation,
          wind_onshore_generation,
          wind_offshore_generation,
          last_updated
        ) VALUES (
          '${yearMonth}',
          ${totalEnergy},
          ${totalPayment},
          NOW(),
          NOW(),
          ${totalWind},
          ${onshoreWind},
          ${offshoreWind},
          NOW()
        )`
      );
      
      logger.info(`Created monthly summary for ${yearMonth}`);
    }
  } catch (error) {
    logger.error(`Error updating monthly summary: ${error}`);
    throw error;
  }
}

/**
 * Update Bitcoin monthly summary
 */
async function updateBitcoinMonthlySummary(): Promise<void> {
  try {
    const yearMonth = TARGET_DATE.substring(0, 7);
    
    // Delete existing Bitcoin monthly summaries for this month
    await db.execute(
      `DELETE FROM bitcoin_monthly_summaries WHERE year_month = '${yearMonth}'`
    );
    
    logger.info(`Deleted existing Bitcoin monthly summaries for ${yearMonth}`);
    
    // Create summaries for each miner model
    for (const minerModel of MINER_MODELS) {
      // Get total Bitcoin mined for the month with this miner model
      const result = await db.execute(
        `SELECT SUM(bitcoin_mined) as total_bitcoin
        FROM bitcoin_daily_summaries
        WHERE EXTRACT(YEAR FROM summary_date) = ${TARGET_DATE.substring(0, 4)}
        AND EXTRACT(MONTH FROM summary_date) = ${TARGET_DATE.substring(5, 7)}
        AND miner_model = '${minerModel}'`
      );
      
      const totalBitcoin = result.rows[0]?.total_bitcoin || 0;
      
      // Insert the summary
      await db.execute(
        `INSERT INTO bitcoin_monthly_summaries (
          year_month,
          miner_model,
          bitcoin_mined,
          created_at,
          updated_at
        ) VALUES (
          '${yearMonth}',
          '${minerModel}',
          ${totalBitcoin},
          NOW(),
          NOW()
        )`
      );
      
      logger.info(`Created Bitcoin monthly summary for ${yearMonth} and miner ${minerModel}: ${totalBitcoin} BTC`);
    }
  } catch (error) {
    logger.error(`Error updating Bitcoin monthly summary: ${error}`);
    throw error;
  }
}

/**
 * Main function to reprocess May 4th, 2025 data
 */
async function main() {
  logger.info('Starting May 4th, 2025 data reprocessing');

  try {
    // Step 1: Process curtailment data
    logger.info('Step 1: Processing curtailment data...');
    const insertedRecords = await processCurtailmentData();
    logger.info(`Inserted ${insertedRecords} curtailment records`);

    // Step 2: Process Bitcoin calculations
    logger.info('Step 2: Processing Bitcoin calculations...');
    await processBitcoinCalculations();

    // Step 3: Create Bitcoin daily summary
    logger.info('Step 3: Creating Bitcoin daily summary...');
    await createBitcoinDailySummary();

    // Step 4: Update daily summary
    logger.info('Step 4: Updating daily summary...');
    await updateDailySummary();

    // Step 5: Update monthly summary
    logger.info('Step 5: Updating monthly summary...');
    await updateMonthlySummary();

    // Step 6: Update Bitcoin monthly summary
    logger.info('Step 6: Updating Bitcoin monthly summary...');
    await updateBitcoinMonthlySummary();

    logger.info('May 4th, 2025 data reprocessing completed successfully');
  } catch (error) {
    logger.error(`Error reprocessing May 4th data: ${error}`);
    process.exit(1);
  }
}

// Run the main function
main();