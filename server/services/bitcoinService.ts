/**
 * Bitcoin Service
 * 
 * This service handles Bitcoin calculations and summary updates.
 */

import { db } from '../../db';
import { 
  historicalBitcoinCalculations, 
  bitcoinMonthlySummaries, 
  bitcoinYearlySummaries,
  curtailmentRecords
} from '../../db/schema';
import { eq, and, sql, desc, inArray, gte, lte, sum } from 'drizzle-orm';
import { calculateBitcoin } from '../utils/bitcoin';
import { getDifficultyData } from './dynamodbService';

/**
 * Process Bitcoin calculations for a single day and miner model
 * 
 * @param date - The settlement date in format 'YYYY-MM-DD'
 * @param minerModel - The miner model (e.g., 'S19J_PRO', 'S9', 'M20S')
 */
export async function processSingleDay(
  date: string,
  minerModel: string
): Promise<void> {
  try {
    console.log(`Processing Bitcoin calculations for ${date} with miner model ${minerModel}`);
    
    // Step 1: Get the network difficulty for this date
    const difficulty = await getDifficultyData(date);
    console.log(`Using difficulty ${difficulty} for ${date}`);
    
    // Step 2: Delete any existing calculations for this date and model to avoid duplicates
    await db.delete(historicalBitcoinCalculations)
      .where(and(
        eq(historicalBitcoinCalculations.settlementDate, date),
        eq(historicalBitcoinCalculations.minerModel, minerModel)
      ));
    
    console.log(`Deleted existing calculations for ${date} and ${minerModel}`);
    
    // Step 3: Get all curtailment records for this date
    const records = await db.select({
      settlementPeriod: curtailmentRecords.settlementPeriod,
      farmId: curtailmentRecords.farmId,
      leadPartyName: curtailmentRecords.leadPartyName,
      volume: curtailmentRecords.volume
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date));
    
    if (records.length === 0) {
      console.log(`No curtailment records found for ${date}`);
      return;
    }
    
    console.log(`Found ${records.length} curtailment records for ${date}`);
    
    // Step 4: Calculate Bitcoin for each curtailment record
    let totalBitcoin = 0;
    const insertPromises = [];
    
    for (const record of records) {
      // Convert volume (MWh) to positive number for calculation
      const mwh = Math.abs(Number(record.volume));
      
      // Skip records with zero or invalid volume
      if (mwh <= 0 || isNaN(mwh)) {
        continue;
      }
      
      // Calculate Bitcoin mined
      const bitcoinMined = calculateBitcoin(mwh, minerModel, difficulty);
      totalBitcoin += bitcoinMined;
      
      // Insert the calculation record
      insertPromises.push(
        db.insert(historicalBitcoinCalculations).values({
          settlementDate: date,
          settlementPeriod: Number(record.settlementPeriod),
          minerModel: minerModel,
          farmId: record.farmId,
          bitcoinMined: bitcoinMined.toString(),
          difficulty: difficulty.toString()
        })
      );
    }
    
    // Execute all inserts
    await Promise.all(insertPromises);
    
    console.log(`Successfully processed ${insertPromises.length} Bitcoin calculations for ${date} and ${minerModel}`);
    console.log(`Total Bitcoin calculated: ${totalBitcoin.toFixed(8)}`);
    
    // Calculate monthly summary for the month containing this date
    const yearMonth = date.substring(0, 7); // YYYY-MM
    await calculateMonthlyBitcoinSummary(yearMonth, minerModel);
    
    // Update yearly summary for the year containing this date
    const year = date.substring(0, 4); // YYYY
    await manualUpdateYearlyBitcoinSummary(year);
    
  } catch (error) {
    console.error(`Error processing Bitcoin calculations for ${date} and ${minerModel}:`, error);
    throw error;
  }
}

/**
 * Process historical calculations for a date range and multiple miner models
 * 
 * @param startDate - Start date in YYYY-MM-DD format
 * @param endDate - End date in YYYY-MM-DD format
 * @param minerModels - Array of miner models to process
 */
export async function processHistoricalCalculations(
  startDate: string,
  endDate: string,
  minerModels: string[]
): Promise<void> {
  try {
    console.log(`Processing historical calculations from ${startDate} to ${endDate}`);
    
    // Get all dates in the range
    const start = new Date(startDate);
    const end = new Date(endDate);
    const dates: string[] = [];
    
    // Generate all dates in the range
    const current = new Date(start);
    while (current <= end) {
      dates.push(current.toISOString().split('T')[0]);
      current.setDate(current.getDate() + 1);
    }
    
    console.log(`Processing ${dates.length} days and ${minerModels.length} miner models`);
    
    // Process each date and miner model
    for (const date of dates) {
      for (const minerModel of minerModels) {
        await processSingleDay(date, minerModel);
      }
    }
    
    console.log(`Successfully processed historical calculations from ${startDate} to ${endDate}`);
  } catch (error) {
    console.error(`Error processing historical calculations:`, error);
    throw error;
  }
}

/**
 * Calculate Monthly Bitcoin Summary
 * 
 * @param yearMonth - Year and month in format 'YYYY-MM'
 * @param minerModel - Miner model (e.g., 'S19J_PRO')
 */
export async function calculateMonthlyBitcoinSummary(
  yearMonth: string,
  minerModel: string
): Promise<void> {
  try {
    console.log(`Calculating monthly Bitcoin summary for ${yearMonth} and ${minerModel}...`);
    
    // Extract year and month from YYYY-MM format
    const [year, month] = yearMonth.split('-');
    
    if (!year || !month) {
      throw new Error(`Invalid year-month format: ${yearMonth}, expected 'YYYY-MM'`);
    }
    
    // Calculate start and end date for the month
    const startDate = `${year}-${month}-01`;
    
    // Get the last day of the month
    const endDate = new Date(Number(year), Number(month), 0).toISOString().split('T')[0];
    
    // Query the historical Bitcoin calculations for the month
    const result = await db.execute(sql`
      SELECT
        SUM(bitcoin_mined::NUMERIC) as total_bitcoin,
        COUNT(DISTINCT settlement_date) as days_count,
        MIN(settlement_date) as first_date,
        MAX(settlement_date) as last_date
      FROM
        historical_bitcoin_calculations
      WHERE
        settlement_date >= ${startDate}
        AND settlement_date <= ${endDate}
        AND miner_model = ${minerModel}
    `);
    
    const data = result[0] as any;
    
    if (!data || !data.total_bitcoin) {
      console.log(`No Bitcoin data found for ${yearMonth} and ${minerModel}`);
      return;
    }
    
    // Delete existing summary if any
    const yearMonth2 = `${year}-${month}`; // Using a different variable name to avoid redeclaration
    await db.execute(sql`
      DELETE FROM bitcoin_monthly_summaries
      WHERE year_month = ${yearMonth2}
      AND miner_model = ${minerModel}
    `);
    
    // Insert new summary
    await db.insert(bitcoinMonthlySummaries).values({
      yearMonth: yearMonth,
      minerModel: minerModel,
      bitcoinMined: data.total_bitcoin.toString(),
      updatedAt: new Date()
    });
    
    console.log(`Monthly Bitcoin summary updated for ${yearMonth} and ${minerModel}: ${data.total_bitcoin} BTC`);
  } catch (error) {
    console.error(`Error calculating monthly Bitcoin summary:`, error);
    throw error;
  }
}

/**
 * Update Yearly Bitcoin Summary
 * 
 * @param year - Year in format 'YYYY'
 */
export async function manualUpdateYearlyBitcoinSummary(year: string): Promise<void> {
  try {
    console.log(`Updating yearly Bitcoin summary for ${year}...`);
    
    // Get all unique miner models in the monthly summaries for this year
    const yearPrefix = `${year}-`;
    const minerModelsResult = await db.execute(sql`
      SELECT DISTINCT miner_model
      FROM bitcoin_monthly_summaries
      WHERE year_month LIKE ${yearPrefix + '%'}
    `);
    
    // Convert result to array of miner models
    const minerModels: string[] = [];
    for (let i = 0; i < minerModelsResult.length; i++) {
      const row = minerModelsResult[i] as any;
      if (row.miner_model) {
        minerModels.push(row.miner_model);
      }
    }
    
    if (minerModels.length === 0) {
      console.log(`No miner models found for ${year}`);
      return;
    }
    
    console.log(`Found ${minerModels.length} miner models: ${minerModels.join(', ')}`);
    
    // Process each miner model
    for (const minerModel of minerModels) {
      // Query the monthly summaries for the year for this miner model
      const monthlyResult = await db.execute(sql`
        SELECT
          SUM(bitcoin_mined::NUMERIC) as total_bitcoin,
          COUNT(*) as months_count
        FROM
          bitcoin_monthly_summaries
        WHERE
          year_month LIKE ${yearPrefix + '%'}
          AND miner_model = ${minerModel}
      `);
      
      let data: any = null;
      if (monthlyResult.length > 0) {
        data = monthlyResult[0] as any;
      }
      
      if (!data || !data.total_bitcoin) {
        console.log(`No monthly summary data found for ${year} and ${minerModel}`);
        continue;
      }
      
      // Check if the yearly summaries table exists
      const tableExistsResult = await db.execute(sql`
        SELECT EXISTS (
          SELECT FROM information_schema.tables 
          WHERE table_name = 'bitcoin_yearly_summaries'
        ) as exists
      `);
      
      const tableExists = tableExistsResult[0] && (tableExistsResult[0] as any).exists === true;
      
      if (!tableExists) {
        console.log(`Warning: bitcoin_yearly_summaries table doesn't exist. Skipping yearly summary update.`);
        continue;
      }
      
      // Delete existing yearly summary if any
      await db.execute(sql`
        DELETE FROM bitcoin_yearly_summaries
        WHERE year = ${year}
        AND miner_model = ${minerModel}
      `);
      
      // Insert new yearly summary
      await db.execute(sql`
        INSERT INTO bitcoin_yearly_summaries 
        (year, miner_model, bitcoin_mined, months_count, updated_at)
        VALUES (
          ${year},
          ${minerModel},
          ${data.total_bitcoin.toString()},
          ${data.months_count || 0},
          ${new Date().toISOString()}
        )
      `);
      
      console.log(`Yearly Bitcoin summary updated for ${year} and ${minerModel}: ${data.total_bitcoin} BTC`);
    }
    
    console.log(`Yearly Bitcoin summary update completed for ${year}`);
  } catch (error) {
    console.error(`Error updating yearly Bitcoin summary:`, error);
    throw error;
  }
}