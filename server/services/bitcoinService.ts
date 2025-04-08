/**
 * Bitcoin Service
 * 
 * This service handles Bitcoin calculations and summary updates.
 */

import { db } from '../../db';
import { 
  historicalBitcoinCalculations, 
  bitcoinMonthlySummaries, 
  bitcoinYearlySummaries 
} from '../../db/schema';
import { eq, and, sql, desc, inArray, gte, lte, sum } from 'drizzle-orm';

/**
 * Calculate Bitcoin for a BMU based on energy, miner model, and difficulty
 * 
 * @param energyMWh - Energy in MWh
 * @param minerModel - Miner model (e.g., 'S19J_PRO')
 * @param difficulty - Bitcoin network difficulty
 * @returns Potential Bitcoin mined
 */
export function calculateBitcoinForBMU(
  energyMWh: number,
  minerModel: string,
  difficulty: number
): number {
  try {
    // Convert MWh to kWh
    const energyKWh = energyMWh * 1000;
    
    // Miner model specifications (terahashes per second and power consumption in watts)
    const minerSpecs: Record<string, { hashrate: number, power: number }> = {
      'S19J_PRO': { hashrate: 100, power: 3050 },
      'S19_XP': { hashrate: 140, power: 3010 },
      'S21_XP': { hashrate: 190, power: 3100 },
      'M50S': { hashrate: 166, power: 3300 },
      'M50': { hashrate: 132, power: 3348 }
    };
    
    // Use specified miner or default to S19J_PRO
    const miner = minerSpecs[minerModel] || minerSpecs['S19J_PRO'];
    
    // Calculate how many miners this energy could power
    const hoursInDay = 24;
    const minerDailyEnergyKWh = (miner.power / 1000) * hoursInDay;
    const minersSupported = energyKWh / minerDailyEnergyKWh;
    
    // Calculate total hashrate these miners would produce (TH/s)
    const totalHashrateTHs = minersSupported * miner.hashrate;
    
    // Convert TH/s to H/s for the Bitcoin formula
    const totalHashrateHs = totalHashrateTHs * 1e12;
    
    // Bitcoin mining formula: 
    // BTC = (hashrate * block_reward * seconds_per_day) / (difficulty * 2^32)
    const blockReward = 6.25; // Current block reward in BTC
    const secondsPerDay = 86400;
    const divisor = difficulty * Math.pow(2, 32);
    
    // Calculate Bitcoin mined per day
    const bitcoinMined = (totalHashrateHs * blockReward * secondsPerDay) / divisor;
    
    return bitcoinMined;
  } catch (error) {
    console.error('Error calculating Bitcoin:', error);
    return 0;
  }
}

/**
 * Process historical Bitcoin calculations for a specific date and miner model
 */
export async function processHistoricalCalculations(
  date: string,
  minerModel: string
): Promise<number> {
  // Implementation needed
  return 0;
}

/**
 * Process a single day's worth of Bitcoin calculations
 */
export async function processSingleDay(
  date: string
): Promise<void> {
  // Implementation needed
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
    const parts = yearMonth.split('-');
    
    if (parts.length !== 2) {
      throw new Error(`Invalid year-month format: ${yearMonth}, expected 'YYYY-MM'`);
    }
    
    const year = parts[0];
    const month = parts[1];
    
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
    
    const rows = result as any[];
    const data = rows.length > 0 ? rows[0] : null;
    
    if (!data || !data.total_bitcoin) {
      console.log(`No Bitcoin data found for ${yearMonth} and ${minerModel}`);
      return;
    }
    
    // Delete existing summary if any
    await db.execute(sql`
      DELETE FROM bitcoin_monthly_summaries
      WHERE year_month = ${yearMonth}
      AND miner_model = ${minerModel}
    `);
    
    // Insert new summary using the correct column names
    await db.execute(sql`
      INSERT INTO bitcoin_monthly_summaries 
      (year_month, miner_model, bitcoin_mined, value_at_mining, updated_at)
      VALUES (
        ${yearMonth},
        ${minerModel},
        ${data.total_bitcoin.toString()},
        '0', 
        ${new Date().toISOString()}
      )
    `);
    
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
    const minerModelResults = minerModelsResult as any[];
    
    for (let i = 0; i < minerModelResults.length; i++) {
      const row = minerModelResults[i] as any;
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
      
      const monthlyResults = monthlyResult as any[];
      let data: any = null;
      
      if (monthlyResults.length > 0) {
        data = monthlyResults[0];
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
      
      const tableExistsResults = tableExistsResult as any[];
      const tableExists = tableExistsResults.length > 0 && tableExistsResults[0].exists === true;
      
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
      
      // Insert new yearly summary with all required fields
      await db.execute(sql`
        INSERT INTO bitcoin_yearly_summaries 
        (year, miner_model, bitcoin_mined, value_at_mining, updated_at)
        VALUES (
          ${year},
          ${minerModel},
          ${data.total_bitcoin.toString()},
          '0', // Default value for value_at_mining
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