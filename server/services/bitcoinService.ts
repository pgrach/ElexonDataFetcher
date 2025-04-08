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