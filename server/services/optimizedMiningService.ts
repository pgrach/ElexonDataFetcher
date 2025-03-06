/**
 * Optimized Mining Service
 * 
 * This service provides direct query optimization for mining potential calculations
 * without relying on materialized view tables. It uses efficient query patterns
 * directly on the core tables:
 * - curtailment_records
 * - historical_bitcoin_calculations
 */

import { db } from "../../db";
import { 
  historicalBitcoinCalculations, 
  curtailmentRecords,
  bitcoinYearlySummaries,
  yearlySummaries,
  bitcoinMonthlySummaries
} from "../../db/schema";
import { sql, eq, and, or, desc, asc } from "drizzle-orm";
import { format } from "date-fns";

/**
 * Get daily mining potential data directly from core tables with optimized queries
 */
export async function getDailyMiningPotential(date: string, minerModel: string, farmId?: string): Promise<any> {
  console.log(`Calculating daily mining potential for ${date}, model: ${minerModel}, farm: ${farmId || 'all'}`);
  
  try {
    // Query for Bitcoin calculations first
    let bitcoinQuery = db
      .select({
        totalBitcoinMined: sql<number>`SUM(bitcoin_mined)`,
        // Also retrieve difficulty as it's useful for the response
        difficulty: sql<number>`MAX(difficulty)`
      })
      .from(historicalBitcoinCalculations)
      .where(
        and(
          eq(historicalBitcoinCalculations.settlementDate, date),
          eq(historicalBitcoinCalculations.minerModel, minerModel),
          farmId ? eq(historicalBitcoinCalculations.farmId, farmId) : undefined
        )
      );
      
    const bitcoinResults = await bitcoinQuery;
    
    // Query for curtailment data
    let curtailmentQuery = db
      .select({
        totalCurtailedEnergy: sql<number>`SUM(ABS(volume))`
      })
      .from(curtailmentRecords)
      .where(
        and(
          eq(curtailmentRecords.settlementDate, date),
          farmId ? eq(curtailmentRecords.farmId, farmId) : undefined
        )
      );
      
    const curtailmentResults = await curtailmentQuery;
    
    // Return consolidated results
    return {
      date,
      totalCurtailedEnergy: Number(curtailmentResults[0]?.totalCurtailedEnergy || 0),
      totalBitcoinMined: Number(bitcoinResults[0]?.totalBitcoinMined || 0),
      difficulty: Number(bitcoinResults[0]?.difficulty || 0)
    };
  } catch (error) {
    console.error(`Error calculating daily mining potential for ${date}:`, error);
    throw error;
  }
}

/**
 * Get monthly mining potential data directly from core tables with optimized queries
 */
export async function getMonthlyMiningPotential(yearMonth: string, minerModel: string, farmId?: string): Promise<any> {
  console.log(`Calculating monthly mining potential for ${yearMonth}, model: ${minerModel}, farm: ${farmId || 'all'}`);
  
  try {
    // Convert yearMonth to date range (e.g., "2025-03" to "2025-03-01" and "2025-03-31")
    const [year, month] = yearMonth.split('-').map(n => parseInt(n, 10));
    const startDate = new Date(year, month - 1, 1);
    const endDate = new Date(year, month, 0); // Last day of month
    
    const formattedStartDate = format(startDate, 'yyyy-MM-dd');
    const formattedEndDate = format(endDate, 'yyyy-MM-dd');
    
    // Query for Bitcoin calculations with date range
    let bitcoinQuery = db
      .select({
        totalBitcoinMined: sql<number>`SUM(bitcoin_mined)`,
        difficulty: sql<number>`AVG(difficulty)` // Average difficulty for the month
      })
      .from(historicalBitcoinCalculations)
      .where(
        and(
          sql`settlement_date BETWEEN ${formattedStartDate} AND ${formattedEndDate}`,
          eq(historicalBitcoinCalculations.minerModel, minerModel),
          farmId ? eq(historicalBitcoinCalculations.farmId, farmId) : undefined
        )
      );
      
    const bitcoinResults = await bitcoinQuery;
    
    // Query for curtailment data with date range
    let curtailmentQuery = db
      .select({
        totalCurtailedEnergy: sql<number>`SUM(ABS(volume))`
      })
      .from(curtailmentRecords)
      .where(
        and(
          sql`settlement_date BETWEEN ${formattedStartDate} AND ${formattedEndDate}`,
          farmId ? eq(curtailmentRecords.farmId, farmId) : undefined
        )
      );
      
    const curtailmentResults = await curtailmentQuery;
    
    // Return consolidated results
    return {
      month: yearMonth,
      totalCurtailedEnergy: Number(curtailmentResults[0]?.totalCurtailedEnergy || 0),
      totalBitcoinMined: Number(bitcoinResults[0]?.totalBitcoinMined || 0),
      averageDifficulty: Number(bitcoinResults[0]?.difficulty || 0)
    };
  } catch (error) {
    console.error(`Error calculating monthly mining potential for ${yearMonth}:`, error);
    throw error;
  }
}

/**
 * Get yearly mining potential data directly from core tables with optimized queries
 */
export async function getYearlyMiningPotential(year: string, minerModel: string, farmId?: string): Promise<any> {
  console.log(`Calculating yearly mining potential for ${year}, model: ${minerModel}, farm: ${farmId || 'all'}`);
  
  try {
    // If requesting for a specific farm, we still need to use the detailed calculation approach
    if (farmId) {
      // Query for Bitcoin calculations with year filter by farm
      let bitcoinQuery = db
        .select({
          totalBitcoinMined: sql<number>`SUM(bitcoin_mined)`,
          avgDifficulty: sql<number>`AVG(difficulty)` // Average difficulty for the year
        })
        .from(historicalBitcoinCalculations)
        .where(
          and(
            sql`EXTRACT(YEAR FROM settlement_date) = ${parseInt(year, 10)}`,
            eq(historicalBitcoinCalculations.minerModel, minerModel),
            eq(historicalBitcoinCalculations.farmId, farmId)
          )
        );
        
      const bitcoinResults = await bitcoinQuery;
      
      // Query for curtailment data with year filter by farm
      let curtailmentQuery = db
        .select({
          totalCurtailedEnergy: sql<number>`SUM(ABS(volume))`,
          totalPayment: sql<number>`SUM(payment)`
        })
        .from(curtailmentRecords)
        .where(
          and(
            sql`EXTRACT(YEAR FROM settlement_date) = ${parseInt(year, 10)}`,
            eq(curtailmentRecords.farmId, farmId)
          )
        );
        
      const curtailmentResults = await curtailmentQuery;
      
      // Return consolidated results
      return {
        year,
        totalCurtailedEnergy: Number(curtailmentResults[0]?.totalCurtailedEnergy || 0),
        totalBitcoinMined: Number(bitcoinResults[0]?.totalBitcoinMined || 0),
        totalPayment: Number(curtailmentResults[0]?.totalPayment || 0),
        averageDifficulty: Number(bitcoinResults[0]?.avgDifficulty || 0)
      };
    }
    
    // For all farms (no farmId filter), use the yearly summaries table for better performance
    const yearlySummary = await db
      .select({
        bitcoinMined: bitcoinYearlySummaries.bitcoinMined,
        averageDifficulty: bitcoinYearlySummaries.averageDifficulty
      })
      .from(bitcoinYearlySummaries)
      .where(
        and(
          eq(bitcoinYearlySummaries.year, year),
          eq(bitcoinYearlySummaries.minerModel, minerModel)
        )
      );
      
    // If we don't have a yearly summary yet, calculate it on the fly
    if (!yearlySummary.length) {
      console.log(`No yearly summary found in bitcoin_yearly_summaries for ${year}, calculating on the fly`);
      
      // Query for Bitcoin calculations directly from the historical data
      let bitcoinQuery = db
        .select({
          totalBitcoinMined: sql<number>`SUM(bitcoin_mined)`,
          avgDifficulty: sql<number>`AVG(difficulty)` // Average difficulty for the year
        })
        .from(historicalBitcoinCalculations)
        .where(
          and(
            sql`EXTRACT(YEAR FROM settlement_date) = ${parseInt(year, 10)}`,
            eq(historicalBitcoinCalculations.minerModel, minerModel)
          )
        );
        
      const bitcoinResults = await bitcoinQuery;
      
      // Query for yearly summary from yearly_summaries table for total payment
      const yearlySummaryData = await db
        .select({
          totalCurtailedEnergy: yearlySummaries.totalCurtailedEnergy,
          totalPayment: yearlySummaries.totalPayment
        })
        .from(yearlySummaries)
        .where(eq(yearlySummaries.year, year));
      
      // Return consolidated results
      return {
        year,
        totalCurtailedEnergy: Number(yearlySummaryData[0]?.totalCurtailedEnergy || 0),
        totalBitcoinMined: Number(bitcoinResults[0]?.totalBitcoinMined || 0),
        totalPayment: Number(yearlySummaryData[0]?.totalPayment || 0),
        averageDifficulty: Number(bitcoinResults[0]?.avgDifficulty || 0)
      };
    }
    
    // Get curtailment energy and payment data from yearlySummaries table
    const generalYearlySummary = await db
      .select({
        totalCurtailedEnergy: yearlySummaries.totalCurtailedEnergy,
        totalPayment: yearlySummaries.totalPayment
      })
      .from(yearlySummaries)
      .where(eq(yearlySummaries.year, year));
    
    // Return consolidated results from the summaries tables
    return {
      year,
      totalCurtailedEnergy: Number(generalYearlySummary[0]?.totalCurtailedEnergy || 0),
      totalBitcoinMined: Number(yearlySummary[0]?.bitcoinMined || 0),
      totalPayment: Number(generalYearlySummary[0]?.totalPayment || 0),
      averageDifficulty: Number(yearlySummary[0]?.averageDifficulty || 0)
    };
  } catch (error) {
    console.error(`Error calculating yearly mining potential for ${year}:`, error);
    throw error;
  }
}

/**
 * Get farm-specific statistics across time periods
 */
export async function getFarmStatistics(farmId: string, period: 'day' | 'month' | 'year', value: string): Promise<any> {
  console.log(`Getting farm statistics for ${farmId}, period: ${period}, value: ${value}`);
  
  try {
    let bitcoinDateCondition;
    let curtailmentDateCondition;
    
    if (period === 'day') {
      // For a specific day
      bitcoinDateCondition = eq(historicalBitcoinCalculations.settlementDate, value);
      curtailmentDateCondition = eq(curtailmentRecords.settlementDate, value);
    } else if (period === 'month') {
      // For a specific month (YYYY-MM)
      const [year, month] = value.split('-').map(n => parseInt(n, 10));
      const startDate = new Date(year, month - 1, 1);
      const endDate = new Date(year, month, 0); // Last day of month
      const formattedStartDate = format(startDate, 'yyyy-MM-dd');
      const formattedEndDate = format(endDate, 'yyyy-MM-dd');
      
      bitcoinDateCondition = sql`${historicalBitcoinCalculations.settlementDate} BETWEEN ${formattedStartDate} AND ${formattedEndDate}`;
      curtailmentDateCondition = sql`${curtailmentRecords.settlementDate} BETWEEN ${formattedStartDate} AND ${formattedEndDate}`;
    } else if (period === 'year') {
      // For a specific year
      bitcoinDateCondition = sql`EXTRACT(YEAR FROM ${historicalBitcoinCalculations.settlementDate}) = ${parseInt(value, 10)}`;
      curtailmentDateCondition = sql`EXTRACT(YEAR FROM ${curtailmentRecords.settlementDate}) = ${parseInt(value, 10)}`;
    } else {
      throw new Error(`Invalid period type: ${period}`);
    }
    
    // Get statistics by miner model for the farm
    const results = await db
      .select({
        minerModel: historicalBitcoinCalculations.minerModel,
        totalBitcoinMined: sql<number>`SUM(bitcoin_mined)`,
        periodCount: sql<number>`COUNT(DISTINCT settlement_date)`,
        averageDifficulty: sql<number>`AVG(difficulty)`
      })
      .from(historicalBitcoinCalculations)
      .where(
        and(
          bitcoinDateCondition,
          eq(historicalBitcoinCalculations.farmId, farmId)
        )
      )
      .groupBy(historicalBitcoinCalculations.minerModel);
    
    // Get curtailment data
    const curtailmentData = await db
      .select({
        totalCurtailedEnergy: sql<number>`SUM(ABS(volume))`,
        periodCount: sql<number>`COUNT(DISTINCT settlement_date)`,
        totalPayment: sql<number>`SUM(payment)`
      })
      .from(curtailmentRecords)
      .where(
        and(
          curtailmentDateCondition,
          eq(curtailmentRecords.farmId, farmId)
        )
      );
    
    // Combine and return results
    return {
      farmId,
      period,
      value,
      totalCurtailedEnergy: Number(curtailmentData[0]?.totalCurtailedEnergy || 0),
      totalPayment: Number(curtailmentData[0]?.totalPayment || 0),
      totalPeriods: Number(curtailmentData[0]?.periodCount || 0),
      minerModels: results.map(r => ({
        model: r.minerModel,
        bitcoinMined: Number(r.totalBitcoinMined || 0),
        averageDifficulty: Number(r.averageDifficulty || 0)
      }))
    };
  } catch (error) {
    console.error(`Error getting farm statistics for ${farmId}:`, error);
    throw error;
  }
}