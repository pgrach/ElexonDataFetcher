/**
 * Mining Potential Service
 * 
 * This service manages materialized view tables for mining potential calculations:
 * - settlement_period_mining: Per-period mining data
 * - daily_mining_potential: Daily aggregated mining data
 * - yearly_mining_potential: Yearly aggregated mining data
 * 
 * These tables act as materialized views to improve performance for frequently accessed calculations.
 */

import { db } from "../../db";
import { 
  historicalBitcoinCalculations, 
  curtailmentRecords,
  settlementPeriodMining,
  dailyMiningPotential,
  yearlyMiningPotential
} from "../../db/schema";
import { sql, eq, and, or, desc, asc } from "drizzle-orm";
import { format, parseISO } from "date-fns";

// Cache management
let MATERIALIZATION_IN_PROGRESS = false;
const MATERIALIZATION_QUEUE: Set<string> = new Set();

/**
 * Populate settlement_period_mining table for a specific date and miner model
 */
export async function populateSettlementPeriodMining(date: string, minerModel: string): Promise<void> {
  console.log(`Populating settlement_period_mining for ${date} and ${minerModel}`);
  
  // Check if data already exists
  const existingCount = await db
    .select({ count: sql<number>`count(*)` })
    .from(settlementPeriodMining)
    .where(
      and(
        eq(settlementPeriodMining.settlementDate, date),
        eq(settlementPeriodMining.minerModel, minerModel)
      )
    );
    
  if (existingCount[0]?.count > 0) {
    console.log(`Settlement period mining data already exists for ${date} and ${minerModel}`);
    return;
  }
  
  // Get historical bitcoin calculations
  const calculations = await db
    .select({
      settlementDate: historicalBitcoinCalculations.settlementDate,
      settlementPeriod: historicalBitcoinCalculations.settlementPeriod,
      farmId: historicalBitcoinCalculations.farmId,
      bitcoinMined: historicalBitcoinCalculations.bitcoinMined,
      difficulty: historicalBitcoinCalculations.difficulty
    })
    .from(historicalBitcoinCalculations)
    .where(
      and(
        eq(historicalBitcoinCalculations.settlementDate, date),
        eq(historicalBitcoinCalculations.minerModel, minerModel)
      )
    );
    
  if (calculations.length === 0) {
    console.log(`No historical calculations found for ${date} and ${minerModel}`);
    return;
  }
  
  // Get curtailment data to match with calculations
  const curtailmentData = await db
    .select({
      settlementDate: curtailmentRecords.settlementDate,
      settlementPeriod: curtailmentRecords.settlementPeriod,
      farmId: curtailmentRecords.farmId,
      volume: curtailmentRecords.volume
    })
    .from(curtailmentRecords)
    .where(
      eq(curtailmentRecords.settlementDate, date)
    );
    
  // Create a lookup for curtailment volumes
  const curtailmentLookup = new Map();
  for (const record of curtailmentData) {
    const key = `${record.settlementDate}_${record.settlementPeriod}_${record.farmId}`;
    curtailmentLookup.set(key, Math.abs(Number(record.volume)));
  }
  
  // Prepare data for bulk insert
  const dataToInsert = calculations.map(calc => {
    const key = `${calc.settlementDate}_${calc.settlementPeriod}_${calc.farmId}`;
    const curtailedEnergy = curtailmentLookup.get(key) || 0;
    
    return {
      settlementDate: calc.settlementDate,
      settlementPeriod: calc.settlementPeriod,
      farmId: calc.farmId,
      minerModel: minerModel,
      curtailedEnergy: curtailedEnergy,
      bitcoinMined: calc.bitcoinMined,
      difficulty: calc.difficulty,
      // Price and value will be updated later
      price: null,
      valueAtPrice: null
    };
  });
  
  // Batch insert data
  if (dataToInsert.length > 0) {
    await db.insert(settlementPeriodMining).values(dataToInsert);
    console.log(`Inserted ${dataToInsert.length} records into settlement_period_mining for ${date} and ${minerModel}`);
  }
}

/**
 * Populate daily_mining_potential table for a specific date and miner model
 */
export async function populateDailyMiningPotential(date: string, minerModel: string): Promise<void> {
  console.log(`Populating daily_mining_potential for ${date} and ${minerModel}`);
  
  // Check if data already exists
  const existingCount = await db
    .select({ count: sql<number>`count(*)` })
    .from(dailyMiningPotential)
    .where(
      and(
        eq(dailyMiningPotential.summaryDate, date),
        eq(dailyMiningPotential.minerModel, minerModel)
      )
    );
    
  if (existingCount[0]?.count > 0) {
    console.log(`Daily mining potential data already exists for ${date} and ${minerModel}`);
    return;
  }
  
  // Try to use settlement_period_mining data first
  const periodData = await db
    .select({
      farmId: settlementPeriodMining.farmId,
      totalCurtailedEnergy: sql<number>`SUM(curtailed_energy)`,
      totalBitcoinMined: sql<number>`SUM(bitcoin_mined)`
    })
    .from(settlementPeriodMining)
    .where(
      and(
        eq(settlementPeriodMining.settlementDate, date),
        eq(settlementPeriodMining.minerModel, minerModel)
      )
    )
    .groupBy(settlementPeriodMining.farmId);
    
  if (periodData.length > 0) {
    // Insert aggregated data
    const dataToInsert = periodData.map(data => ({
      summaryDate: date,
      farmId: data.farmId,
      minerModel: minerModel,
      totalCurtailedEnergy: String(data.totalCurtailedEnergy),
      totalBitcoinMined: String(data.totalBitcoinMined),
      averageValue: null, // Will be calculated later if needed
    }));
    
    await db.insert(dailyMiningPotential).values(dataToInsert);
    console.log(`Inserted ${dataToInsert.length} records into daily_mining_potential for ${date} and ${minerModel}`);
    return;
  }
  
  // If no period data, try to calculate directly from historicalBitcoinCalculations
  const bitcoinData = await db
    .select({
      farmId: historicalBitcoinCalculations.farmId,
      totalBitcoinMined: sql<number>`SUM(bitcoin_mined)`
    })
    .from(historicalBitcoinCalculations)
    .where(
      and(
        eq(historicalBitcoinCalculations.settlementDate, date),
        eq(historicalBitcoinCalculations.minerModel, minerModel)
      )
    )
    .groupBy(historicalBitcoinCalculations.farmId);
    
  if (bitcoinData.length > 0) {
    // Get curtailment data
    const curtailmentSums = await db
      .select({
        farmId: curtailmentRecords.farmId,
        totalCurtailedEnergy: sql<number>`SUM(ABS(volume))`
      })
      .from(curtailmentRecords)
      .where(
        eq(curtailmentRecords.settlementDate, date)
      )
      .groupBy(curtailmentRecords.farmId);
      
    // Create a lookup for curtailment volumes
    const curtailmentLookup = new Map();
    for (const record of curtailmentSums) {
      curtailmentLookup.set(record.farmId, Number(record.totalCurtailedEnergy));
    }
    
    // Prepare data for insert
    const dataToInsert = bitcoinData.map(data => ({
      summaryDate: date,
      farmId: data.farmId,
      minerModel: minerModel,
      totalCurtailedEnergy: String(curtailmentLookup.get(data.farmId) || 0),
      totalBitcoinMined: String(data.totalBitcoinMined),
      averageValue: null, // Will be calculated later if needed
    }));
    
    await db.insert(dailyMiningPotential).values(dataToInsert);
    console.log(`Inserted ${dataToInsert.length} records into daily_mining_potential for ${date} and ${minerModel}`);
  } else {
    console.log(`No Bitcoin calculation data found for ${date} and ${minerModel}`);
  }
}

/**
 * Populate yearly_mining_potential table for a specific year and miner model
 */
export async function populateYearlyMiningPotential(year: string, minerModel: string): Promise<void> {
  console.log(`Populating yearly_mining_potential for ${year} and ${minerModel}`);
  
  // Check if data already exists
  const existingCount = await db
    .select({ count: sql<number>`count(*)` })
    .from(yearlyMiningPotential)
    .where(
      and(
        eq(yearlyMiningPotential.year, year),
        eq(yearlyMiningPotential.minerModel, minerModel)
      )
    );
    
  if (existingCount[0]?.count > 0) {
    console.log(`Yearly mining potential data already exists for ${year} and ${minerModel}`);
    return;
  }
  
  // Try to aggregate from daily_mining_potential
  const dailyData = await db
    .select({
      farmId: dailyMiningPotential.farmId,
      totalCurtailedEnergy: sql<number>`SUM(total_curtailed_energy)`,
      totalBitcoinMined: sql<number>`SUM(total_bitcoin_mined)`
    })
    .from(dailyMiningPotential)
    .where(
      and(
        sql`DATE_TRUNC('year', summary_date)::text LIKE ${year + '%'}`,
        eq(dailyMiningPotential.minerModel, minerModel)
      )
    )
    .groupBy(dailyMiningPotential.farmId);
    
  if (dailyData.length > 0) {
    // Insert aggregated data
    const dataToInsert = dailyData.map(data => ({
      year,
      farmId: data.farmId,
      minerModel,
      totalCurtailedEnergy: String(data.totalCurtailedEnergy),
      totalBitcoinMined: String(data.totalBitcoinMined),
      averageValue: null, // Will be calculated later if needed
    }));
    
    await db.insert(yearlyMiningPotential).values(dataToInsert);
    console.log(`Inserted ${dataToInsert.length} records into yearly_mining_potential for ${year} and ${minerModel}`);
    return;
  }
  
  // If no daily data, try to calculate directly from historicalBitcoinCalculations
  const bitcoinData = await db
    .select({
      farmId: historicalBitcoinCalculations.farmId,
      totalBitcoinMined: sql<number>`SUM(bitcoin_mined)`
    })
    .from(historicalBitcoinCalculations)
    .where(
      and(
        sql`DATE_TRUNC('year', settlement_date)::text LIKE ${year + '%'}`,
        eq(historicalBitcoinCalculations.minerModel, minerModel)
      )
    )
    .groupBy(historicalBitcoinCalculations.farmId);
    
  if (bitcoinData.length > 0) {
    // Get curtailment data for the same year
    const curtailmentSums = await db
      .select({
        farmId: curtailmentRecords.farmId,
        totalCurtailedEnergy: sql<number>`SUM(ABS(volume))`
      })
      .from(curtailmentRecords)
      .where(
        sql`DATE_TRUNC('year', settlement_date)::text LIKE ${year + '%'}`
      )
      .groupBy(curtailmentRecords.farmId);
      
    // Create a lookup for curtailment volumes
    const curtailmentLookup = new Map();
    for (const record of curtailmentSums) {
      curtailmentLookup.set(record.farmId, Number(record.totalCurtailedEnergy));
    }
    
    // Prepare data for insert
    const dataToInsert = bitcoinData.map(data => ({
      year,
      farmId: data.farmId,
      minerModel,
      totalCurtailedEnergy: String(curtailmentLookup.get(data.farmId) || 0),
      totalBitcoinMined: String(data.totalBitcoinMined),
      averageValue: null, // Will be calculated later if needed
    }));
    
    await db.insert(yearlyMiningPotential).values(dataToInsert);
    console.log(`Inserted ${dataToInsert.length} records into yearly_mining_potential for ${year} and ${minerModel}`);
  } else {
    console.log(`No Bitcoin calculation data found for ${year} and ${minerModel}`);
  }
}

/**
 * Refresh all materialized view tables for a specific date
 */
export async function refreshMaterializedViews(date: string): Promise<void> {
  if (MATERIALIZATION_IN_PROGRESS) {
    // Add to queue if already processing
    MATERIALIZATION_QUEUE.add(date);
    console.log(`Added ${date} to materialization queue`);
    return;
  }
  
  try {
    MATERIALIZATION_IN_PROGRESS = true;
    
    const minerModels = ['S19J_PRO', 'S9', 'M20S'];
    const year = date.substring(0, 4);
    
    // Process for each miner model
    for (const model of minerModels) {
      // Populate tables in order of granularity
      await populateSettlementPeriodMining(date, model);
      await populateDailyMiningPotential(date, model);
      await populateYearlyMiningPotential(year, model);
    }
    
    console.log(`Successfully refreshed materialized views for ${date}`);
  } catch (error) {
    console.error(`Error refreshing materialized views for ${date}:`, error);
  } finally {
    MATERIALIZATION_IN_PROGRESS = false;
    
    // Process next item in queue if any
    if (MATERIALIZATION_QUEUE.size > 0) {
      const nextDate = MATERIALIZATION_QUEUE.values().next().value;
      if (nextDate) {
        MATERIALIZATION_QUEUE.delete(nextDate);
        
        // Process next item asynchronously
        setTimeout(() => {
          refreshMaterializedViews(nextDate).catch(err => {
            console.error(`Error processing queued materialization for ${nextDate}:`, err);
          });
        }, 100);
      }
    }
  }
}

/**
 * Get daily mining potential data
 */
export async function getDailyMiningPotential(date: string, minerModel: string, farmId?: string): Promise<any> {
  // Try to get from materialized view first
  const query = db
    .select({
      summaryDate: dailyMiningPotential.summaryDate,
      farmId: dailyMiningPotential.farmId,
      totalCurtailedEnergy: dailyMiningPotential.totalCurtailedEnergy,
      totalBitcoinMined: dailyMiningPotential.totalBitcoinMined,
      averageValue: dailyMiningPotential.averageValue
    })
    .from(dailyMiningPotential)
    .where(
      and(
        eq(dailyMiningPotential.summaryDate, date),
        eq(dailyMiningPotential.minerModel, minerModel),
        farmId ? eq(dailyMiningPotential.farmId, farmId) : undefined
      )
    );
    
  const results = await query;
  
  if (results.length > 0) {
    // If looking for a specific farm
    if (farmId) {
      return results[0];
    }
    
    // Otherwise, aggregate all farms
    return results.reduce((acc, curr) => {
      return {
        summaryDate: curr.summaryDate,
        totalCurtailedEnergy: Number(acc.totalCurtailedEnergy || 0) + Number(curr.totalCurtailedEnergy || 0),
        totalBitcoinMined: Number(acc.totalBitcoinMined || 0) + Number(curr.totalBitcoinMined || 0),
        averageValue: Number(acc.averageValue || 0) + Number(curr.averageValue || 0)
      };
    }, { totalCurtailedEnergy: 0, totalBitcoinMined: 0, averageValue: 0 });
  }
  
  // If not found in materialized view, refresh the data and try calculation
  await refreshMaterializedViews(date);
  
  // Recalculate from original tables if still no data (fallback)
  // This could happen during initial population
  return await calculateDailyMiningPotential(date, minerModel, farmId);
}

/**
 * Calculate daily mining potential directly from original tables
 */
async function calculateDailyMiningPotential(date: string, minerModel: string, farmId?: string): Promise<any> {
  let query = db
    .select({
      totalBitcoinMined: sql<number>`SUM(bitcoin_mined)`
    })
    .from(historicalBitcoinCalculations)
    .where(
      and(
        eq(historicalBitcoinCalculations.settlementDate, date),
        eq(historicalBitcoinCalculations.minerModel, minerModel),
        farmId ? eq(historicalBitcoinCalculations.farmId, farmId) : undefined
      )
    );
    
  const bitcoinResults = await query;
  
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
  
  return {
    summaryDate: date,
    totalCurtailedEnergy: Number(curtailmentResults[0]?.totalCurtailedEnergy || 0),
    totalBitcoinMined: Number(bitcoinResults[0]?.totalBitcoinMined || 0),
    averageValue: null
  };
}

/**
 * Get yearly mining potential data
 */
export async function getYearlyMiningPotential(year: string, minerModel: string, farmId?: string): Promise<any> {
  // Try to get from materialized view first
  const query = db
    .select({
      year: yearlyMiningPotential.year,
      farmId: yearlyMiningPotential.farmId,
      totalCurtailedEnergy: yearlyMiningPotential.totalCurtailedEnergy,
      totalBitcoinMined: yearlyMiningPotential.totalBitcoinMined,
      averageValue: yearlyMiningPotential.averageValue
    })
    .from(yearlyMiningPotential)
    .where(
      and(
        eq(yearlyMiningPotential.year, year),
        eq(yearlyMiningPotential.minerModel, minerModel),
        farmId ? eq(yearlyMiningPotential.farmId, farmId) : undefined
      )
    );
    
  const results = await query;
  
  if (results.length > 0) {
    // If looking for a specific farm
    if (farmId) {
      return results[0];
    }
    
    // Otherwise, aggregate all farms
    return results.reduce((acc, curr) => {
      return {
        year: curr.year,
        totalCurtailedEnergy: Number(acc.totalCurtailedEnergy || 0) + Number(curr.totalCurtailedEnergy || 0),
        totalBitcoinMined: Number(acc.totalBitcoinMined || 0) + Number(curr.totalBitcoinMined || 0),
        averageValue: Number(acc.averageValue || 0) + Number(curr.averageValue || 0)
      };
    }, { totalCurtailedEnergy: 0, totalBitcoinMined: 0, averageValue: 0 });
  }
  
  // If not found in materialized view, calculate from daily summaries
  // This is a more complex year calculation that would require refreshing multiple days
  // So we'll just do a direct calculation
  return await calculateYearlyMiningPotential(year, minerModel, farmId);
}

/**
 * Calculate yearly mining potential directly from original tables
 */
async function calculateYearlyMiningPotential(year: string, minerModel: string, farmId?: string): Promise<any> {
  let query = db
    .select({
      totalBitcoinMined: sql<number>`SUM(bitcoin_mined)`
    })
    .from(historicalBitcoinCalculations)
    .where(
      and(
        sql`DATE_TRUNC('year', settlement_date)::text LIKE ${year + '%'}`,
        eq(historicalBitcoinCalculations.minerModel, minerModel),
        farmId ? eq(historicalBitcoinCalculations.farmId, farmId) : undefined
      )
    );
    
  const bitcoinResults = await query;
  
  let curtailmentQuery = db
    .select({
      totalCurtailedEnergy: sql<number>`SUM(ABS(volume))`
    })
    .from(curtailmentRecords)
    .where(
      and(
        sql`DATE_TRUNC('year', settlement_date)::text LIKE ${year + '%'}`,
        farmId ? eq(curtailmentRecords.farmId, farmId) : undefined
      )
    );
    
  const curtailmentResults = await curtailmentQuery;
  
  return {
    year,
    totalCurtailedEnergy: Number(curtailmentResults[0]?.totalCurtailedEnergy || 0),
    totalBitcoinMined: Number(bitcoinResults[0]?.totalBitcoinMined || 0),
    averageValue: null
  };
}