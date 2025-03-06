/**
 * Deduplication Utilities for Bitcoin Mining Analytics platform
 * 
 * This module provides functions to identify and remove duplicate records
 * from the curtailment_records table while preserving data integrity.
 */

import { db } from "../../db";
import { curtailmentRecords, dailySummaries, monthlySummaries, yearlySummaries } from "../../db/schema";
import { eq, sql, count, and } from "drizzle-orm";
import { logger } from "./logger";
import { sql as drizzleSql } from "drizzle-orm";

/**
 * Interface representing duplicate record groups
 */
interface DuplicateGroup {
  settlementDate: string;
  settlementPeriod: number;
  farmId: string;
  count: number;
  totalVolume: string;
  recordIds: number[];
}

/**
 * Interface for deduplication results
 */
interface DeduplicationResult {
  duplicateGroups: number;
  recordsRemoved: number;
  volumeReduced: number;
  paymentReduced: number;
}

/**
 * Helper to safely parse SQL query results
 */
function parseQueryResult<T>(result: any, defaultValue: T): T {
  if (!result || !Array.isArray(result) || result.length === 0) {
    return defaultValue;
  }
  return result[0] as unknown as T;
}

/**
 * Find duplicate records for a specific date
 */
export async function findDuplicateRecords(date: string): Promise<DuplicateGroup[]> {
  try {
    logger.info(`Finding duplicate records for date ${date}`);
    // First identify groups with duplicates
    const duplicateGroups = await db.execute(sql`
      WITH duplicate_groups AS (
        SELECT 
          settlement_date, 
          settlement_period, 
          farm_id, 
          COUNT(*) as record_count,
          SUM(ABS(volume::numeric)) as total_volume
        FROM curtailment_records
        WHERE settlement_date = ${date}
        GROUP BY settlement_date, settlement_period, farm_id
        HAVING COUNT(*) > 1
      )
      SELECT 
        d.settlement_date as "settlementDate",
        d.settlement_period as "settlementPeriod",
        d.farm_id as "farmId",
        d.record_count as "count",
        d.total_volume as "totalVolume",
        ARRAY_AGG(c.id) as "recordIds"
      FROM duplicate_groups d
      JOIN curtailment_records c ON 
        d.settlement_date = c.settlement_date AND
        d.settlement_period = c.settlement_period AND
        d.farm_id = c.farm_id
      GROUP BY d.settlement_date, d.settlement_period, d.farm_id, d.record_count, d.total_volume
      ORDER BY d.record_count DESC, d.settlement_period
    `);

    logger.info(`Found ${duplicateGroups?.length || 0} duplicate groups for date ${date}`);
    
    // Handle case where result might not be an array
    if (!duplicateGroups || !Array.isArray(duplicateGroups)) {
      logger.warning(`Unexpected result format for duplicate groups query: ${typeof duplicateGroups}`);
      return [];
    }
    
    return duplicateGroups as unknown as DuplicateGroup[];
  } catch (error: unknown) {
    logger.error(`Error finding duplicate records for date ${date}`, { 
      error: error instanceof Error ? error : new Error(String(error)) 
    });
    return [];
  }
}

/**
 * Get statistics about duplicate records for a date
 */
export async function getDuplicateStatistics(date: string): Promise<{
  totalDuplicateGroups: number;
  totalDuplicateRecords: number;
  totalDuplicateVolume: number;
}> {
  const result = await db.execute(sql`
    WITH duplicate_groups AS (
      SELECT 
        settlement_date, 
        settlement_period, 
        farm_id, 
        COUNT(*) as record_count,
        SUM(ABS(volume::numeric)) as total_volume
      FROM curtailment_records
      WHERE settlement_date = ${date}
      GROUP BY settlement_date, settlement_period, farm_id
      HAVING COUNT(*) > 1
    )
    SELECT 
      COUNT(*) as "totalDuplicateGroups",
      SUM(record_count - 1) as "totalDuplicateRecords",
      SUM(total_volume * (record_count - 1) / record_count) as "totalDuplicateVolume"
    FROM duplicate_groups
  `);

  interface StatResult {
    totalDuplicateGroups: string;
    totalDuplicateRecords: string;
    totalDuplicateVolume: string;
  }

  const defaultStats: StatResult = {
    totalDuplicateGroups: "0",
    totalDuplicateRecords: "0",
    totalDuplicateVolume: "0"
  };

  const stats = parseQueryResult<StatResult>(result, defaultStats);

  return {
    totalDuplicateGroups: parseInt(stats.totalDuplicateGroups || '0'),
    totalDuplicateRecords: parseInt(stats.totalDuplicateRecords || '0'),
    totalDuplicateVolume: parseFloat(stats.totalDuplicateVolume || '0')
  };
}

/**
 * Deduplicate records for a specific date while preserving one record per group
 */
export async function deduplicateRecords(date: string): Promise<DeduplicationResult> {
  try {
    logger.info(`Starting deduplication for date ${date}`);
    
    // Get current totals before deduplication
    const beforeTotals = await db.execute(sql`
      SELECT 
        SUM(ABS(volume::numeric)) as total_volume,
        SUM(payment::numeric) as total_payment,
        COUNT(*) as record_count
      FROM curtailment_records
      WHERE settlement_date = ${date}
    `);
    
    interface TotalResult {
      total_volume: string;
      total_payment: string;
      record_count: string;
    }

    const defaultTotals: TotalResult = {
      total_volume: "0",
      total_payment: "0",
      record_count: "0"
    };
    
    const beforeData = parseQueryResult<TotalResult>(beforeTotals, defaultTotals);
    
    const beforeVolume = parseFloat(beforeData.total_volume || '0');
    const beforePayment = parseFloat(beforeData.total_payment || '0');
    const beforeCount = parseInt(beforeData.record_count || '0');
    
    // Get duplicate groups
    const duplicateGroups = await findDuplicateRecords(date);
    logger.info(`Found ${duplicateGroups.length} duplicate groups for ${date}`);
    
    // For each group, keep the first record and delete others
    let recordsRemoved = 0;
    
    for (const group of duplicateGroups) {
      // Keep the first record, delete the rest
      const keepId = group.recordIds[0];
      const deleteIds = group.recordIds.slice(1);
      
      // Delete duplicate records
      const deleteResult = await db.delete(curtailmentRecords)
        .where(sql`id = ANY(${deleteIds})`)
        .returning();
      
      recordsRemoved += deleteResult.length;
      logger.info(`Removed ${deleteResult.length} duplicate records for ${group.farmId} in period ${group.settlementPeriod}`);
    }
    
    // Get new totals after deduplication
    const afterTotals = await db.execute(sql`
      SELECT 
        SUM(ABS(volume::numeric)) as total_volume,
        SUM(payment::numeric) as total_payment,
        COUNT(*) as record_count
      FROM curtailment_records
      WHERE settlement_date = ${date}
    `);
    
    const afterData = parseQueryResult<TotalResult>(afterTotals, defaultTotals);
    
    const afterVolume = parseFloat(afterData.total_volume || '0');
    const afterPayment = parseFloat(afterData.total_payment || '0');
    const afterCount = parseInt(afterData.record_count || '0');
    
    const volumeReduced = beforeVolume - afterVolume;
    const paymentReduced = beforePayment - afterPayment;
    
    // Update the daily summary to reflect the new totals
    await db.update(dailySummaries)
      .set({
        totalCurtailedEnergy: afterVolume.toString(),
        totalPayment: afterPayment.toString()
      })
      .where(eq(dailySummaries.summaryDate, date));
    
    logger.info(`Updated daily summary for ${date}`);
    
    // Update monthly and yearly summaries
    const yearMonth = date.substring(0, 7);
    const year = date.substring(0, 4);
    
    // Update monthly summary
    const monthlyTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(${dailySummaries.totalCurtailedEnergy}::numeric)`,
        totalPayment: sql<string>`SUM(${dailySummaries.totalPayment}::numeric)`
      })
      .from(dailySummaries)
      .where(sql`date_trunc('month', ${dailySummaries.summaryDate}::date) = date_trunc('month', ${date}::date)`);
    
    if (monthlyTotals && monthlyTotals.length > 0 && monthlyTotals[0].totalCurtailedEnergy && monthlyTotals[0].totalPayment) {
      await db.update(monthlySummaries)
        .set({
          totalCurtailedEnergy: monthlyTotals[0].totalCurtailedEnergy,
          totalPayment: monthlyTotals[0].totalPayment
        })
        .where(eq(monthlySummaries.yearMonth, yearMonth));
      
      logger.info(`Updated monthly summary for ${yearMonth}`);
    }
    
    // Update yearly summary
    const yearlyTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(${dailySummaries.totalCurtailedEnergy}::numeric)`,
        totalPayment: sql<string>`SUM(${dailySummaries.totalPayment}::numeric)`
      })
      .from(dailySummaries)
      .where(sql`date_trunc('year', ${dailySummaries.summaryDate}::date) = date_trunc('year', ${date}::date)`);
    
    if (yearlyTotals && yearlyTotals.length > 0 && yearlyTotals[0].totalCurtailedEnergy && yearlyTotals[0].totalPayment) {
      await db.update(yearlySummaries)
        .set({
          totalCurtailedEnergy: yearlyTotals[0].totalCurtailedEnergy,
          totalPayment: yearlyTotals[0].totalPayment
        })
        .where(eq(yearlySummaries.year, year));
      
      logger.info(`Updated yearly summary for ${year}`);
    }
    
    return {
      duplicateGroups: duplicateGroups.length,
      recordsRemoved,
      volumeReduced,
      paymentReduced
    };
  } catch (error: unknown) {
    logger.error(`Error during deduplication for date ${date}`, { 
      error: error instanceof Error ? error : new Error(String(error)) 
    });
    throw error;
  }
}

/**
 * Preview deduplication without making any changes
 */
export async function previewDeduplication(date: string): Promise<{
  beforeVolume: number;
  afterVolume: number;
  beforePayment: number;
  afterPayment: number;
  duplicateGroups: number;
  recordsToRemove: number;
  volumeToReduce: number;
  paymentToReduce: number;
}> {
  // Get current totals
  const beforeTotals = await db.execute(sql`
    SELECT 
      SUM(ABS(volume::numeric)) as total_volume,
      SUM(payment::numeric) as total_payment,
      COUNT(*) as record_count
    FROM curtailment_records
    WHERE settlement_date = ${date}
  `);
  
  interface TotalResult {
    total_volume: string;
    total_payment: string;
    record_count: string;
  }

  const defaultTotals: TotalResult = {
    total_volume: "0",
    total_payment: "0",
    record_count: "0"
  };
  
  const beforeData = parseQueryResult<TotalResult>(beforeTotals, defaultTotals);
  
  const beforeVolume = parseFloat(beforeData.total_volume || '0');
  const beforePayment = parseFloat(beforeData.total_payment || '0');
  const beforeCount = parseInt(beforeData.record_count || '0');
  
  // Get statistics about what would be removed
  const stats = await getDuplicateStatistics(date);
  
  // Calculate what the totals would be after deduplication
  const afterVolume = beforeVolume - stats.totalDuplicateVolume;
  const duplicatePaymentRatio = stats.totalDuplicateVolume / beforeVolume;
  const paymentToReduce = beforePayment * duplicatePaymentRatio;
  const afterPayment = beforePayment - paymentToReduce;
  
  return {
    beforeVolume,
    afterVolume,
    beforePayment,
    afterPayment,
    duplicateGroups: stats.totalDuplicateGroups,
    recordsToRemove: stats.totalDuplicateRecords,
    volumeToReduce: stats.totalDuplicateVolume,
    paymentToReduce
  };
}

/**
 * Check if a date needs deduplication
 */
export async function needsDeduplication(date: string): Promise<boolean> {
  const result = await db.execute(sql`
    SELECT EXISTS (
      SELECT 1
      FROM (
        SELECT 
          settlement_date, 
          settlement_period, 
          farm_id, 
          COUNT(*) as count
        FROM curtailment_records
        WHERE settlement_date = ${date}
        GROUP BY settlement_date, settlement_period, farm_id
        HAVING COUNT(*) > 1
        LIMIT 1
      ) as duplicates
    ) as has_duplicates
  `);
  
  interface ExistsResult {
    has_duplicates: boolean;
  }
  
  const parsedResult = parseQueryResult<ExistsResult>(result, { has_duplicates: false });
  return parsedResult.has_duplicates;
}