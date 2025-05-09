/**
 * Enhanced Curtailment Service
 * 
 * This is an improved version of the curtailment service that includes additional
 * validation and fixes for the payment calculation discrepancy.
 */

import { db } from "@db";
import { curtailmentRecords, dailySummaries, monthlySummaries, yearlySummaries } from "@db/schema";
import { fetchBidsOffers } from "./elexon";
import { eq, sql } from "drizzle-orm";
import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from 'url';
import { logger } from '../utils/logger';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const BMU_MAPPING_PATH = path.join(__dirname, "../data/bmuMapping.json");

let windFarmBmuIds: Set<string> | null = null;
let bmuLeadPartyMap: Map<string, string> | null = null;

/**
 * Load wind farm BMU IDs and lead party names from the mapping file
 */
async function loadWindFarmIds(): Promise<Set<string>> {
  try {
    if (process.env.NODE_ENV === 'development' || windFarmBmuIds === null || bmuLeadPartyMap === null) {
      logger.info('Loading BMU mapping from: ' + BMU_MAPPING_PATH);
      const mappingContent = await fs.readFile(BMU_MAPPING_PATH, 'utf8');
      const bmuMapping = JSON.parse(mappingContent);
      logger.info(`Loaded ${bmuMapping.length} BMU mappings`);

      windFarmBmuIds = new Set(
        bmuMapping
          .filter((bmu: any) => bmu.fuelType === "WIND")
          .map((bmu: any) => bmu.elexonBmUnit)
      );

      bmuLeadPartyMap = new Map(
        bmuMapping
          .filter((bmu: any) => bmu.fuelType === "WIND")
          .map((bmu: any) => [bmu.elexonBmUnit, bmu.leadPartyName || 'Unknown'])
      );

      logger.info(`Found ${windFarmBmuIds.size} wind farm BMUs`);
    }

    if (!windFarmBmuIds || !bmuLeadPartyMap) {
      throw new Error('Failed to initialize BMU mappings');
    }

    return windFarmBmuIds;
  } catch (error) {
    logger.error('Error loading BMU mapping:', error);
    throw error;
  }
}

/**
 * Process daily curtailment data for a specified date
 * This enhanced version ensures payment calculations are accurate and includes better logging
 */
export async function processDailyCurtailment(date: string): Promise<void> {
  const BATCH_SIZE = 12;
  const validWindFarmIds = await loadWindFarmIds();
  let totalVolume = 0;
  let totalPayment = 0;
  let recordsProcessed = 0;

  logger.info(`Processing curtailment for ${date}`);

  // Clear existing records for the date to prevent partial updates
  await db.delete(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date));

  // Create an array to store all inserted record IDs for verification
  const insertedRecordIds: string[] = [];

  // Process all 48 periods in batches
  for (let startPeriod = 1; startPeriod <= 48; startPeriod += BATCH_SIZE) {
    const endPeriod = Math.min(startPeriod + BATCH_SIZE - 1, 48);
    const periodPromises = [];

    for (let period = startPeriod; period <= endPeriod; period++) {
      periodPromises.push((async () => {
        try {
          const records = await fetchBidsOffers(date, period);
          const validRecords = records.filter(record =>
            record.volume < 0 &&
            (record.soFlag || record.cadlFlag) &&
            validWindFarmIds.has(record.id)
          );

          if (validRecords.length > 0) {
            logger.info(`[${date} P${period}] Processing ${validRecords.length} records`);
          }

          const periodResults = await Promise.all(
            validRecords.map(async record => {
              const volume = Math.abs(record.volume);
              const payment = volume * record.originalPrice;

              try {
                const result = await db.insert(curtailmentRecords).values({
                  settlementDate: date,
                  settlementPeriod: period,
                  farmId: record.id,
                  leadPartyName: bmuLeadPartyMap?.get(record.id) || 'Unknown',
                  volume: record.volume.toString(), // Keep the original negative value
                  payment: payment.toString(),
                  originalPrice: record.originalPrice.toString(),
                  finalPrice: record.finalPrice.toString(),
                  soFlag: record.soFlag,
                  cadlFlag: record.cadlFlag
                }).returning({ id: curtailmentRecords.id });

                if (result && result[0]) {
                  insertedRecordIds.push(result[0].id);
                  recordsProcessed++;
                }

                logger.info(`[${date} P${period}] Added record for ${record.id}: ${volume} MWh, £${payment}`);
                return { volume, payment };
              } catch (error) {
                logger.error(`[${date} P${period}] Error inserting record for ${record.id}:`, error);
                return { volume: 0, payment: 0 };
              }
            })
          );

          const periodTotal = periodResults.reduce(
            (acc, curr) => ({
              volume: acc.volume + curr.volume,
              payment: acc.payment + curr.payment
            }),
            { volume: 0, payment: 0 }
          );

          if (periodTotal.volume > 0) {
            logger.info(`[${date} P${period}] Total: ${periodTotal.volume.toFixed(2)} MWh, £${periodTotal.payment.toFixed(2)}`);
          }

          return periodTotal;
        } catch (error) {
          logger.error(`Error processing period ${period} for date ${date}:`, error);
          return { volume: 0, payment: 0 };
        }
      })());
    }

    const batchResults = await Promise.all(periodPromises);
    for (const result of batchResults) {
      totalVolume += result.volume;
      totalPayment += result.payment;
    }
  }

  try {
    logger.info(`=== Summary for ${date} ===`);
    logger.info(`Records processed: ${recordsProcessed}`);
    logger.info(`Total volume: ${totalVolume.toFixed(2)} MWh`);
    logger.info(`Total payment: £${totalPayment.toFixed(2)}`);

    // Double-check the totals directly from the database to ensure accuracy
    const dbTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(ABS(${curtailmentRecords.volume})::numeric)`,
        totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date));

    if (dbTotals[0] && dbTotals[0].totalCurtailedEnergy && dbTotals[0].totalPayment) {
      const dbEnergy = parseFloat(dbTotals[0].totalCurtailedEnergy);
      const dbPayment = parseFloat(dbTotals[0].totalPayment);

      logger.info(`Database calculated totals:`);
      logger.info(`- Energy: ${dbEnergy.toFixed(2)} MWh`);
      logger.info(`- Payment: £${dbPayment.toFixed(2)}`);

      // Use the database calculated totals for the daily summary update to ensure accuracy
      totalVolume = dbEnergy;
      totalPayment = dbPayment;

      // Compare in-memory totals with database totals as a sanity check
      const energyDiff = Math.abs(totalVolume - dbEnergy);
      const paymentDiff = Math.abs(totalPayment - dbPayment);

      if (energyDiff > 0.01 || paymentDiff > 0.01) {
        logger.warning(`Difference detected between calculated and database totals:`);
        logger.warning(`- Energy diff: ${energyDiff.toFixed(2)} MWh`);
        logger.warning(`- Payment diff: £${paymentDiff.toFixed(2)}`);
      }
    }

    // Update daily summary
    await db.insert(dailySummaries).values({
      summaryDate: date,
      totalCurtailedEnergy: totalVolume.toString(),
      totalPayment: totalPayment.toString()
    }).onConflictDoUpdate({
      target: [dailySummaries.summaryDate],
      set: {
        totalCurtailedEnergy: totalVolume.toString(),
        totalPayment: totalPayment.toString()
      }
    });

    // Verify the daily summary was updated correctly
    const updatedSummary = await db
      .select()
      .from(dailySummaries)
      .where(eq(dailySummaries.summaryDate, date));

    if (updatedSummary[0]) {
      logger.info(`Daily summary updated successfully:`);
      logger.info(`- Energy: ${updatedSummary[0].totalCurtailedEnergy} MWh`);
      logger.info(`- Payment: £${updatedSummary[0].totalPayment}`);
    }

    // Update monthly summary
    const yearMonth = date.substring(0, 7);
    const monthlyTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(${dailySummaries.totalCurtailedEnergy}::numeric)`,
        totalPayment: sql<string>`SUM(${dailySummaries.totalPayment}::numeric)`
      })
      .from(dailySummaries)
      .where(sql`date_trunc('month', ${dailySummaries.summaryDate}::date) = date_trunc('month', ${date}::date)`);

    if (monthlyTotals[0].totalCurtailedEnergy && monthlyTotals[0].totalPayment) {
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

      logger.info(`Monthly summary updated for ${yearMonth}:`);
      logger.info(`- Energy: ${monthlyTotals[0].totalCurtailedEnergy} MWh`);
      logger.info(`- Payment: £${monthlyTotals[0].totalPayment}`);
    }

    // Update yearly summary
    const year = date.substring(0, 4);
    const yearlyTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(${dailySummaries.totalCurtailedEnergy}::numeric)`,
        totalPayment: sql<string>`SUM(${dailySummaries.totalPayment}::numeric)`
      })
      .from(dailySummaries)
      .where(sql`date_trunc('year', ${dailySummaries.summaryDate}::date) = date_trunc('year', ${date}::date)`);

    if (yearlyTotals[0].totalCurtailedEnergy && yearlyTotals[0].totalPayment) {
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

      logger.info(`Yearly summary updated for ${year}:`);
      logger.info(`- Energy: ${yearlyTotals[0].totalCurtailedEnergy} MWh`);
      logger.info(`- Payment: £${yearlyTotals[0].totalPayment}`);
    }

    logger.info(`Successfully processed data for ${date}`);
  } catch (error) {
    logger.error(`Error updating summaries for ${date}:`, error);
    throw error;
  }
}

/**
 * Get aggregated curtailment data for a specific date
 */
export async function getDailyCurtailment(date: string): Promise<any> {
  try {
    // First check if we have a daily summary
    const dailySummary = await db
      .select()
      .from(dailySummaries)
      .where(eq(dailySummaries.summaryDate, date));

    if (dailySummary.length > 0) {
      return {
        date,
        totalCurtailedEnergy: parseFloat(dailySummary[0].totalCurtailedEnergy),
        totalPayment: Math.abs(parseFloat(dailySummary[0].totalPayment)), // Convert to positive for display
        breakdown: await getPeriodBreakdown(date)
      };
    }

    // If no summary, calculate directly
    const totals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(ABS(${curtailmentRecords.volume})::numeric)`,
        totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date));

    return {
      date,
      totalCurtailedEnergy: totals[0]?.totalCurtailedEnergy ? parseFloat(totals[0].totalCurtailedEnergy) : 0,
      totalPayment: totals[0]?.totalPayment ? Math.abs(parseFloat(totals[0].totalPayment)) : 0, // Convert to positive for display
      breakdown: await getPeriodBreakdown(date)
    };
  } catch (error) {
    logger.error(`Error retrieving curtailment data for ${date}:`, error);
    throw error;
  }
}

/**
 * Get hourly breakdown of curtailment data for a specific date
 */
async function getPeriodBreakdown(date: string): Promise<any[]> {
  try {
    const periodTotals = await db
      .select({
        period: curtailmentRecords.settlementPeriod,
        volume: sql<string>`SUM(ABS(${curtailmentRecords.volume})::numeric)`,
        payment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date))
      .groupBy(curtailmentRecords.settlementPeriod)
      .orderBy(curtailmentRecords.settlementPeriod);

    return periodTotals.map(period => ({
      period: period.period,
      curtailedEnergy: parseFloat(period.volume),
      payment: Math.abs(parseFloat(period.payment)) // Convert to positive for display
    }));
  } catch (error) {
    logger.error(`Error retrieving period breakdown for ${date}:`, error);
    return [];
  }
}

/**
 * Get farm-level breakdown of curtailment data for a specific date
 */
export async function getFarmBreakdown(date: string): Promise<any[]> {
  try {
    const farmTotals = await db
      .select({
        farmId: curtailmentRecords.farmId,
        leadPartyName: curtailmentRecords.leadPartyName,
        volume: sql<string>`SUM(ABS(${curtailmentRecords.volume})::numeric)`,
        payment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date))
      .groupBy(curtailmentRecords.farmId, curtailmentRecords.leadPartyName)
      .orderBy(sql<string>`SUM(ABS(${curtailmentRecords.volume})::numeric)`, 'desc');

    return farmTotals.map(farm => ({
      farmId: farm.farmId,
      leadPartyName: farm.leadPartyName,
      curtailedEnergy: parseFloat(farm.volume),
      payment: Math.abs(parseFloat(farm.payment)) // Convert to positive for display
    }));
  } catch (error) {
    logger.error(`Error retrieving farm breakdown for ${date}:`, error);
    return [];
  }
}

/**
 * Get monthly curtailment data
 */
export async function getMonthlyCurtailment(yearMonth: string): Promise<any> {
  try {
    // First check if we have a monthly summary
    const monthlySummary = await db
      .select()
      .from(monthlySummaries)
      .where(eq(monthlySummaries.yearMonth, yearMonth));

    if (monthlySummary.length > 0) {
      return {
        yearMonth,
        totalCurtailedEnergy: parseFloat(monthlySummary[0].totalCurtailedEnergy),
        totalPayment: Math.abs(parseFloat(monthlySummary[0].totalPayment)), // Convert to positive for display
        breakdown: await getMonthlyDailyBreakdown(yearMonth)
      };
    }

    // If no summary, calculate from daily summaries
    const dailyTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(${dailySummaries.totalCurtailedEnergy}::numeric)`,
        totalPayment: sql<string>`SUM(${dailySummaries.totalPayment}::numeric)`
      })
      .from(dailySummaries)
      .where(sql`date_trunc('month', ${dailySummaries.summaryDate}::date) = date_trunc('month', ${yearMonth + '-01'}::date)`);

    return {
      yearMonth,
      totalCurtailedEnergy: dailyTotals[0]?.totalCurtailedEnergy ? parseFloat(dailyTotals[0].totalCurtailedEnergy) : 0,
      totalPayment: dailyTotals[0]?.totalPayment ? Math.abs(parseFloat(dailyTotals[0].totalPayment)) : 0, // Convert to positive for display
      breakdown: await getMonthlyDailyBreakdown(yearMonth)
    };
  } catch (error) {
    logger.error(`Error retrieving monthly curtailment data for ${yearMonth}:`, error);
    throw error;
  }
}

/**
 * Get daily breakdown for a specific month
 */
async function getMonthlyDailyBreakdown(yearMonth: string): Promise<any[]> {
  try {
    const dailyTotals = await db
      .select()
      .from(dailySummaries)
      .where(sql`date_trunc('month', ${dailySummaries.summaryDate}::date) = date_trunc('month', ${yearMonth + '-01'}::date)`)
      .orderBy(dailySummaries.summaryDate);

    return dailyTotals.map(day => ({
      date: day.summaryDate,
      curtailedEnergy: parseFloat(day.totalCurtailedEnergy),
      payment: Math.abs(parseFloat(day.totalPayment)) // Convert to positive for display
    }));
  } catch (error) {
    logger.error(`Error retrieving daily breakdown for ${yearMonth}:`, error);
    return [];
  }
}

/**
 * Get yearly curtailment data
 */
export async function getYearlyCurtailment(year: string): Promise<any> {
  try {
    // First check if we have a yearly summary
    const yearlySummary = await db
      .select()
      .from(yearlySummaries)
      .where(eq(yearlySummaries.year, year));

    if (yearlySummary.length > 0) {
      return {
        year,
        totalCurtailedEnergy: parseFloat(yearlySummary[0].totalCurtailedEnergy),
        totalPayment: Math.abs(parseFloat(yearlySummary[0].totalPayment)), // Convert to positive for display
        breakdown: await getYearlyMonthlyBreakdown(year)
      };
    }

    // If no summary, calculate from daily summaries
    const dailyTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(${dailySummaries.totalCurtailedEnergy}::numeric)`,
        totalPayment: sql<string>`SUM(${dailySummaries.totalPayment}::numeric)`
      })
      .from(dailySummaries)
      .where(sql`date_trunc('year', ${dailySummaries.summaryDate}::date) = date_trunc('year', ${year + '-01-01'}::date)`);

    return {
      year,
      totalCurtailedEnergy: dailyTotals[0]?.totalCurtailedEnergy ? parseFloat(dailyTotals[0].totalCurtailedEnergy) : 0,
      totalPayment: dailyTotals[0]?.totalPayment ? Math.abs(parseFloat(dailyTotals[0].totalPayment)) : 0, // Convert to positive for display
      breakdown: await getYearlyMonthlyBreakdown(year)
    };
  } catch (error) {
    logger.error(`Error retrieving yearly curtailment data for ${year}:`, error);
    throw error;
  }
}

/**
 * Get monthly breakdown for a specific year
 */
async function getYearlyMonthlyBreakdown(year: string): Promise<any[]> {
  try {
    // Use monthly summaries if available
    const monthlyTotals = await db
      .select()
      .from(monthlySummaries)
      .where(sql`${monthlySummaries.yearMonth} LIKE ${year + '-%'}`)
      .orderBy(monthlySummaries.yearMonth);

    if (monthlyTotals.length > 0) {
      return monthlyTotals.map(month => ({
        month: month.yearMonth,
        curtailedEnergy: parseFloat(month.totalCurtailedEnergy),
        payment: Math.abs(parseFloat(month.totalPayment)) // Convert to positive for display
      }));
    }

    // Otherwise compute from daily summaries
    const aggregatedMonths = await db
      .select({
        month: sql<string>`to_char(${dailySummaries.summaryDate}::date, 'YYYY-MM')`,
        curtailedEnergy: sql<string>`SUM(${dailySummaries.totalCurtailedEnergy}::numeric)`,
        payment: sql<string>`SUM(${dailySummaries.totalPayment}::numeric)`
      })
      .from(dailySummaries)
      .where(sql`date_trunc('year', ${dailySummaries.summaryDate}::date) = date_trunc('year', ${year + '-01-01'}::date)`)
      .groupBy(sql`to_char(${dailySummaries.summaryDate}::date, 'YYYY-MM')`)
      .orderBy(sql`to_char(${dailySummaries.summaryDate}::date, 'YYYY-MM')`);

    return aggregatedMonths.map(month => ({
      month: month.month,
      curtailedEnergy: parseFloat(month.curtailedEnergy),
      payment: Math.abs(parseFloat(month.payment)) // Convert to positive for display
    }));
  } catch (error) {
    logger.error(`Error retrieving monthly breakdown for ${year}:`, error);
    return [];
  }
}

/**
 * Verify the data integrity for a specific date
 */
export async function verifyDataIntegrity(date: string): Promise<{ 
  isConsistent: boolean, 
  recordCount: number,
  dbEnergy: number,
  dbPayment: number,
  summaryEnergy: number,
  summaryPayment: number
}> {
  try {
    // Get totals directly from curtailment_records
    const dbTotals = await db
      .select({
        recordCount: sql<string>`COUNT(*)`,
        totalCurtailedEnergy: sql<string>`SUM(ABS(${curtailmentRecords.volume})::numeric)`,
        totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date));

    // Get values from daily_summaries
    const summary = await db
      .select()
      .from(dailySummaries)
      .where(eq(dailySummaries.summaryDate, date));

    const dbEnergy = dbTotals[0]?.totalCurtailedEnergy ? parseFloat(dbTotals[0].totalCurtailedEnergy) : 0;
    const dbPayment = dbTotals[0]?.totalPayment ? parseFloat(dbTotals[0].totalPayment) : 0;
    
    let summaryEnergy = 0;
    let summaryPayment = 0;
    
    if (summary.length > 0) {
      summaryEnergy = parseFloat(summary[0].totalCurtailedEnergy);
      summaryPayment = parseFloat(summary[0].totalPayment);
    }

    // Check if values are consistent (within 0.01 tolerance)
    const isEnergyConsistent = Math.abs(dbEnergy - summaryEnergy) < 0.01;
    const isPaymentConsistent = Math.abs(dbPayment - summaryPayment) < 0.01;
    const isConsistent = isEnergyConsistent && isPaymentConsistent;

    return {
      isConsistent,
      recordCount: parseInt(dbTotals[0]?.recordCount as string || '0'),
      dbEnergy,
      dbPayment,
      summaryEnergy,
      summaryPayment
    };
  } catch (error) {
    logger.error(`Error verifying data integrity for ${date}:`, error);
    throw error;
  }
}