/**
 * Wind Generation Summary Service
 * 
 * This service manages updating daily, monthly, and yearly summaries with wind generation data.
 * It includes functions for calculating aggregated wind generation metrics and updating
 * the corresponding summary tables.
 */

import { db } from '../../db';
import { sql } from 'drizzle-orm';
import { eq } from 'drizzle-orm';
import { format, parse, startOfMonth, endOfMonth, getYear, isValid } from 'date-fns';
import { logger } from '../utils/logger';
import { dailySummaries, monthlySummaries, yearlySummaries } from '../../db/schema';

/**
 * Update daily summary with wind generation data
 * 
 * @param date - Date in YYYY-MM-DD format
 */
export async function updateDailySummary(date: string): Promise<void> {
  try {
    logger.info(`Updating daily summary with wind generation data for ${date}`, {
      module: 'windSummaryService'
    });

    // First check if we have wind generation data for this date
    // Explicitly cast the input date to date type to ensure proper comparison
    const windGenDataCheck = await db.execute(sql`
      SELECT COUNT(*) as count
      FROM wind_generation_data
      WHERE settlement_date = ${date}::date
    `);

    // Get count from the first row of the result
    const recordCount = parseInt(windGenDataCheck.rows[0]?.count as string || '0', 10);
    
    if (recordCount === 0) {
      logger.warning(`No wind generation data found for ${date}, skipping daily summary update`, {
        module: 'windSummaryService'
      });
      return;
    }

    // Calculate aggregated wind generation values
    const windGenAggregates = await db.execute(sql`
      SELECT 
        SUM(total_wind) as total_wind_generation,
        SUM(wind_onshore) as wind_onshore_generation,
        SUM(wind_offshore) as wind_offshore_generation
      FROM wind_generation_data
      WHERE settlement_date = ${date}::date
    `);

    // Get generation values from the first row of the result
    const totalWindGeneration = parseFloat(windGenAggregates.rows[0]?.total_wind_generation as string || '0');
    const windOnshoreGeneration = parseFloat(windGenAggregates.rows[0]?.wind_onshore_generation as string || '0');
    const windOffshoreGeneration = parseFloat(windGenAggregates.rows[0]?.wind_offshore_generation as string || '0');

    // Check if daily summary exists for this date
    const existingSummary = await db
      .select()
      .from(dailySummaries)
      .where(eq(dailySummaries.summaryDate, date));

    if (existingSummary.length > 0) {
      // Update existing summary
      await db
        .update(dailySummaries)
        .set({
          totalWindGeneration: totalWindGeneration.toString(),
          windOnshoreGeneration: windOnshoreGeneration.toString(),
          windOffshoreGeneration: windOffshoreGeneration.toString(),
          lastUpdated: new Date()
        })
        .where(eq(dailySummaries.summaryDate, date));

      logger.info(`Updated daily summary with wind generation data for ${date}`, {
        module: 'windSummaryService',
        context: {
          totalWindGeneration,
          windOnshoreGeneration,
          windOffshoreGeneration
        }
      });
    } else {
      // Create new summary with 0 values for curtailment data (will be updated separately)
      await db
        .insert(dailySummaries)
        .values({
          summaryDate: date,
          totalCurtailedEnergy: '0',
          totalPayment: '0',
          totalWindGeneration: totalWindGeneration.toString(),
          windOnshoreGeneration: windOnshoreGeneration.toString(),
          windOffshoreGeneration: windOffshoreGeneration.toString(),
          lastUpdated: new Date()
        });

      logger.info(`Created new daily summary with wind generation data for ${date}`, {
        module: 'windSummaryService',
        context: {
          totalWindGeneration,
          windOnshoreGeneration,
          windOffshoreGeneration
        }
      });
    }
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    logger.error(`Error updating daily summary with wind generation data for ${date}: ${errorMessage}`, {
      module: 'windSummaryService'
    });
    throw error;
  }
}

/**
 * Update monthly summary with wind generation data
 * 
 * @param yearMonth - Month in YYYY-MM format
 */
export async function updateMonthlySummary(yearMonth: string): Promise<void> {
  try {
    logger.info(`Updating monthly summary with wind generation data for ${yearMonth}`, {
      module: 'windSummaryService'
    });

    // Validate format
    if (!/^\d{4}-\d{2}$/.test(yearMonth)) {
      throw new Error(`Invalid year-month format: ${yearMonth}. Expected YYYY-MM.`);
    }

    // Calculate the date range for the month
    const monthDate = parse(yearMonth, 'yyyy-MM', new Date());
    if (!isValid(monthDate)) {
      throw new Error(`Invalid date from year-month: ${yearMonth}`);
    }

    const startDate = format(startOfMonth(monthDate), 'yyyy-MM-dd');
    const endDate = format(endOfMonth(monthDate), 'yyyy-MM-dd');

    // First check if we have wind generation data for this month
    const windGenDataCheck = await db.execute(sql`
      SELECT COUNT(*) as count
      FROM wind_generation_data
      WHERE settlement_date >= ${startDate}::date AND settlement_date <= ${endDate}::date
    `);

    const recordCount = parseInt(windGenDataCheck.rows[0]?.count as string || '0', 10);
    
    if (recordCount === 0) {
      logger.warning(`No wind generation data found for ${yearMonth}, skipping monthly summary update`, {
        module: 'windSummaryService'
      });
      return;
    }

    // Calculate aggregated wind generation values
    const windGenAggregates = await db.execute(sql`
      SELECT 
        SUM(total_wind) as total_wind_generation,
        SUM(wind_onshore) as wind_onshore_generation,
        SUM(wind_offshore) as wind_offshore_generation
      FROM wind_generation_data
      WHERE settlement_date >= ${startDate}::date AND settlement_date <= ${endDate}::date
    `);

    const totalWindGeneration = parseFloat(windGenAggregates.rows[0]?.total_wind_generation as string || '0');
    const windOnshoreGeneration = parseFloat(windGenAggregates.rows[0]?.wind_onshore_generation as string || '0');
    const windOffshoreGeneration = parseFloat(windGenAggregates.rows[0]?.wind_offshore_generation as string || '0');

    // Check if monthly summary exists for this month
    const existingSummary = await db
      .select()
      .from(monthlySummaries)
      .where(eq(monthlySummaries.yearMonth, yearMonth));

    if (existingSummary.length > 0) {
      // Update existing summary
      await db
        .update(monthlySummaries)
        .set({
          totalWindGeneration: totalWindGeneration.toString(),
          windOnshoreGeneration: windOnshoreGeneration.toString(),
          windOffshoreGeneration: windOffshoreGeneration.toString(),
          lastUpdated: new Date()
        })
        .where(eq(monthlySummaries.yearMonth, yearMonth));

      logger.info(`Updated monthly summary with wind generation data for ${yearMonth}`, {
        module: 'windSummaryService',
        context: {
          totalWindGeneration,
          windOnshoreGeneration,
          windOffshoreGeneration
        }
      });
    } else {
      // Create new summary with 0 values for curtailment data (will be updated separately)
      await db
        .insert(monthlySummaries)
        .values({
          yearMonth: yearMonth,
          totalCurtailedEnergy: '0',
          totalPayment: '0',
          totalWindGeneration: totalWindGeneration.toString(),
          windOnshoreGeneration: windOnshoreGeneration.toString(),
          windOffshoreGeneration: windOffshoreGeneration.toString(),
          lastUpdated: new Date()
        });

      logger.info(`Created new monthly summary with wind generation data for ${yearMonth}`, {
        module: 'windSummaryService',
        context: {
          totalWindGeneration,
          windOnshoreGeneration,
          windOffshoreGeneration
        }
      });
    }
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    logger.error(`Error updating monthly summary with wind generation data for ${yearMonth}: ${errorMessage}`, {
      module: 'windSummaryService'
    });
    throw error;
  }
}

/**
 * Update yearly summary with wind generation data
 * 
 * @param year - Year in YYYY format
 */
export async function updateYearlySummary(year: string): Promise<void> {
  try {
    logger.info(`Updating yearly summary with wind generation data for ${year}`, {
      module: 'windSummaryService'
    });

    // Validate format
    if (!/^\d{4}$/.test(year)) {
      throw new Error(`Invalid year format: ${year}. Expected YYYY.`);
    }

    // Calculate date range for the year
    const startDate = `${year}-01-01`;
    const endDate = `${year}-12-31`;

    // First check if we have wind generation data for this year
    const windGenDataCheck = await db.execute(sql`
      SELECT COUNT(*) as count
      FROM wind_generation_data
      WHERE settlement_date >= ${startDate}::date AND settlement_date <= ${endDate}::date
    `);

    const recordCount = parseInt(windGenDataCheck.rows[0]?.count as string || '0', 10);
    
    if (recordCount === 0) {
      logger.warning(`No wind generation data found for ${year}, skipping yearly summary update`, {
        module: 'windSummaryService'
      });
      return;
    }

    // Calculate aggregated wind generation values
    const windGenAggregates = await db.execute(sql`
      SELECT 
        SUM(total_wind) as total_wind_generation,
        SUM(wind_onshore) as wind_onshore_generation,
        SUM(wind_offshore) as wind_offshore_generation
      FROM wind_generation_data
      WHERE settlement_date >= ${startDate}::date AND settlement_date <= ${endDate}::date
    `);

    const totalWindGeneration = parseFloat(windGenAggregates.rows[0]?.total_wind_generation as string || '0');
    const windOnshoreGeneration = parseFloat(windGenAggregates.rows[0]?.wind_onshore_generation as string || '0');
    const windOffshoreGeneration = parseFloat(windGenAggregates.rows[0]?.wind_offshore_generation as string || '0');

    // Check if yearly summary exists for this year
    const existingSummary = await db
      .select()
      .from(yearlySummaries)
      .where(eq(yearlySummaries.year, year));

    if (existingSummary.length > 0) {
      // Update existing summary
      await db
        .update(yearlySummaries)
        .set({
          totalWindGeneration: totalWindGeneration.toString(),
          windOnshoreGeneration: windOnshoreGeneration.toString(),
          windOffshoreGeneration: windOffshoreGeneration.toString(),
          lastUpdated: new Date()
        })
        .where(eq(yearlySummaries.year, year));

      logger.info(`Updated yearly summary with wind generation data for ${year}`, {
        module: 'windSummaryService',
        context: {
          totalWindGeneration,
          windOnshoreGeneration,
          windOffshoreGeneration
        }
      });
    } else {
      // Create new summary with 0 values for curtailment data (will be updated separately)
      await db
        .insert(yearlySummaries)
        .values({
          year: year,
          totalCurtailedEnergy: '0',
          totalPayment: '0',
          totalWindGeneration: totalWindGeneration.toString(),
          windOnshoreGeneration: windOnshoreGeneration.toString(),
          windOffshoreGeneration: windOffshoreGeneration.toString(),
          lastUpdated: new Date()
        });

      logger.info(`Created new yearly summary with wind generation data for ${year}`, {
        module: 'windSummaryService',
        context: {
          totalWindGeneration,
          windOnshoreGeneration,
          windOffshoreGeneration
        }
      });
    }
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    logger.error(`Error updating yearly summary with wind generation data for ${year}: ${errorMessage}`, {
      module: 'windSummaryService'
    });
    throw error;
  }
}

/**
 * Process daily wind generation summaries for a specific date
 * This will update daily, monthly, and yearly summaries as needed
 * 
 * @param date - Date to process in YYYY-MM-DD format
 */
export async function processDailySummaries(date: string): Promise<void> {
  try {
    // Update daily summary
    await updateDailySummary(date);

    // Extract year and month for related summaries
    const dateObj = new Date(date);
    const yearMonth = format(dateObj, 'yyyy-MM');
    const year = format(dateObj, 'yyyy');

    // Update related monthly and yearly summaries
    await updateMonthlySummary(yearMonth);
    await updateYearlySummary(year);

    logger.info(`Processed all wind generation summaries for ${date}`, {
      module: 'windSummaryService'
    });
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    logger.error(`Error processing wind generation summaries for ${date}: ${errorMessage}`, {
      module: 'windSummaryService'
    });
    throw error;
  }
}

/**
 * Recalculate all summaries from existing wind generation data
 * This is useful for a full reprocessing after data fixes or schema changes
 */
export async function recalculateAllSummaries(): Promise<void> {
  try {
    logger.info('Starting recalculation of all wind generation summaries', {
      module: 'windSummaryService'
    });

    // Get all unique dates with wind generation data
    const datesResult = await db.execute(sql`
      SELECT DISTINCT settlement_date::text as date
      FROM wind_generation_data
      ORDER BY date
    `);
    
    // Extract dates from result rows
    const dates: string[] = [];
    if (datesResult && datesResult.rows) {
      for (const row of datesResult.rows) {
        if (row.date) {
          dates.push(row.date);
        }
      }
    }

    // Get all unique year-months with wind generation data
    const yearMonthsResult = await db.execute(sql`
      SELECT DISTINCT TO_CHAR(settlement_date, 'YYYY-MM') as year_month
      FROM wind_generation_data
      ORDER BY year_month
    `);
    
    // Extract year-months from result rows
    const yearMonths: string[] = [];
    if (yearMonthsResult && yearMonthsResult.rows) {
      for (const row of yearMonthsResult.rows) {
        if (row.year_month) {
          yearMonths.push(row.year_month);
        }
      }
    }

    // Get all unique years with wind generation data
    const yearsResult = await db.execute(sql`
      SELECT DISTINCT TO_CHAR(settlement_date, 'YYYY') as year
      FROM wind_generation_data
      ORDER BY year
    `);
    
    // Extract years from result rows
    const years: string[] = [];
    if (yearsResult && yearsResult.rows) {
      for (const row of yearsResult.rows) {
        if (row.year) {
          years.push(row.year);
        }
      }
    }

    logger.info(`Found ${dates.length} dates, ${yearMonths.length} months, and ${years.length} years to process`, {
      module: 'windSummaryService'
    });

    // Update all daily summaries
    for (let i = 0; i < dates.length; i++) {
      const date = dates[i];
      logger.info(`Processing daily summary for ${date} (${i+1}/${dates.length})`, {
        module: 'windSummaryService'
      });
      await updateDailySummary(date);
    }

    // Update all monthly summaries
    for (let i = 0; i < yearMonths.length; i++) {
      const yearMonth = yearMonths[i];
      logger.info(`Processing monthly summary for ${yearMonth} (${i+1}/${yearMonths.length})`, {
        module: 'windSummaryService'
      });
      await updateMonthlySummary(yearMonth);
    }

    // Update all yearly summaries
    for (let i = 0; i < years.length; i++) {
      const year = years[i];
      logger.info(`Processing yearly summary for ${year} (${i+1}/${years.length})`, {
        module: 'windSummaryService'
      });
      await updateYearlySummary(year);
    }

    logger.info('Completed recalculation of all wind generation summaries', {
      module: 'windSummaryService'
    });
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    logger.error(`Error recalculating wind generation summaries: ${errorMessage}`, {
      module: 'windSummaryService'
    });
    throw error;
  }
}