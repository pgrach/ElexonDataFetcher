/**
 * Summary Service
 * 
 * Responsible for fetching and processing summary data from various sources.
 * This service handles business logic for summary-related operations.
 */

import { db } from "@db";
import { dailySummaries, monthlySummaries, yearlySummaries, curtailmentRecords } from "@db/schema";
import { eq, sql, and, desc, count } from "drizzle-orm";

/**
 * Get all lead parties in the system
 * 
 * @returns Promise resolving to array of lead party names
 */
export async function getAllLeadParties(): Promise<string[]> {
  const leadParties = await db
    .select({
      leadPartyName: curtailmentRecords.leadPartyName,
    })
    .from(curtailmentRecords)
    .groupBy(curtailmentRecords.leadPartyName)
    .orderBy(curtailmentRecords.leadPartyName);

  // Filter out any null values and cast to string[]
  return leadParties
    .map(party => party.leadPartyName)
    .filter((name): name is string => name !== null);
}

/**
 * Get lead parties that had curtailment on a specific date
 * 
 * @param date Date in YYYY-MM-DD format
 * @returns Promise resolving to array of lead party names
 */
export async function getLeadPartiesForDate(date: string): Promise<string[]> {
  // Validate date format
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
    throw new Error("Invalid date format. Please use YYYY-MM-DD");
  }

  const leadParties = await db
    .select({
      leadPartyName: curtailmentRecords.leadPartyName,
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date))
    .groupBy(curtailmentRecords.leadPartyName)
    .orderBy(curtailmentRecords.leadPartyName);

  // Filter out any null values and cast to string[]
  return leadParties
    .map(party => party.leadPartyName)
    .filter((name): name is string => name !== null);
}

/**
 * Get daily summary for a specific date
 * 
 * @param date Date in YYYY-MM-DD format
 * @returns Promise resolving to daily summary or null if not found
 */
export async function getDailySummary(date: string): Promise<{
  date: string;
  totalCurtailedEnergy: number;
  totalPayment: number;
} | null> {
  // Validate date format
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
    throw new Error("Invalid date format. Please use YYYY-MM-DD");
  }

  const summary = await db
    .select({
      summaryDate: dailySummaries.summaryDate,
      totalCurtailedEnergy: dailySummaries.totalCurtailedEnergy,
      totalPayment: dailySummaries.totalPayment
    })
    .from(dailySummaries)
    .where(eq(dailySummaries.summaryDate, date))
    .limit(1);

  if (summary.length === 0) {
    return null;
  }

  return {
    date: summary[0].summaryDate,
    totalCurtailedEnergy: Number(summary[0].totalCurtailedEnergy),
    totalPayment: Number(summary[0].totalPayment)
  };
}

/**
 * Get monthly summary for a specific month
 * 
 * @param yearMonth Month in YYYY-MM format
 * @returns Promise resolving to monthly summary or null if not found
 */
export async function getMonthlySummary(yearMonth: string): Promise<{
  yearMonth: string;
  totalCurtailedEnergy: number;
  totalPayment: number;
} | null> {
  // Validate month format
  if (!/^\d{4}-\d{2}$/.test(yearMonth)) {
    throw new Error("Invalid month format. Please use YYYY-MM");
  }

  const summary = await db
    .select({
      yearMonth: monthlySummaries.yearMonth,
      totalCurtailedEnergy: monthlySummaries.totalCurtailedEnergy,
      totalPayment: monthlySummaries.totalPayment
    })
    .from(monthlySummaries)
    .where(eq(monthlySummaries.yearMonth, yearMonth))
    .limit(1);

  if (summary.length === 0) {
    return null;
  }

  return {
    yearMonth: summary[0].yearMonth,
    totalCurtailedEnergy: Number(summary[0].totalCurtailedEnergy),
    totalPayment: Number(summary[0].totalPayment)
  };
}

/**
 * Get yearly summary for a specific year
 * 
 * @param year Year in YYYY format
 * @returns Promise resolving to yearly summary or null if not found
 */
export async function getYearlySummary(year: string): Promise<{
  year: string;
  totalCurtailedEnergy: number;
  totalPayment: number;
} | null> {
  // Validate year format
  if (!/^\d{4}$/.test(year)) {
    throw new Error("Invalid year format. Please use YYYY");
  }

  const summary = await db
    .select({
      year: yearlySummaries.year,
      totalCurtailedEnergy: yearlySummaries.totalCurtailedEnergy,
      totalPayment: yearlySummaries.totalPayment
    })
    .from(yearlySummaries)
    .where(eq(yearlySummaries.year, year))
    .limit(1);

  if (summary.length === 0) {
    return null;
  }

  return {
    year: summary[0].year,
    totalCurtailedEnergy: Number(summary[0].totalCurtailedEnergy),
    totalPayment: Number(summary[0].totalPayment)
  };
}

/**
 * Get hourly curtailment data for a specific date
 * 
 * @param date Date in YYYY-MM-DD format
 * @returns Promise resolving to hourly curtailment data
 */
export async function getHourlyCurtailment(date: string): Promise<any[]> {
  // Hourly curtailment query that groups records by settlement period
  const hourlyCurtailment = await db
    .select({
      hour: sql<number>`FLOOR((${curtailmentRecords.settlementPeriod} - 1) / 2)`,
      totalVolume: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
      totalPayment: sql<string>`SUM(ABS(${curtailmentRecords.payment}::numeric))`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date))
    .groupBy(sql`FLOOR((${curtailmentRecords.settlementPeriod} - 1) / 2)`)
    .orderBy(sql`FLOOR((${curtailmentRecords.settlementPeriod} - 1) / 2)`);

  // Transform and format the results
  return hourlyCurtailment.map(record => ({
    hour: record.hour,
    label: `${record.hour}:00 - ${record.hour + 1}:00`,
    curtailedEnergy: Number(record.totalVolume),
    payment: Number(record.totalPayment)
  }));
}

/**
 * Get hourly comparison data for a specific date
 * Compares curtailment with wind generation data
 * 
 * @param date Date in YYYY-MM-DD format
 * @returns Promise resolving to hourly comparison data
 */
export async function getHourlyComparison(date: string): Promise<any[]> {
  // Hourly curtailment data grouped by period/hour
  const curtailmentByHour = await db
    .select({
      hour: sql<number>`FLOOR((${curtailmentRecords.settlementPeriod} - 1) / 2)`,
      curtailedEnergy: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date))
    .groupBy(sql`FLOOR((${curtailmentRecords.settlementPeriod} - 1) / 2)`)
    .orderBy(sql`FLOOR((${curtailmentRecords.settlementPeriod} - 1) / 2)`);

  // Transform the data for the chart
  const hours = Array.from({ length: 24 }, (_, i) => i);
  
  return hours.map(hour => {
    const curtailmentData = curtailmentByHour.find(c => c.hour === hour);
    
    return {
      hour,
      label: `${hour}:00`,
      curtailedEnergy: curtailmentData ? Number(curtailmentData.curtailedEnergy) : 0
    };
  });
}

/**
 * Get monthly comparison data for a specific year-month
 * 
 * @param yearMonth Year-month in YYYY-MM format
 * @returns Promise resolving to monthly comparison data
 */
export async function getMonthlyComparison(yearMonth: string): Promise<any[]> {
  // Extract year from yearMonth
  const year = yearMonth.substring(0, 4);
  
  // Get all monthly summaries for the year
  const monthlySummaryData = await db
    .select({
      yearMonth: monthlySummaries.yearMonth,
      totalCurtailedEnergy: monthlySummaries.totalCurtailedEnergy,
      totalPayment: monthlySummaries.totalPayment
    })
    .from(monthlySummaries)
    .where(sql`${monthlySummaries.yearMonth} LIKE ${year + '-%'}`)
    .orderBy(monthlySummaries.yearMonth);

  // Transform data for chart display
  const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sept', 'Oct', 'Nov', 'Dec'];
  
  return months.map((month, index) => {
    const monthNum = String(index + 1).padStart(2, '0');
    const currentYearMonth = `${year}-${monthNum}`;
    const monthData = monthlySummaryData.find(m => m.yearMonth === currentYearMonth);
    
    return {
      month,
      curtailedEnergy: monthData ? Number(monthData.totalCurtailedEnergy) : 0,
      payment: monthData ? Number(monthData.totalPayment) : 0,
      isSelected: currentYearMonth === yearMonth
    };
  });
}

/**
 * Get the most recent date with curtailment data 
 * 
 * @returns Promise resolving to the most recent date with curtailment data or null if no data exists
 */
export async function getMostRecentDateWithData(): Promise<string | null> {
  const result = await db
    .select({
      mostRecentDate: curtailmentRecords.settlementDate
    })
    .from(curtailmentRecords)
    .orderBy(desc(curtailmentRecords.settlementDate))
    .limit(1);

  return result.length > 0 ? result[0].mostRecentDate : null;
}