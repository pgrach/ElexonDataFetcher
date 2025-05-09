/**
 * Summary Service
 * 
 * Responsible for fetching and processing summary data from various sources.
 * This service handles business logic for summary-related operations.
 */

import { db } from "@db";
import { dailySummaries, monthlySummaries, yearlySummaries, curtailmentRecords } from "@db/schema";
import { eq, sql, and, desc } from "drizzle-orm";

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

  return leadParties.map(party => party.leadPartyName);
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

  return leadParties.map(party => party.leadPartyName);
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