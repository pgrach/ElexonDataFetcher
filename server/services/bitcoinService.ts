/**
 * Bitcoin Service
 * 
 * This service handles Bitcoin-related calculations and data processing.
 * It encapsulates the business logic for Bitcoin mining potential and calculations.
 */

import { db } from "@db";
import { historicalBitcoinCalculations, curtailmentRecords, bitcoinMonthlySummaries, bitcoinDailySummaries, bitcoinYearlySummaries } from "@db/schema";
import { and, eq, sql, between, inArray, desc } from "drizzle-orm";
import { format, parseISO, startOfMonth, endOfMonth } from 'date-fns';
import { calculateBitcoin } from '../utils/bitcoin';
import { BitcoinCalculation } from '../types/bitcoin';
import { getDifficultyData } from './dynamodbService';

/**
 * Process Bitcoin calculations for a single day
 * 
 * @param date Date in YYYY-MM-DD format
 * @param minerModel Miner model to use for calculations
 * @returns Promise that resolves when processing is complete
 */
export async function processSingleDay(date: string, minerModel: string): Promise<void> {
  try {
    console.log(`Processing Bitcoin calculations for ${date} with ${minerModel}`);
    
    // Validate date format
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
      throw new Error("Invalid date format. Please use YYYY-MM-DD");
    }
    
    // Get the Bitcoin difficulty for this date (ideally from historical data)
    const difficultyData = await getDifficultyData(date);
    
    // Fetch curtailment records for the given date
    const curtailmentData = await db
      .select({
        date: curtailmentRecords.settlementDate,
        farmId: curtailmentRecords.farmId,
        leadPartyName: curtailmentRecords.leadPartyName,
        settlementPeriod: curtailmentRecords.settlementPeriod,
        volume: curtailmentRecords.volume,
        payment: curtailmentRecords.payment
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date))
      .orderBy(curtailmentRecords.settlementPeriod);
    
    // Group curtailment by lead party for aggregate calculations
    const leadPartyMap = new Map<string, {
      totalEnergy: number;
      records: typeof curtailmentData;
    }>();
    
    // Process all curtailment records
    for (const record of curtailmentData) {
      const energy = Math.abs(Number(record.volume));
      const leadParty = record.leadPartyName;
      
      if (!leadPartyMap.has(leadParty)) {
        leadPartyMap.set(leadParty, {
          totalEnergy: 0,
          records: []
        });
      }
      
      const partyData = leadPartyMap.get(leadParty)!;
      partyData.totalEnergy += energy;
      partyData.records.push(record);
    }
    
    // Process calculations for each lead party
    for (const [leadParty, data] of leadPartyMap.entries()) {
      // Calculate Bitcoin mined
      const bitcoinMined = calculateBitcoin({
        curtailedEnergy: data.totalEnergy,
        minerModel,
        difficulty: difficultyData.difficulty,
        proportional: true // Use energy proportional calculations
      });
      
      // Store the calculations
      await db.insert(historicalBitcoinCalculations).values({
        date,
        leadPartyName: leadParty,
        minerModel,
        curtailedEnergy: data.totalEnergy.toString(),
        bitcoinMined: bitcoinMined.toString(),
        difficulty: difficultyData.difficulty.toString(),
        createdAt: new Date()
      });
    }
    
    // Also calculate for "All" parties together
    const totalEnergy = Array.from(leadPartyMap.values())
      .reduce((sum, data) => sum + data.totalEnergy, 0);
    
    // Calculate and store the aggregate calculations
    if (totalEnergy > 0) {
      const bitcoinMined = calculateBitcoin({
        curtailedEnergy: totalEnergy,
        minerModel,
        difficulty: difficultyData.difficulty,
        proportional: true
      });
      
      await db.insert(historicalBitcoinCalculations).values({
        date,
        leadPartyName: 'All',
        minerModel,
        curtailedEnergy: totalEnergy.toString(),
        bitcoinMined: bitcoinMined.toString(),
        difficulty: difficultyData.difficulty.toString(),
        createdAt: new Date()
      });
    }
    
    // Update daily summaries
    await updateBitcoinDailySummary(date, minerModel);
    
    console.log(`Completed Bitcoin calculations for ${date} with ${minerModel}`);
  } catch (error) {
    console.error(`Error processing Bitcoin calculations for ${date}:`, error);
    throw error;
  }
}

/**
 * Update daily Bitcoin summary after processing daily calculations
 * 
 * @param date Date in YYYY-MM-DD format
 * @param minerModel Miner model string
 */
async function updateBitcoinDailySummary(date: string, minerModel: string): Promise<void> {
  try {
    // Get all calculations for this date and miner model
    const calculations = await db
      .select()
      .from(historicalBitcoinCalculations)
      .where(
        and(
          eq(historicalBitcoinCalculations.date, date),
          eq(historicalBitcoinCalculations.minerModel, minerModel)
        )
      );
    
    // Group by lead party for summary
    const leadPartyMap = new Map<string, {
      totalEnergy: number;
      bitcoinMined: number;
      difficulty: number;
    }>();
    
    for (const calc of calculations) {
      const leadParty = calc.leadPartyName;
      const energy = Number(calc.curtailedEnergy);
      const bitcoin = Number(calc.bitcoinMined);
      const difficulty = Number(calc.difficulty);
      
      if (!leadPartyMap.has(leadParty)) {
        leadPartyMap.set(leadParty, {
          totalEnergy: 0,
          bitcoinMined: 0,
          difficulty: 0
        });
      }
      
      const summary = leadPartyMap.get(leadParty)!;
      summary.totalEnergy += energy;
      summary.bitcoinMined += bitcoin;
      summary.difficulty = difficulty; // Last one will overwrite, but they should all be the same
    }
    
    // Store or update summaries for each lead party
    for (const [leadParty, summary] of leadPartyMap.entries()) {
      // Check if a summary already exists
      const existing = await db
        .select()
        .from(bitcoinDailySummaries)
        .where(
          and(
            eq(bitcoinDailySummaries.summaryDate, date),
            eq(bitcoinDailySummaries.minerModel, minerModel),
            eq(bitcoinDailySummaries.leadPartyName, leadParty)
          )
        )
        .limit(1);
      
      if (existing.length > 0) {
        // Update existing summary
        await db
          .update(bitcoinDailySummaries)
          .set({
            curtailedEnergy: summary.totalEnergy.toString(),
            bitcoinMined: summary.bitcoinMined.toString(),
            difficulty: summary.difficulty.toString(),
            updatedAt: new Date()
          })
          .where(eq(bitcoinDailySummaries.id, existing[0].id));
      } else {
        // Insert new summary
        await db
          .insert(bitcoinDailySummaries)
          .values({
            summaryDate: date,
            minerModel,
            leadPartyName: leadParty,
            curtailedEnergy: summary.totalEnergy.toString(),
            bitcoinMined: summary.bitcoinMined.toString(),
            difficulty: summary.difficulty.toString(),
            createdAt: new Date()
          });
      }
    }
    
    // Update the monthly summary after updating the daily summary
    const yearMonth = date.substring(0, 7); // Extract YYYY-MM from YYYY-MM-DD
    await updateMonthlyBitcoinSummary(yearMonth, minerModel);
  } catch (error) {
    console.error(`Error updating Bitcoin daily summary for ${date}:`, error);
    throw error;
  }
}

/**
 * Update monthly Bitcoin summary after processing daily calculations
 * 
 * @param yearMonth Month in YYYY-MM format
 * @param minerModel Miner model string
 */
async function updateMonthlyBitcoinSummary(yearMonth: string, minerModel: string): Promise<void> {
  try {
    // Parse year and month
    const [yearStr, monthStr] = yearMonth.split('-');
    const startDate = format(startOfMonth(new Date(parseInt(yearStr), parseInt(monthStr) - 1, 1)), 'yyyy-MM-dd');
    const endDate = format(endOfMonth(new Date(parseInt(yearStr), parseInt(monthStr) - 1, 1)), 'yyyy-MM-dd');
    
    // Get all daily summaries for this month and miner model
    const dailySummaries = await db
      .select()
      .from(bitcoinDailySummaries)
      .where(
        and(
          between(bitcoinDailySummaries.summaryDate, startDate, endDate),
          eq(bitcoinDailySummaries.minerModel, minerModel)
        )
      );
    
    // Group by lead party for monthly summary
    const leadPartyMap = new Map<string, {
      totalEnergy: number;
      bitcoinMined: number;
      difficultySum: number;
      count: number;
    }>();
    
    for (const summary of dailySummaries) {
      const leadParty = summary.leadPartyName;
      const energy = Number(summary.curtailedEnergy);
      const bitcoin = Number(summary.bitcoinMined);
      const difficulty = Number(summary.difficulty);
      
      if (!leadPartyMap.has(leadParty)) {
        leadPartyMap.set(leadParty, {
          totalEnergy: 0,
          bitcoinMined: 0,
          difficultySum: 0,
          count: 0
        });
      }
      
      const monthlySummary = leadPartyMap.get(leadParty)!;
      monthlySummary.totalEnergy += energy;
      monthlySummary.bitcoinMined += bitcoin;
      monthlySummary.difficultySum += difficulty;
      monthlySummary.count++;
    }
    
    // Store or update monthly summaries for each lead party
    for (const [leadParty, summary] of leadPartyMap.entries()) {
      const avgDifficulty = summary.count > 0 ? summary.difficultySum / summary.count : 0;
      
      // Check if a summary already exists
      const existing = await db
        .select()
        .from(bitcoinMonthlySummaries)
        .where(
          and(
            eq(bitcoinMonthlySummaries.yearMonth, yearMonth),
            eq(bitcoinMonthlySummaries.minerModel, minerModel),
            eq(bitcoinMonthlySummaries.leadPartyName, leadParty)
          )
        )
        .limit(1);
      
      if (existing.length > 0) {
        // Update existing summary
        await db
          .update(bitcoinMonthlySummaries)
          .set({
            curtailedEnergy: summary.totalEnergy.toString(),
            bitcoinMined: summary.bitcoinMined.toString(),
            difficulty: avgDifficulty.toString(),
            updatedAt: new Date()
          })
          .where(eq(bitcoinMonthlySummaries.id, existing[0].id));
      } else {
        // Insert new summary
        await db
          .insert(bitcoinMonthlySummaries)
          .values({
            yearMonth,
            minerModel,
            leadPartyName: leadParty,
            curtailedEnergy: summary.totalEnergy.toString(),
            bitcoinMined: summary.bitcoinMined.toString(),
            difficulty: avgDifficulty.toString(),
            createdAt: new Date()
          });
      }
    }
    
    // Update the yearly summary after updating the monthly summary
    const yearValue = yearMonth.substring(0, 4); // Extract YYYY from YYYY-MM
    await updateYearlyBitcoinSummary(yearValue, minerModel);
  } catch (error) {
    console.error(`Error updating Bitcoin monthly summary for ${yearMonth}:`, error);
    throw error;
  }
}

/**
 * Update yearly Bitcoin summary after processing monthly calculations
 * 
 * @param year Year in YYYY format
 * @param minerModel Miner model string
 */
async function updateYearlyBitcoinSummary(year: string, minerModel: string): Promise<void> {
  try {
    // Get all monthly summaries for this year and miner model
    const monthlySummaries = await db
      .select()
      .from(bitcoinMonthlySummaries)
      .where(
        and(
          sql`substring(${bitcoinMonthlySummaries.yearMonth}, 1, 4) = ${year}`,
          eq(bitcoinMonthlySummaries.minerModel, minerModel)
        )
      );
    
    // Group by lead party for yearly summary
    const leadPartyMap = new Map<string, {
      totalEnergy: number;
      bitcoinMined: number;
      difficultySum: number;
      count: number;
    }>();
    
    for (const summary of monthlySummaries) {
      const leadParty = summary.leadPartyName;
      const energy = Number(summary.curtailedEnergy);
      const bitcoin = Number(summary.bitcoinMined);
      const difficulty = Number(summary.difficulty);
      
      if (!leadPartyMap.has(leadParty)) {
        leadPartyMap.set(leadParty, {
          totalEnergy: 0,
          bitcoinMined: 0,
          difficultySum: 0,
          count: 0
        });
      }
      
      const yearlySummary = leadPartyMap.get(leadParty)!;
      yearlySummary.totalEnergy += energy;
      yearlySummary.bitcoinMined += bitcoin;
      yearlySummary.difficultySum += difficulty;
      yearlySummary.count++;
    }
    
    // Store or update yearly summaries for each lead party
    for (const [leadParty, summary] of leadPartyMap.entries()) {
      const avgDifficulty = summary.count > 0 ? summary.difficultySum / summary.count : 0;
      
      // Check if a summary already exists
      const existing = await db
        .select()
        .from(bitcoinYearlySummaries)
        .where(
          and(
            eq(bitcoinYearlySummaries.year, year),
            eq(bitcoinYearlySummaries.minerModel, minerModel),
            eq(bitcoinYearlySummaries.leadPartyName, leadParty)
          )
        )
        .limit(1);
      
      if (existing.length > 0) {
        // Update existing summary
        await db
          .update(bitcoinYearlySummaries)
          .set({
            curtailedEnergy: summary.totalEnergy.toString(),
            bitcoinMined: summary.bitcoinMined.toString(),
            difficulty: avgDifficulty.toString(),
            updatedAt: new Date()
          })
          .where(eq(bitcoinYearlySummaries.id, existing[0].id));
      } else {
        // Insert new summary
        await db
          .insert(bitcoinYearlySummaries)
          .values({
            year,
            minerModel,
            leadPartyName: leadParty,
            curtailedEnergy: summary.totalEnergy.toString(),
            bitcoinMined: summary.bitcoinMined.toString(),
            difficulty: avgDifficulty.toString(),
            createdAt: new Date()
          });
      }
    }
  } catch (error) {
    console.error(`Error updating Bitcoin yearly summary for ${year}:`, error);
    throw error;
  }
}

/**
 * Manual update method for yearly Bitcoin summary - used for reconciliation
 * 
 * @param year Year in YYYY format
 * @param minerModel Miner model string
 * @param leadParty Optional lead party to filter by
 */
export async function manualUpdateYearlyBitcoinSummary(
  year: string, 
  minerModel: string,
  leadParty?: string
): Promise<void> {
  try {
    await updateYearlyBitcoinSummary(year, minerModel);
  } catch (error) {
    console.error(`Error in manual update of yearly Bitcoin summary for ${year}:`, error);
    throw error;
  }
}

/**
 * Fetch difficulty data for 2024
 * This is used by scripts to backfill historical data
 */
export async function fetch2024Difficulties(): Promise<Record<string, number>> {
  try {
    // Placeholder - in production, fetch from DynamoDB or other source
    return {
      '2024-01': 67432054290484,
      '2024-02': 71724458780223,
      '2024-03': 75422283366266,
      '2024-04': 79824602321633
    };
  } catch (error) {
    console.error('Error fetching 2024 difficulties:', error);
    throw error;
  }
}

/**
 * Calculate monthly Bitcoin mining summary for a specific month
 * 
 * @param yearMonth Month in YYYY-MM format
 * @param minerModel Miner model string (e.g., S19J_PRO)
 * @param leadParty Optional lead party filter
 * @param currentDifficulty Current Bitcoin difficulty
 * @param currentPrice Current Bitcoin price in GBP
 * @returns Promise resolving to Bitcoin mining calculations for the month
 */
export async function calculateMonthlyBitcoinSummary(
  yearMonth: string,
  minerModel: string,
  leadParty: string | undefined,
  currentDifficulty: number,
  currentPrice: number
): Promise<{
  yearMonth: string;
  bitcoinMined: number;
  valueAtCurrentPrice: number;
  difficulty: number;
  currentPrice: number;
  dailyDetails?: Array<{
    date: string;
    bitcoinMined: number;
    valueAtCurrentPrice: number;
  }>;
}> {
  try {
    // Validate month format
    if (!/^\d{4}-\d{2}$/.test(yearMonth)) {
      throw new Error("Invalid month format. Please use YYYY-MM");
    }

    // First, check if we have a cached monthly summary
    const existingSummary = await db
      .select()
      .from(bitcoinMonthlySummaries)
      .where(
        and(
          eq(bitcoinMonthlySummaries.yearMonth, yearMonth),
          eq(bitcoinMonthlySummaries.minerModel, minerModel),
          leadParty 
            ? eq(bitcoinMonthlySummaries.leadPartyName, leadParty)
            : sql`1=1`
        )
      )
      .limit(1);

    if (existingSummary.length > 0) {
      const summary = existingSummary[0];
      
      return {
        yearMonth,
        bitcoinMined: Number(summary.bitcoinMined),
        valueAtCurrentPrice: Number(summary.bitcoinMined) * currentPrice,
        difficulty: Number(summary.difficulty),
        currentPrice
      };
    }

    // If no cached summary, calculate from historical calculations
    console.log(`Calculating monthly Bitcoin summary for ${yearMonth} and ${minerModel}...`);
    
    // Parse year and month
    const [yearPart, monthPart] = yearMonth.split('-');
    const startDate = format(startOfMonth(new Date(parseInt(yearPart), parseInt(monthPart) - 1, 1)), 'yyyy-MM-dd');
    const endDate = format(endOfMonth(new Date(parseInt(yearPart), parseInt(monthPart) - 1, 1)), 'yyyy-MM-dd');
    
    // Get historical Bitcoin calculations for the month
    const query = db
      .select()
      .from(historicalBitcoinCalculations)
      .where(
        and(
          between(historicalBitcoinCalculations.date, startDate, endDate),
          eq(historicalBitcoinCalculations.minerModel, minerModel),
          leadParty 
            ? eq(historicalBitcoinCalculations.leadPartyName, leadParty)
            : sql`1=1`
        )
      );
      
    const calculations = await query;
    
    if (calculations.length === 0) {
      console.log(`No Bitcoin data found for ${yearMonth} and ${minerModel}`);
      
      return {
        yearMonth,
        bitcoinMined: 0,
        valueAtCurrentPrice: 0,
        difficulty: 0,
        currentPrice
      };
    }
    
    // Sum up Bitcoin mined for the month
    const bitcoinMined = calculations.reduce(
      (total, record) => total + Number(record.bitcoinMined), 0
    );
    
    // Get the average difficulty used
    const difficultySum = calculations.reduce(
      (total, record) => total + Number(record.difficulty), 0
    );
    const averageDifficulty = difficultySum / calculations.length;
    
    return {
      yearMonth,
      bitcoinMined,
      valueAtCurrentPrice: bitcoinMined * currentPrice,
      difficulty: averageDifficulty,
      currentPrice,
      dailyDetails: calculations.map(calc => ({
        date: calc.date,
        bitcoinMined: Number(calc.bitcoinMined),
        valueAtCurrentPrice: Number(calc.bitcoinMined) * currentPrice
      }))
    };
  } catch (error) {
    console.error('Error calculating monthly Bitcoin summary:', error);
    throw error;
  }
}