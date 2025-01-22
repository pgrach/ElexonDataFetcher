import { Request, Response } from "express";
import { db } from "@db";
import { dailySummaries, curtailmentRecords, monthlySummaries, yearlySummaries } from "@db/schema";
import { eq, sql } from "drizzle-orm";

export async function getDailySummary(req: Request, res: Response) {
  try {
    const { date } = req.params;

    // Validate date format
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
      return res.status(400).json({
        error: "Invalid date format. Please use YYYY-MM-DD"
      });
    }

    // Calculate totals from curtailment_records for verification
    const recordTotals = await db
      .select({
        totalVolume: sql<string>`SUM(${curtailmentRecords.volume}::numeric)`,
        totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date));

    console.log('Curtailment records totals:', recordTotals[0]);

    // Get the daily summary
    const summary = await db.query.dailySummaries.findFirst({
      where: eq(dailySummaries.summaryDate, date)
    });

    console.log('Daily summary:', summary);

    if (!summary) {
      return res.status(404).json({
        error: "No data available for this date"
      });
    }

    res.json({
      date,
      totalCurtailedEnergy: Number(summary.totalCurtailedEnergy),
      totalPayment: Math.abs(Number(summary.totalPayment)), // Convert to positive number
      recordTotals: {
        totalVolume: Number(recordTotals[0]?.totalVolume || 0),
        totalPayment: Math.abs(Number(recordTotals[0]?.totalPayment || 0)) // Convert to positive number
      }
    });
  } catch (error) {
    console.error('Error fetching daily summary:', error);
    res.status(500).json({
      error: "Internal server error while fetching summary"
    });
  }
}

export async function getMonthlySummary(req: Request, res: Response) {
  try {
    const { yearMonth } = req.params;

    // Validate yearMonth format (YYYY-MM)
    if (!/^\d{4}-\d{2}$/.test(yearMonth)) {
      return res.status(400).json({
        error: "Invalid format. Please use YYYY-MM"
      });
    }

    // Get the monthly summary
    const summary = await db.query.monthlySummaries.findFirst({
      where: eq(monthlySummaries.yearMonth, yearMonth)
    });

    if (!summary) {
      return res.status(404).json({
        error: "No data available for this month"
      });
    }

    // Calculate totals from daily_summaries for verification
    const dailyTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(${dailySummaries.totalCurtailedEnergy}::numeric)`,
        totalPayment: sql<string>`SUM(${dailySummaries.totalPayment}::numeric)`
      })
      .from(dailySummaries)
      .where(sql`date_trunc('month', ${dailySummaries.summaryDate}::date) = date_trunc('month', ${yearMonth + '-01'}::date)`);

    res.json({
      yearMonth,
      totalCurtailedEnergy: Number(summary.totalCurtailedEnergy),
      totalPayment: Math.abs(Number(summary.totalPayment)), // Convert to positive number
      dailyTotals: {
        totalCurtailedEnergy: Number(dailyTotals[0]?.totalCurtailedEnergy || 0),
        totalPayment: Math.abs(Number(dailyTotals[0]?.totalPayment || 0)) // Convert to positive number
      }
    });
  } catch (error) {
    console.error('Error fetching monthly summary:', error);
    res.status(500).json({
      error: "Internal server error while fetching summary"
    });
  }
}

export async function getYearlySummary(req: Request, res: Response) {
  try {
    const { year } = req.params;

    // Validate year format (YYYY)
    if (!/^\d{4}$/.test(year)) {
      return res.status(400).json({
        error: "Invalid format. Please use YYYY"
      });
    }

    // Get the yearly summary
    const summary = await db.query.yearlySummaries.findFirst({
      where: eq(yearlySummaries.year, year)
    });

    if (!summary) {
      return res.status(404).json({
        error: "No data available for this year"
      });
    }

    // Calculate totals from monthly_summaries for verification
    const monthlyTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(${monthlySummaries.totalCurtailedEnergy}::numeric)`,
        totalPayment: sql<string>`SUM(${monthlySummaries.totalPayment}::numeric)`
      })
      .from(monthlySummaries)
      .where(sql`substring(${monthlySummaries.yearMonth}, 1, 4) = ${year}`);

    res.json({
      year,
      totalCurtailedEnergy: Number(summary.totalCurtailedEnergy),
      totalPayment: Math.abs(Number(summary.totalPayment)), // Convert to positive number
      monthlyTotals: {
        totalCurtailedEnergy: Number(monthlyTotals[0]?.totalCurtailedEnergy || 0),
        totalPayment: Math.abs(Number(monthlyTotals[0]?.totalPayment || 0)) // Convert to positive number
      }
    });
  } catch (error) {
    console.error('Error fetching yearly summary:', error);
    res.status(500).json({
      error: "Internal server error while fetching summary"
    });
  }
}