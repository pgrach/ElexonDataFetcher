import { Request, Response } from "express";
import { db } from "@db";
import { dailySummaries, curtailmentRecords, monthlySummaries } from "@db/schema";
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

export async function getHourlyCurtailment(req: Request, res: Response) {
  try {
    const { date } = req.params;

    // Validate date format
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
      return res.status(400).json({
        error: "Invalid date format. Please use YYYY-MM-DD"
      });
    }

    // Query to get hourly curtailment data
    // We need to combine adjacent settlement periods into hourly data
    const hourlyData = await db
      .select({
        hour: sql<number>`FLOOR((${curtailmentRecords.settlementPeriod} - 1) / 2)`,
        totalVolume: sql<string>`SUM(${curtailmentRecords.volume}::numeric)`,
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date))
      .groupBy(sql`FLOOR((${curtailmentRecords.settlementPeriod} - 1) / 2)`)
      .orderBy(sql`FLOOR((${curtailmentRecords.settlementPeriod} - 1) / 2)`);

    // Transform the data into a 24-hour format
    const hourlyResults = Array.from({ length: 24 }, (_, i) => ({
      hour: `${i.toString().padStart(2, '0')}:00`,
      curtailedEnergy: 0
    }));

    hourlyData.forEach(record => {
      if (record.hour >= 0 && record.hour < 24) {
        hourlyResults[record.hour].curtailedEnergy = Number(record.totalVolume) || 0;
      }
    });

    res.json(hourlyResults);
  } catch (error) {
    console.error('Error fetching hourly curtailment:', error);
    res.status(500).json({
      error: "Internal server error while fetching hourly data"
    });
  }
}