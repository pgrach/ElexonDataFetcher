import { Request, Response } from "express";
import { db } from "@db";
import { dailySummaries, curtailmentRecords, monthlySummaries } from "@db/schema";
import { eq, sql } from "drizzle-orm";

export async function getLeadParties(req: Request, res: Response) {
  try {
    const leadParties = await db
      .select({
        leadPartyName: curtailmentRecords.leadPartyName,
      })
      .from(curtailmentRecords)
      .groupBy(curtailmentRecords.leadPartyName)
      .orderBy(curtailmentRecords.leadPartyName);

    res.json(leadParties.map(party => party.leadPartyName));
  } catch (error) {
    console.error('Error fetching lead parties:', error);
    res.status(500).json({
      error: "Internal server error while fetching lead parties"
    });
  }
}

export async function getDailySummary(req: Request, res: Response) {
  try {
    const { date, leadParty } = req.query;

    // Validate date format
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date as string)) {
      return res.status(400).json({
        error: "Invalid date format. Please use YYYY-MM-DD"
      });
    }

    // Base query
    let query = db
      .select({
        totalVolume: sql<string>`SUM(${curtailmentRecords.volume}::numeric)`,
        totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date as string));

    // Add lead party filter if specified
    if (leadParty) {
      query = query.where(eq(curtailmentRecords.leadPartyName, leadParty as string));
    }

    const recordTotals = await query;
    console.log('Curtailment records totals:', recordTotals[0]);

    // Get the daily summary (aggregate only)
    const summary = await db.query.dailySummaries.findFirst({
      where: eq(dailySummaries.summaryDate, date as string)
    });

    console.log('Daily summary:', summary);

    if (!summary && !recordTotals[0]?.totalVolume) {
      return res.status(404).json({
        error: "No data available for this date"
      });
    }

    res.json({
      date,
      totalCurtailedEnergy: leadParty ?
        Number(recordTotals[0]?.totalVolume || 0) :
        Number(summary?.totalCurtailedEnergy || 0),
      totalPayment: leadParty ?
        Math.abs(Number(recordTotals[0]?.totalPayment || 0)) :
        Math.abs(Number(summary?.totalPayment || 0)),
      leadParty: leadParty || null
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
    const { leadParty } = req.query;

    // Validate yearMonth format (YYYY-MM)
    if (!/^\d{4}-\d{2}$/.test(yearMonth)) {
      return res.status(400).json({
        error: "Invalid format. Please use YYYY-MM"
      });
    }

    // Base query for curtailment records
    let recordsQuery = db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(${curtailmentRecords.volume}::numeric)`,
        totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(sql`date_trunc('month', ${curtailmentRecords.settlementDate}::date) = date_trunc('month', ${yearMonth + '-01'}::date)`);

    // Add lead party filter if specified
    if (leadParty) {
      recordsQuery = recordsQuery.where(eq(curtailmentRecords.leadPartyName, leadParty as string));
    }

    const recordTotals = await recordsQuery;

    if (!recordTotals[0]?.totalCurtailedEnergy) {
      return res.status(404).json({
        error: "No data available for this month"
      });
    }

    // Get the monthly summary for comparison (when not filtering by lead party)
    let summary = null;
    if (!leadParty) {
      summary = await db.query.monthlySummaries.findFirst({
        where: eq(monthlySummaries.yearMonth, yearMonth)
      });
    }

    res.json({
      yearMonth,
      totalCurtailedEnergy: Number(recordTotals[0].totalCurtailedEnergy),
      totalPayment: Math.abs(Number(recordTotals[0].totalPayment)),
      summary: summary ? {
        totalCurtailedEnergy: Number(summary.totalCurtailedEnergy),
        totalPayment: Math.abs(Number(summary.totalPayment))
      } : null,
      leadParty: leadParty || null
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