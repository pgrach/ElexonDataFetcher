import { Request, Response } from "express";
import { db } from "@db";
import { dailySummaries, curtailmentRecords, monthlySummaries } from "@db/schema";
import { eq, sql, and } from "drizzle-orm";

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
    const { date } = req.params;
    const { leadParty } = req.query;

    console.log('Received request for date:', date, 'leadParty:', leadParty);

    // Validate date format
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
      console.error('Invalid date format received:', date);
      return res.status(400).json({
        error: "Invalid date format. Please use YYYY-MM-DD"
      });
    }

    // Get the daily summary (aggregate only)
    const summary = await db.query.dailySummaries.findFirst({
      where: eq(dailySummaries.summaryDate, date)
    });

    console.log('Daily summary:', summary);

    // If no lead party filter, return data directly from daily_summaries
    if (!leadParty) {
      if (!summary) {
        return res.status(404).json({
          error: "No data available for this date"
        });
      }

      return res.json({
        date,
        totalCurtailedEnergy: Number(summary.totalCurtailedEnergy),
        totalPayment: Math.abs(Number(summary.totalPayment)),
        leadParty: null
      });
    }

    // For filtered requests, calculate from curtailment_records
    const recordTotals = await db
      .select({
        totalVolume: sql<string>`SUM(${curtailmentRecords.volume}::numeric)`,
        totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(
        and(
          eq(curtailmentRecords.settlementDate, date),
          eq(curtailmentRecords.leadPartyName, leadParty as string)
        )
      );

    if (!recordTotals[0] || !recordTotals[0].totalVolume) {
      return res.status(404).json({
        error: "No data available for this date and lead party"
      });
    }

    res.json({
      date,
      totalCurtailedEnergy: Number(recordTotals[0].totalVolume),
      totalPayment: Math.abs(Number(recordTotals[0].totalPayment)),
      leadParty
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
    const { leadParty } = req.query;

    // Validate date format
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
      return res.status(400).json({
        error: "Invalid date format. Please use YYYY-MM-DD"
      });
    }

    console.log(`Fetching hourly curtailment for date: ${date}, leadParty: ${leadParty || 'all'}`);

    // Calculate hourly totals directly in the query
    const hourlyTotals = await db
      .select({
        hour: sql<number>`FLOOR((${curtailmentRecords.settlementPeriod} - 1) / 2)`,
        curtailedEnergy: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`
      })
      .from(curtailmentRecords)
      .where(
        leadParty 
          ? and(
              eq(curtailmentRecords.settlementDate, date),
              eq(curtailmentRecords.leadPartyName, leadParty as string)
            )
          : eq(curtailmentRecords.settlementDate, date)
      )
      .groupBy(sql`FLOOR((${curtailmentRecords.settlementPeriod} - 1) / 2)`)
      .orderBy(sql`FLOOR((${curtailmentRecords.settlementPeriod} - 1) / 2)`);

    console.log('Raw hourly totals:', 
      hourlyTotals.map(r => ({
        hour: r.hour,
        volume: Number(r.curtailedEnergy).toFixed(2)
      }))
    );

    // Initialize 24-hour array with zeros
    const hourlyResults = Array.from({ length: 24 }, (_, i) => ({
      hour: `${i.toString().padStart(2, '0')}:00`,
      curtailedEnergy: 0
    }));

    // Map the calculated totals to our hourly results
    hourlyTotals.forEach(record => {
      if (record.hour !== null && record.curtailedEnergy) {
        const hour = Number(record.hour);
        if (hour >= 0 && hour < 24) {
          hourlyResults[hour].curtailedEnergy = Number(record.curtailedEnergy);
        }
      }
    });

    // For current day, zero out future hours
    const currentDate = new Date();
    const requestDate = new Date(date);
    if (
      requestDate.getFullYear() === currentDate.getFullYear() &&
      requestDate.getMonth() === currentDate.getMonth() &&
      requestDate.getDate() === currentDate.getDate()
    ) {
      const currentHour = currentDate.getHours();
      hourlyResults.forEach((result, index) => {
        if (index > currentHour) {
          result.curtailedEnergy = 0;
        }
      });
    }

    // Log final hourly totals for debugging
    console.log('Final hourly results:', 
      hourlyResults
        .filter(r => r.curtailedEnergy > 0)
        .map(r => `${r.hour}: ${r.curtailedEnergy.toFixed(2)} MWh`)
    );

    res.json(hourlyResults);
  } catch (error) {
    console.error('Error fetching hourly curtailment:', error);
    res.status(500).json({
      error: "Internal server error while fetching hourly data"
    });
  }
}