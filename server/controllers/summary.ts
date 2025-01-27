import { Request, Response } from "express";
import { db } from "@db";
import { dailySummaries, curtailmentRecords, monthlySummaries } from "@db/schema";
import { eq, sql, and, desc } from "drizzle-orm";

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

    // Validate date format
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
      return res.status(400).json({
        error: "Invalid date format. Please use YYYY-MM-DD"
      });
    }

    // Get the daily summary (aggregate only)
    const summary = await db.query.dailySummaries.findFirst({
      where: eq(dailySummaries.summaryDate, date)
    });

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
      totalCurtailedEnergy: Math.abs(Number(recordTotals[0].totalVolume)),
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
      totalPayment: Math.abs(Number(summary.totalPayment)),
      dailyTotals: {
        totalCurtailedEnergy: Number(dailyTotals[0]?.totalCurtailedEnergy || 0),
        totalPayment: Math.abs(Number(dailyTotals[0]?.totalPayment || 0))
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

    // Initialize 24-hour array with zeros
    const hourlyResults = Array.from({ length: 24 }, (_, i) => ({
      hour: `${i.toString().padStart(2, '0')}:00`,
      curtailedEnergy: 0
    }));

    // Get raw records for the date to calculate hourly totals
    const rawRecords = await db
      .select({
        settlementPeriod: curtailmentRecords.settlementPeriod,
        volume: curtailmentRecords.volume,
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
      .orderBy(curtailmentRecords.settlementPeriod);

    // Process each record and sum up the volumes per hour
    for (const record of rawRecords) {
      if (record.settlementPeriod && record.volume) {
        const period = Number(record.settlementPeriod);
        const hour = Math.floor((period - 1) / 2);  // Periods 1-2 -> hour 0, 3-4 -> hour 1, etc.

        if (hour >= 0 && hour < 24) {
          // Convert the volume to positive number (curtailment is stored as negative)
          const volume = Math.abs(Number(record.volume));
          hourlyResults[hour].curtailedEnergy += volume;

          if (hour === 0) {
            console.log(`Period ${period}: Adding ${volume} MWh`);
          }
        }
      }
    }

    // For current day, zero out future hours
    const currentDate = new Date();
    const requestDate = new Date(date);

    if (
      requestDate.getFullYear() === currentDate.getFullYear() &&
      requestDate.getMonth() === currentDate.getMonth() &&
      requestDate.getDate() === currentDate.getDate()
    ) {
      const currentHour = currentDate.getHours();
      for (let i = currentHour + 1; i < 24; i++) {
        hourlyResults[i].curtailedEnergy = 0;
      }
    }

    // Round values for consistency
    hourlyResults.forEach(result => {
      result.curtailedEnergy = Number(result.curtailedEnergy.toFixed(2));
    });

    console.log(`Hour 0 total: ${hourlyResults[0].curtailedEnergy} MWh`);
    res.json(hourlyResults);

  } catch (error) {
    console.error('Error fetching hourly curtailment:', error);
    res.status(500).json({
      error: "Internal server error while fetching hourly data"
    });
  }
}