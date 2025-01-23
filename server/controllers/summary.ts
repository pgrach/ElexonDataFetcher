import { Request, Response } from "express";
import { db } from "@db";
import { dailySummaries, curtailmentRecords } from "@db/schema";
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

    // Base query for curtailment records
    let query = db
      .select({
        totalVolume: sql<string>`SUM(${curtailmentRecords.volume}::numeric)`,
        totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date));

    // Add lead party filter if specified
    if (leadParty && leadParty !== 'all') {
      query = query.where(eq(curtailmentRecords.leadPartyName, leadParty as string));
    }

    const recordTotals = await query;
    console.log('Curtailment records totals:', recordTotals[0]);

    // Get the daily summary (aggregate only)
    const summary = await db.query.dailySummaries.findFirst({
      where: eq(dailySummaries.summaryDate, date)
    });

    console.log('Daily summary:', summary);

    if (!summary && (!recordTotals[0] || !recordTotals[0].totalVolume)) {
      return res.status(404).json({
        error: "No data available for this date"
      });
    }

    // Return the appropriate data based on whether a lead party filter was applied
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

    // First get the daily summary total for validation
    const dailySummary = await db.query.dailySummaries.findFirst({
      where: eq(dailySummaries.summaryDate, date)
    });

    const dailyTotal = Number(dailySummary?.totalCurtailedEnergy || 0);
    console.log(`Daily summary total for ${date}:`, dailyTotal);

    // Initialize 24-hour array with zeros
    const hourlyResults = Array.from({ length: 24 }, (_, i) => ({
      hour: `${i.toString().padStart(2, '0')}:00`,
      curtailedEnergy: 0
    }));

    // Get all periods data first
    let query = db
      .select({
        settlementPeriod: curtailmentRecords.settlementPeriod,
        volume: curtailmentRecords.volume,
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date))
      .orderBy(curtailmentRecords.settlementPeriod);

    if (leadParty && leadParty !== 'all') {
      query = query.where(eq(curtailmentRecords.leadPartyName, leadParty as string));
    }

    const allPeriodData = await query;

    console.log(`\nSettlement period data for ${date}:`);
    allPeriodData.forEach((record) => {
      if (record.settlementPeriod && record.volume !== null) {
        const hour = Math.floor((Number(record.settlementPeriod) - 1) / 2);
        const volume = Number(record.volume);
        console.log(`[P${record.settlementPeriod}] Volume: ${volume.toFixed(2)} MWh -> Hour ${hour}`);

        if (hour >= 0 && hour < 24) {
          hourlyResults[hour].curtailedEnergy += volume;
        }
      }
    });

    console.log(`\nHourly distribution check for ${date}:`);
    let totalDistributed = 0;
    hourlyResults.forEach(result => {
      totalDistributed += result.curtailedEnergy;
      console.log(`${result.hour}: ${result.curtailedEnergy.toFixed(2)} MWh`);
    });
    console.log(`Total distributed: ${totalDistributed.toFixed(2)} MWh (Daily total: ${dailyTotal.toFixed(2)} MWh)`);

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

    res.json(hourlyResults);
  } catch (error) {
    console.error('Error fetching hourly curtailment:', error);
    res.status(500).json({
      error: "Internal server error while fetching hourly data"
    });
  }
}