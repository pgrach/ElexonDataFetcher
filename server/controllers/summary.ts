import { Request, Response } from "express";
import { db } from "@db";
import { dailySummaries, curtailmentRecords, monthlySummaries, yearlySummaries } from "@db/schema";
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

export async function getCurtailedLeadParties(req: Request, res: Response) {
  try {
    const { date } = req.params;

    // Validate date format
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
      return res.status(400).json({
        error: "Invalid date format. Please use YYYY-MM-DD"
      });
    }

    // Get unique lead parties that had curtailment on the specified date
    const leadParties = await db
      .select({
        leadPartyName: curtailmentRecords.leadPartyName,
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date))
      .groupBy(curtailmentRecords.leadPartyName)
      .orderBy(curtailmentRecords.leadPartyName);

    res.json(leadParties.map(party => party.leadPartyName));
  } catch (error) {
    console.error('Error fetching curtailed lead parties:', error);
    res.status(500).json({
      error: "Internal server error while fetching curtailed lead parties"
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
    const { leadParty } = req.query;

    // Validate yearMonth format (YYYY-MM)
    if (!/^\d{4}-\d{2}$/.test(yearMonth)) {
      return res.status(400).json({
        error: "Invalid format. Please use YYYY-MM"
      });
    }

    // If leadParty is specified, calculate from curtailment_records
    if (leadParty) {
      const farmTotals = await db
        .select({
          totalCurtailedEnergy: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
          totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
        })
        .from(curtailmentRecords)
        .where(
          and(
            sql`date_trunc('month', ${curtailmentRecords.settlementDate}::date) = date_trunc('month', ${yearMonth + '-01'}::date)`,
            eq(curtailmentRecords.leadPartyName, leadParty as string)
          )
        );

      if (!farmTotals[0] || !farmTotals[0].totalCurtailedEnergy) {
        return res.status(404).json({
          error: "No data available for this month and lead party"
        });
      }

      return res.json({
        yearMonth,
        totalCurtailedEnergy: Number(farmTotals[0].totalCurtailedEnergy),
        totalPayment: Math.abs(Number(farmTotals[0].totalPayment)),
        dailyTotals: {
          totalCurtailedEnergy: Number(farmTotals[0].totalCurtailedEnergy),
          totalPayment: Math.abs(Number(farmTotals[0].totalPayment))
        }
      });
    }

    // If no leadParty, get the monthly summary from monthlySummaries table
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

    // Get total volume per settlement period
    const periodTotals = await db
      .select({
        settlementPeriod: curtailmentRecords.settlementPeriod,
        totalVolume: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`
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
      .groupBy(curtailmentRecords.settlementPeriod)
      .orderBy(curtailmentRecords.settlementPeriod);

    // Map settlement periods to hours
    for (const record of periodTotals) {
      if (record.settlementPeriod && record.totalVolume) {
        const period = Number(record.settlementPeriod);
        const hour = Math.floor((period - 1) / 2);  // Periods 1-2 -> hour 0, 3-4 -> hour 1, etc.

        if (hour >= 0 && hour < 24) {
          const volume = Number(record.totalVolume);
          if (hour === 0) {
            console.log(`Period ${period}: Adding ${volume} MWh to hour 0`);
          }
          hourlyResults[hour].curtailedEnergy += volume;
        }
      }
    }

    // Zero out future hours for current day
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

export async function getYearlySummary(req: Request, res: Response) {
  try {
    const { year } = req.params;
    const { leadParty } = req.query;

    // Validate year format (YYYY)
    if (!/^\d{4}$/.test(year)) {
      return res.status(400).json({
        error: "Invalid format. Please use YYYY"
      });
    }

    // If leadParty is specified, calculate from curtailment_records
    if (leadParty) {
      const farmTotals = await db
        .select({
          totalCurtailedEnergy: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
          totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
        })
        .from(curtailmentRecords)
        .where(
          and(
            sql`date_trunc('year', ${curtailmentRecords.settlementDate}::date) = date_trunc('year', ${year}-01-01::date)`,
            eq(curtailmentRecords.leadPartyName, leadParty as string)
          )
        );

      console.log(`Year ${year} farm totals for ${leadParty}:`, farmTotals[0]);

      if (!farmTotals[0] || !farmTotals[0].totalCurtailedEnergy) {
        return res.status(404).json({
          error: "No data available for this year and lead party"
        });
      }

      return res.json({
        year,
        totalCurtailedEnergy: Number(farmTotals[0].totalCurtailedEnergy),
        totalPayment: Math.abs(Number(farmTotals[0].totalPayment)),
      });
    }

    // Calculate from daily_summaries for better accuracy
    const yearTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`COALESCE(SUM(${dailySummaries.totalCurtailedEnergy}::numeric), 0)`,
        totalPayment: sql<string>`COALESCE(SUM(${dailySummaries.totalPayment}::numeric), 0)`
      })
      .from(dailySummaries)
      .where(sql`date_trunc('year', ${dailySummaries.summaryDate}::date) = date_trunc('year', ${year}-01-01::date)`);

    console.log(`Year ${year} totals from daily summaries:`, yearTotals[0]);

    if (!yearTotals[0]) {
      return res.status(404).json({
        error: "No data available for this year"
      });
    }

    res.json({
      year,
      totalCurtailedEnergy: Number(yearTotals[0].totalCurtailedEnergy),
      totalPayment: Math.abs(Number(yearTotals[0].totalPayment)),
    });
  } catch (error) {
    console.error('Error fetching yearly summary:', error);
    res.status(500).json({
      error: "Internal server error while fetching summary"
    });
  }
}