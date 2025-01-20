import { Request, Response } from "express";
import { db } from "@db";
import { dailySummaries, curtailmentRecords } from "@db/schema";
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
      totalPayment: Number(summary.totalPayment),
      recordTotals: {
        totalVolume: Number(recordTotals[0]?.totalVolume || 0),
        totalPayment: Number(recordTotals[0]?.totalPayment || 0)
      }
    });
  } catch (error) {
    console.error('Error fetching daily summary:', error);
    res.status(500).json({
      error: "Internal server error while fetching summary"
    });
  }
}