import { Request, Response } from "express";
import { db } from "@db";
import { farmDailySummaries, curtailmentRecords } from "@db/schema";
import { eq, and, sql, desc } from "drizzle-orm";
import { startOfDay, endOfDay, parseISO, format } from "date-fns";

// Generate or update farm daily summary for a specific date
async function generateFarmDailySummary(farmId: string, date: string) {
  const dailyData = await db
    .select({
      totalCurtailedEnergy: sql<string>`SUM(${curtailmentRecords.volume}::numeric)`,
      totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`,
      avgOriginalPrice: sql<string>`AVG(${curtailmentRecords.originalPrice}::numeric)`,
      avgFinalPrice: sql<string>`AVG(${curtailmentRecords.finalPrice}::numeric)`,
      eventCount: sql<number>`COUNT(*)`,
      soFlaggedCount: sql<number>`SUM(CASE WHEN ${curtailmentRecords.soFlag} THEN 1 ELSE 0 END)`,
      cadlFlaggedCount: sql<number>`SUM(CASE WHEN ${curtailmentRecords.cadlFlag} THEN 1 ELSE 0 END)`
    })
    .from(curtailmentRecords)
    .where(
      and(
        eq(curtailmentRecords.farmId, farmId),
        eq(curtailmentRecords.settlementDate, date)
      )
    );

  if (!dailyData[0] || !dailyData[0].eventCount) {
    return null;
  }

  // Upsert the daily summary
  await db
    .insert(farmDailySummaries)
    .values({
      farmId,
      summaryDate: date,
      totalCurtailedEnergy: dailyData[0].totalCurtailedEnergy || "0",
      totalPayment: dailyData[0].totalPayment || "0",
      averageOriginalPrice: dailyData[0].avgOriginalPrice || "0",
      averageFinalPrice: dailyData[0].avgFinalPrice || "0",
      curtailmentEvents: dailyData[0].eventCount || 0,
      soFlaggedEvents: dailyData[0].soFlaggedCount || 0,
      cadlFlaggedEvents: dailyData[0].cadlFlaggedCount || 0
    })
    .onConflictDoUpdate({
      target: [farmDailySummaries.farmId, farmDailySummaries.summaryDate],
      set: {
        totalCurtailedEnergy: dailyData[0].totalCurtailedEnergy || "0",
        totalPayment: dailyData[0].totalPayment || "0",
        averageOriginalPrice: dailyData[0].avgOriginalPrice || "0",
        averageFinalPrice: dailyData[0].avgFinalPrice || "0",
        curtailmentEvents: dailyData[0].eventCount || 0,
        soFlaggedEvents: dailyData[0].soFlaggedCount || 0,
        cadlFlaggedEvents: dailyData[0].cadlFlaggedCount || 0,
        updatedAt: new Date()
      }
    });

  return dailyData[0];
}

// Get farm daily summary for a specific date
export async function getFarmDailySummary(req: Request, res: Response) {
  try {
    const { farmId, date } = req.params;

    // Validate date format
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
      return res.status(400).json({
        error: "Invalid date format. Please use YYYY-MM-DD"
      });
    }

    // First try to get existing summary
    let summary = await db.query.farmDailySummaries.findFirst({
      where: and(
        eq(farmDailySummaries.farmId, farmId),
        eq(farmDailySummaries.summaryDate, date)
      )
    });

    // If no summary exists, generate it
    if (!summary) {
      const generated = await generateFarmDailySummary(farmId, date);
      if (!generated) {
        return res.status(404).json({
          error: "No curtailment data available for this farm and date"
        });
      }

      // Fetch the newly generated summary
      summary = await db.query.farmDailySummaries.findFirst({
        where: and(
          eq(farmDailySummaries.farmId, farmId),
          eq(farmDailySummaries.summaryDate, date)
        )
      });
    }

    res.json(summary);
  } catch (error) {
    console.error('Error fetching farm daily summary:', error);
    res.status(500).json({
      error: "Internal server error while fetching farm summary"
    });
  }
}

// Get farm summaries for a date range
export async function getFarmSummariesByDateRange(req: Request, res: Response) {
  try {
    const { farmId } = req.params;
    const { startDate, endDate } = req.query;

    if (!startDate || !endDate || 
        !/^\d{4}-\d{2}-\d{2}$/.test(startDate as string) || 
        !/^\d{4}-\d{2}-\d{2}$/.test(endDate as string)) {
      return res.status(400).json({
        error: "Invalid date format. Please use YYYY-MM-DD for both startDate and endDate query parameters"
      });
    }

    const summaries = await db.query.farmDailySummaries.findMany({
      where: and(
        eq(farmDailySummaries.farmId, farmId),
        sql`${farmDailySummaries.summaryDate} >= ${startDate}`,
        sql`${farmDailySummaries.summaryDate} <= ${endDate}`
      ),
      orderBy: desc(farmDailySummaries.summaryDate)
    });

    // Calculate period totals
    const periodTotals = {
      totalCurtailedEnergy: summaries.reduce((sum, day) => 
        sum + Number(day.totalCurtailedEnergy), 0),
      totalPayment: summaries.reduce((sum, day) => 
        sum + Number(day.totalPayment), 0),
      totalEvents: summaries.reduce((sum, day) => 
        sum + day.curtailmentEvents, 0),
      daysWithCurtailment: summaries.length,
      averageOriginalPrice: summaries.reduce((sum, day) => 
        sum + Number(day.averageOriginalPrice), 0) / (summaries.length || 1),
      averageFinalPrice: summaries.reduce((sum, day) => 
        sum + Number(day.averageFinalPrice), 0) / (summaries.length || 1)
    };

    res.json({
      farmId,
      startDate,
      endDate,
      periodTotals,
      dailySummaries: summaries
    });
  } catch (error) {
    console.error('Error fetching farm summaries:', error);
    res.status(500).json({
      error: "Internal server error while fetching farm summaries"
    });
  }
}
