import { Request, Response } from "express";
import { db } from "@db";
import { dailySummaries } from "@db/schema";
import { eq } from "drizzle-orm";

export async function getDailySummary(req: Request, res: Response) {
  try {
    const { date } = req.params;

    // Validate date format
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
      return res.status(400).json({
        error: "Invalid date format. Please use YYYY-MM-DD"
      });
    }

    const summary = await db.query.dailySummaries.findFirst({
      where: eq(dailySummaries.summaryDate, date)
    });

    if (!summary) {
      return res.status(404).json({
        error: "No data available for this date"
      });
    }

    res.json({
      date,
      totalCurtailedEnergy: Number(summary.totalCurtailedEnergy),
      totalPayment: Number(summary.totalPayment)
    });
  } catch (error) {
    console.error('Error fetching daily summary:', error);
    res.status(500).json({
      error: "Internal server error while fetching summary"
    });
  }
}