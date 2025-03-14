import { Router, Request, Response } from "express";
import { db } from "../../db";
import { eq, and, sql } from "drizzle-orm";
import { isValidDateString, isValidYearMonth, isValidYear } from "../utils/dates";
import { ValidationError } from "../utils/errors";
import { curtailmentRecords } from "../../db/schema";

const router = Router();

/**
 * @route GET /api/lead-parties/:date
 * @description Get all lead parties with curtailment data for a specific date/month/year
 * @param date - Date in YYYY-MM-DD, YYYY-MM or YYYY format
 */
router.get("/:date", async (req: Request, res: Response) => {
  const { date } = req.params;
  
  try {
    let query;
    // Daily data (YYYY-MM-DD)
    if (isValidDateString(date)) {
      query = db
        .select({ leadPartyName: curtailmentRecords.leadPartyName })
        .from(curtailmentRecords)
        .where(eq(curtailmentRecords.settlementDate, date))
        .groupBy(curtailmentRecords.leadPartyName);
    } 
    // Monthly data (YYYY-MM)
    else if (isValidYearMonth(date)) {
      const [year, month] = date.split("-");
      query = db
        .select({ leadPartyName: curtailmentRecords.leadPartyName })
        .from(curtailmentRecords)
        .where(and(
          sql`extract(year from ${curtailmentRecords.settlementDate}) = ${parseInt(year)}`,
          sql`extract(month from ${curtailmentRecords.settlementDate}) = ${parseInt(month)}`
        ))
        .groupBy(curtailmentRecords.leadPartyName);
    } 
    // Yearly data (YYYY)
    else if (isValidYear(date)) {
      query = db
        .select({ leadPartyName: curtailmentRecords.leadPartyName })
        .from(curtailmentRecords)
        .where(sql`extract(year from ${curtailmentRecords.settlementDate}) = ${parseInt(date)}`)
        .groupBy(curtailmentRecords.leadPartyName);
    } 
    // Invalid date format
    else {
      throw new ValidationError(`Invalid date format: ${date}. Must be YYYY-MM-DD, YYYY-MM, or YYYY.`);
    }

    const result = await query;
    const leadParties = result
      .filter(item => item.leadPartyName) // Filter out null values
      .map(item => item.leadPartyName as string)
      .sort(); // Sort alphabetically

    res.json(leadParties);
  } catch (error) {
    console.error("Error fetching lead parties:", error);
    res.status(500).json({ error: "Failed to fetch lead parties" });
  }
});

export default router;