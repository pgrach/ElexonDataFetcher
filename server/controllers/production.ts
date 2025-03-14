import { Request, Response } from "express";
import { db } from "@db";
import { curtailmentRecords } from "@db/schema";
import { and, eq, isNotNull, desc, sql } from "drizzle-orm";

/**
 * Get lead parties with curtailment data for a specific date
 */
export async function getCurtailedLeadPartiesByDate(req: Request, res: Response) {
  try {
    const { date } = req.query;
    
    if (!date || typeof date !== 'string') {
      return res.status(400).json({ error: 'Date parameter is required in format YYYY-MM-DD' });
    }
    
    const leadParties = await db
      .select({
        leadPartyName: curtailmentRecords.leadPartyName,
        count: sql<number>`count(distinct ${curtailmentRecords.farmId})`.as('count'),
        totalCurtailedEnergy: sql<number>`sum(abs(${curtailmentRecords.volume}))`.as('total_curtailed_energy'),
      })
      .from(curtailmentRecords)
      .where(
        and(
          isNotNull(curtailmentRecords.leadPartyName),
          eq(curtailmentRecords.settlementDate, date)
        )
      )
      .groupBy(curtailmentRecords.leadPartyName)
      .orderBy(desc(sql`total_curtailed_energy`));

    res.json(
      leadParties.map(({ leadPartyName, count, totalCurtailedEnergy }) => ({
        leadPartyName,
        farmCount: count || 0,
        totalCurtailedEnergy: Number(totalCurtailedEnergy || 0),
      }))
    );
  } catch (error) {
    console.error('[ERROR] Failed to get curtailed lead parties by date:', error);
    res.status(500).json({ error: 'Failed to get curtailed lead parties by date' });
  }
}