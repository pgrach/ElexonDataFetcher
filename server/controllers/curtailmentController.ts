/**
 * Curtailment Controller
 * 
 * Handles API requests related to curtailment data and delegates business logic to services.
 */

import { Request, Response } from 'express';
import { db } from "@db";
import { curtailmentRecords } from "@db/schema";
import { and, eq, sql, between, inArray } from "drizzle-orm";
import { format, parseISO, startOfMonth, endOfMonth } from 'date-fns';
import { fetchBitcoinStats } from '../services/minerstatService';
import { calculateMonthlyBitcoinSummary } from '../services/bitcoinService';

/**
 * Get monthly mining potential data
 */
export async function getMonthlyMiningPotential(req: Request, res: Response) {
  try {
    const { month } = req.params;
    const { minerModel, leadParty } = req.query;
    
    console.log('Monthly mining potential request:', { 
      yearMonth: month, 
      minerModel: minerModel as string,
      leadParty: leadParty as string | undefined
    });

    // Validate month format
    if (!/^\d{4}-\d{2}$/.test(month)) {
      return res.status(400).json({
        error: "Invalid month format. Please use YYYY-MM"
      });
    }

    // Validate miner model is provided
    if (!minerModel) {
      return res.status(400).json({
        error: "Miner model is required. Please provide minerModel query parameter."
      });
    }

    // Get minerstat data (either from cache or API)
    const minerstatData = await fetchBitcoinStats();

    // Calculate Bitcoin summary
    const result = await calculateMonthlyBitcoinSummary(
      month,
      minerModel as string,
      leadParty as string | undefined,
      minerstatData.difficulty,
      minerstatData.priceGbp
    );

    res.json(result);
  } catch (error) {
    console.error('Error fetching monthly mining potential:', error);
    res.status(500).json({
      error: "Internal server error while calculating monthly mining potential"
    });
  }
}

/**
 * Get daily curtailment energy by farm
 */
export async function getDailyCurtailmentByFarm(req: Request, res: Response) {
  try {
    const { date } = req.params;
    const { leadParty } = req.query;

    // Validate date format
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
      return res.status(400).json({
        error: "Invalid date format. Please use YYYY-MM-DD"
      });
    }

    let query = db
      .select({
        farmId: curtailmentRecords.farmId,
        leadPartyName: curtailmentRecords.leadPartyName,
        totalEnergy: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))::text`,
        totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)::text`,
        periodCount: sql<number>`COUNT(DISTINCT ${curtailmentRecords.settlementPeriod})`,
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date))
      .groupBy(curtailmentRecords.farmId, curtailmentRecords.leadPartyName)
      .orderBy(sql`SUM(ABS(${curtailmentRecords.volume}::numeric))`, { direction: 'desc' });

    // Apply lead party filter if provided
    if (leadParty && leadParty !== 'All') {
      query = query.where(eq(curtailmentRecords.leadPartyName, leadParty as string));
    }

    const farms = await query;

    // Format output with typed numbers instead of strings for better client handling
    const result = farms.map(farm => ({
      farmId: farm.farmId,
      leadPartyName: farm.leadPartyName,
      energy: Number(farm.totalEnergy),
      payment: Number(farm.totalPayment),
      periodCount: farm.periodCount
    }));

    res.json(result);
  } catch (error) {
    console.error('Error fetching daily curtailment by farm:', error);
    res.status(500).json({
      error: "Internal server error while fetching daily curtailment by farm"
    });
  }
}