import { Request, Response } from "express";
import { db } from "@db";
import { dailySummaries, curtailmentRecords, monthlySummaries, yearlySummaries, historicalBitcoinCalculations } from "@db/schema";
import { eq, sql, and, desc } from "drizzle-orm";
import axios from 'axios';

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
        totalPayment: Number(summary.totalPayment), // Keep the original sign
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
      totalCurtailedEnergy: Math.abs(Number(recordTotals[0].totalVolume)), // Keep absolute for energy
      totalPayment: Number(recordTotals[0].totalPayment), // Keep the original sign
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
        totalPayment: Number(farmTotals[0].totalPayment), // Keep the original sign
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
      totalPayment: Number(summary.totalPayment), // Keep original sign
      dailyTotals: {
        totalCurtailedEnergy: Number(dailyTotals[0]?.totalCurtailedEnergy || 0),
        totalPayment: Number(dailyTotals[0]?.totalPayment || 0) // Keep original sign
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

    res.json(hourlyResults);

  } catch (error) {
    console.error('Error fetching hourly curtailment:', error);
    res.status(500).json({
      error: "Internal server error while fetching hourly data"
    });
  }
}

export async function getHourlyComparison(req: Request, res: Response) {
  try {
    const { date } = req.params;
    const { leadParty, minerModel = 'S19J_PRO' } = req.query;

    // Validate date format
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
      return res.status(400).json({
        error: "Invalid date format. Please use YYYY-MM-DD"
      });
    }

    if (!leadParty) {
      return res.status(400).json({
        error: "leadParty parameter is required"
      });
    }

    // Initialize 24-hour array with zeros for all metrics
    const hourlyResults = Array.from({ length: 24 }, (_, i) => ({
      hour: `${i.toString().padStart(2, '0')}:00`,
      curtailedEnergy: 0,
      paymentAmount: 0,
      bitcoinMined: 0,
      currentPrice: 0, // This will be set from the API or default value
      paymentPerMwh: 0,
      bitcoinValuePerMwh: 0
    }));

    // First, get the current Bitcoin price
    let currentPrice = 65000; // Default fallback value
    try {
      const fetchedPrice = await fetchCurrentPrice();
      if (fetchedPrice) {
        currentPrice = fetchedPrice;
      }
    } catch (error) {
      console.warn('Error fetching Bitcoin price, using default value:', error);
    }

    // Get data per settlement period
    const periodData = await db
      .select({
        settlementPeriod: curtailmentRecords.settlementPeriod,
        volume: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
        payment: sql<string>`SUM(ABS(${curtailmentRecords.payment}::numeric))`, // Use ABS to get positive payment values
      })
      .from(curtailmentRecords)
      .where(and(
        eq(curtailmentRecords.settlementDate, date),
        eq(curtailmentRecords.leadPartyName, leadParty as string)
      ))
      .groupBy(curtailmentRecords.settlementPeriod)
      .orderBy(curtailmentRecords.settlementPeriod);

    // Get Bitcoin data per settlement period for the same lead party
    const bitcoinData = await db
      .select({
        settlementPeriod: historicalBitcoinCalculations.settlementPeriod,
        bitcoinMined: sql<string>`SUM(${historicalBitcoinCalculations.bitcoinMined}::numeric)`,
      })
      .from(historicalBitcoinCalculations)
      .innerJoin(
        curtailmentRecords,
        and(
          eq(historicalBitcoinCalculations.settlementDate, curtailmentRecords.settlementDate),
          eq(historicalBitcoinCalculations.settlementPeriod, curtailmentRecords.settlementPeriod),
          eq(historicalBitcoinCalculations.farmId, curtailmentRecords.farmId)
        )
      )
      .where(and(
        eq(historicalBitcoinCalculations.settlementDate, date),
        eq(curtailmentRecords.leadPartyName, leadParty as string),
        eq(historicalBitcoinCalculations.minerModel, minerModel as string)
      ))
      .groupBy(historicalBitcoinCalculations.settlementPeriod)
      .orderBy(historicalBitcoinCalculations.settlementPeriod);

    // Create a map of Bitcoin data by period for quick lookup
    const bitcoinByPeriod = new Map();
    for (const record of bitcoinData) {
      if (record.settlementPeriod) {
        bitcoinByPeriod.set(Number(record.settlementPeriod), {
          bitcoinMined: Number(record.bitcoinMined)
        });
      }
    }

    // Map settlement periods to hours
    for (const record of periodData) {
      if (record.settlementPeriod) {
        const period = Number(record.settlementPeriod);
        const hour = Math.floor((period - 1) / 2);  // Periods 1-2 -> hour 0, 3-4 -> hour 1, etc.

        if (hour >= 0 && hour < 24) {
          const volume = Number(record.volume || 0);
          const payment = Number(record.payment || 0);
          
          // Get Bitcoin data if available
          const bitcoinRecord = bitcoinByPeriod.get(period);
          const bitcoinMined = bitcoinRecord ? bitcoinRecord.bitcoinMined : 0;
          
          hourlyResults[hour].curtailedEnergy += volume;
          hourlyResults[hour].paymentAmount += payment;
          hourlyResults[hour].bitcoinMined += bitcoinMined;
          hourlyResults[hour].currentPrice = currentPrice;
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
        hourlyResults[i].paymentAmount = 0;
        hourlyResults[i].bitcoinMined = 0;
      }
    }

    // Calculate rates per MWh and round values for consistency
    hourlyResults.forEach(result => {
      result.curtailedEnergy = Number(result.curtailedEnergy.toFixed(2));
      result.paymentAmount = Number(result.paymentAmount.toFixed(2));
      result.bitcoinMined = Number(result.bitcoinMined.toFixed(6));
      
      // Calculate payment and Bitcoin value per MWh (£/MWh)
      if (result.curtailedEnergy > 0) {
        result.paymentPerMwh = Number((result.paymentAmount / result.curtailedEnergy).toFixed(2));
        result.bitcoinValuePerMwh = Number(((result.bitcoinMined * currentPrice) / result.curtailedEnergy).toFixed(2));
      } else {
        result.paymentPerMwh = 0;
        result.bitcoinValuePerMwh = 0;
      }
    });

    res.json(hourlyResults);

  } catch (error) {
    console.error('Error fetching hourly comparison data:', error);
    res.status(500).json({
      error: "Internal server error while fetching hourly comparison data"
    });
  }
}

// Helper function to fetch current Bitcoin price from Minerstat API
async function fetchCurrentPrice(): Promise<number | null> {
  try {
    const response = await axios.get('https://api.minerstat.com/v2/coins?list=BTC');
    
    if (response.status === 200 && Array.isArray(response.data) && response.data.length > 0) {
      const btcData = response.data[0];
      if (btcData && btcData.price) {
        return Number(btcData.price);
      }
    }
    return null;
  } catch (error) {
    console.error('Error fetching Bitcoin price:', error);
    return null;
  }
}

export async function getMonthlyComparison(req: Request, res: Response) {
  try {
    const { yearMonth } = req.params;
    const { leadParty, minerModel = 'S19J_PRO' } = req.query;

    // Validate yearMonth format (YYYY-MM)
    if (!/^\d{4}-\d{2}$/.test(yearMonth)) {
      return res.status(400).json({
        error: "Invalid format. Please use YYYY-MM"
      });
    }

    if (!leadParty) {
      return res.status(400).json({
        error: "leadParty parameter is required"
      });
    }

    // Parse the year and month
    const year = yearMonth.split('-')[0];
    const month = yearMonth.split('-')[1];
    
    // Get all dates in the month
    const startDate = new Date(`${yearMonth}-01`);
    const endDate = new Date(startDate);
    endDate.setMonth(endDate.getMonth() + 1);
    endDate.setDate(0); // Last day of the month
    
    const daysInMonth = endDate.getDate();
    
    // Initialize an array for each day of the month
    const dailyResults = Array.from({ length: daysInMonth }, (_, i) => ({
      day: `${(i + 1).toString().padStart(2, '0')}`,
      curtailedEnergy: 0,
      paymentAmount: 0,
      bitcoinMined: 0,
      currentPrice: 0,
      paymentPerMwh: 0,
      bitcoinValuePerMwh: 0
    }));
    
    // First, get the current Bitcoin price
    let currentPrice = 65000; // Default fallback value
    try {
      const fetchedPrice = await fetchCurrentPrice();
      if (fetchedPrice) {
        currentPrice = fetchedPrice;
      }
    } catch (error) {
      console.warn('Error fetching Bitcoin price, using default value:', error);
    }

    // Get data per day for the whole month
    const dailyData = await db
      .select({
        settlementDate: curtailmentRecords.settlementDate,
        volume: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
        payment: sql<string>`SUM(ABS(${curtailmentRecords.payment}::numeric))`,
      })
      .from(curtailmentRecords)
      .where(and(
        sql`EXTRACT(YEAR FROM ${curtailmentRecords.settlementDate}::date) = ${year}`,
        sql`EXTRACT(MONTH FROM ${curtailmentRecords.settlementDate}::date) = ${month}`,
        eq(curtailmentRecords.leadPartyName, leadParty as string)
      ))
      .groupBy(curtailmentRecords.settlementDate)
      .orderBy(curtailmentRecords.settlementDate);
    
    // Create a map of daily data for quick lookup
    const dataByDay = new Map();
    for (const record of dailyData) {
      if (record.settlementDate) {
        const date = new Date(record.settlementDate);
        const day = date.getDate();
        
        dataByDay.set(day, {
          volume: Number(record.volume || 0),
          payment: Number(record.payment || 0)
        });
      }
    }
    
    // Get Bitcoin data per day for the month
    const bitcoinData = await db
      .select({
        settlementDate: historicalBitcoinCalculations.settlementDate,
        bitcoinMined: sql<string>`SUM(${historicalBitcoinCalculations.bitcoinMined}::numeric)`,
      })
      .from(historicalBitcoinCalculations)
      .innerJoin(
        curtailmentRecords,
        and(
          eq(historicalBitcoinCalculations.settlementDate, curtailmentRecords.settlementDate),
          eq(historicalBitcoinCalculations.settlementPeriod, curtailmentRecords.settlementPeriod),
          eq(historicalBitcoinCalculations.farmId, curtailmentRecords.farmId)
        )
      )
      .where(and(
        sql`EXTRACT(YEAR FROM ${historicalBitcoinCalculations.settlementDate}::date) = ${year}`,
        sql`EXTRACT(MONTH FROM ${historicalBitcoinCalculations.settlementDate}::date) = ${month}`,
        eq(curtailmentRecords.leadPartyName, leadParty as string),
        eq(historicalBitcoinCalculations.minerModel, minerModel as string)
      ))
      .groupBy(historicalBitcoinCalculations.settlementDate)
      .orderBy(historicalBitcoinCalculations.settlementDate);
    
    // Create a map of Bitcoin data by day for quick lookup
    const bitcoinByDay = new Map();
    for (const record of bitcoinData) {
      if (record.settlementDate) {
        const date = new Date(record.settlementDate);
        const day = date.getDate();
        
        bitcoinByDay.set(day, {
          bitcoinMined: Number(record.bitcoinMined || 0)
        });
      }
    }
    
    // Fill the daily results array with actual data
    for (let i = 0; i < daysInMonth; i++) {
      const day = i + 1;
      const dayData = dataByDay.get(day);
      const bitcoinData = bitcoinByDay.get(day);
      
      if (dayData) {
        dailyResults[i].curtailedEnergy = dayData.volume;
        dailyResults[i].paymentAmount = dayData.payment;
      }
      
      if (bitcoinData) {
        dailyResults[i].bitcoinMined = bitcoinData.bitcoinMined;
      }
      
      dailyResults[i].currentPrice = currentPrice;
      
      // Calculate per MWh rates
      if (dailyResults[i].curtailedEnergy > 0) {
        dailyResults[i].paymentPerMwh = Number((dailyResults[i].paymentAmount / dailyResults[i].curtailedEnergy).toFixed(2));
        dailyResults[i].bitcoinValuePerMwh = Number(((dailyResults[i].bitcoinMined * currentPrice) / dailyResults[i].curtailedEnergy).toFixed(2));
      }
    }
    
    // Format values for consistency
    dailyResults.forEach(result => {
      result.curtailedEnergy = Number(result.curtailedEnergy.toFixed(2));
      result.paymentAmount = Number(result.paymentAmount.toFixed(2));
      result.bitcoinMined = Number(result.bitcoinMined.toFixed(6));
    });
    
    res.json(dailyResults);
    
  } catch (error) {
    console.error('Error fetching monthly comparison data:', error);
    res.status(500).json({
      error: "Internal server error while fetching monthly comparison data"
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

    console.log(`Fetching yearly summary for ${year}${leadParty ? ` (Lead Party: ${leadParty})` : ''}`);

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
            sql`EXTRACT(YEAR FROM ${curtailmentRecords.settlementDate}::date) = ${parseInt(year)}`,
            eq(curtailmentRecords.leadPartyName, leadParty as string)
          )
        );

      console.log(`Lead party yearly totals:`, farmTotals[0]);

      if (!farmTotals[0] || !farmTotals[0].totalCurtailedEnergy) {
        return res.status(404).json({
          error: "No data available for this year and lead party"
        });
      }

      return res.json({
        year,
        totalCurtailedEnergy: Number(farmTotals[0].totalCurtailedEnergy),
        totalPayment: Number(farmTotals[0].totalPayment) // Keep original sign
      });
    }

    // Get monthly records for the year
    const monthlyTotals = await db
      .select({
        yearMonth: monthlySummaries.yearMonth,
        totalCurtailedEnergy: monthlySummaries.totalCurtailedEnergy,
        totalPayment: sql<string>`${monthlySummaries.totalPayment}::numeric`
      })
      .from(monthlySummaries)
      .where(sql`TO_DATE(${monthlySummaries.yearMonth} || '-01', 'YYYY-MM-DD')::date >= DATE_TRUNC('year', TO_DATE(${year}, 'YYYY'))::date
            AND TO_DATE(${monthlySummaries.yearMonth} || '-01', 'YYYY-MM-DD')::date < DATE_TRUNC('year', TO_DATE(${year}, 'YYYY'))::date + INTERVAL '1 year'`)
      .orderBy(monthlySummaries.yearMonth);

    console.log(`Found ${monthlyTotals.length} monthly records for ${year}`);

    // Calculate year totals from monthly records
    const yearTotals = monthlyTotals.reduce((acc, record) => ({
      totalCurtailedEnergy: acc.totalCurtailedEnergy + Number(record.totalCurtailedEnergy),
      totalPayment: acc.totalPayment + Number(record.totalPayment)
    }), { totalCurtailedEnergy: 0, totalPayment: 0 });

    // Verify against daily_summaries as a cross-check
    const dailyTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(${dailySummaries.totalCurtailedEnergy}::numeric)`,
        totalPayment: sql<string>`SUM(ABS(${dailySummaries.totalPayment}::numeric))`
      })
      .from(dailySummaries)
      .where(sql`EXTRACT(YEAR FROM ${dailySummaries.summaryDate}::date) = ${parseInt(year)}`);

    console.log('Year totals comparison:', {
      'Monthly aggregation': yearTotals,
      'Daily aggregation': {
        totalCurtailedEnergy: Number(dailyTotals[0]?.totalCurtailedEnergy || 0),
        totalPayment: Number(dailyTotals[0]?.totalPayment || 0)
      }
    });

    if (yearTotals.totalCurtailedEnergy === 0 && yearTotals.totalPayment === 0) {
      return res.status(404).json({
        error: "No data available for this year"
      });
    }

    res.json({
      year,
      totalCurtailedEnergy: yearTotals.totalCurtailedEnergy,
      totalPayment: yearTotals.totalPayment // Keep original sign
    });
  } catch (error) {
    console.error('Error fetching yearly summary:', error);
    res.status(500).json({
      error: "Internal server error while fetching summary"
    });
  }
}