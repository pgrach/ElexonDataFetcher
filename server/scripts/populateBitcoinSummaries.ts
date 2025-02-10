import { format } from 'date-fns';
import { db } from "@db";
import { 
  historicalBitcoinCalculations, 
  bitcoinDailySummaries,
  bitcoinMonthlySummaries,
  bitcoinYearlySummaries 
} from "@db/schema";
import { sql } from "drizzle-orm";
import axios from 'axios';

interface SummaryRecord {
  settlement_date?: string;
  year_month?: string;
  year?: string;
  miner_model: string;
  total_bitcoin: string;
  avg_difficulty: string;
}

async function fetchCurrentBitcoinPrice(): Promise<number> {
  try {
    const response = await axios.get('https://api.minerstat.com/v2/stats/bitcoin');
    return response.data.price;
  } catch (error) {
    console.error('Error fetching Bitcoin price:', error);
    throw error;
  }
}

async function populateDailySummaries() {
  try {
    console.log('Populating daily summaries...');
    const currentPrice = await fetchCurrentBitcoinPrice();

    const result = await db.execute(sql`
      SELECT 
        settlement_date,
        miner_model,
        SUM(bitcoin_mined::numeric) as total_bitcoin,
        AVG(difficulty::numeric) as avg_difficulty
      FROM historical_bitcoin_calculations
      GROUP BY settlement_date, miner_model
      ORDER BY settlement_date DESC
    `);

    const dailyData = result.rows as SummaryRecord[];

    for (const record of dailyData) {
      if (!record.settlement_date) continue;

      await db.insert(bitcoinDailySummaries)
        .values({
          summaryDate: record.settlement_date,
          minerModel: record.miner_model,
          bitcoinMined: record.total_bitcoin,
          valueAtMining: (Number(record.total_bitcoin) * currentPrice).toString(),
          averageDifficulty: record.avg_difficulty,
          updatedAt: new Date()
        })
        .onConflictDoUpdate({
          target: [bitcoinDailySummaries.summaryDate, bitcoinDailySummaries.minerModel],
          set: {
            bitcoinMined: record.total_bitcoin,
            valueAtMining: (Number(record.total_bitcoin) * currentPrice).toString(),
            averageDifficulty: record.avg_difficulty,
            updatedAt: new Date()
          }
        });
    }

    console.log(`Populated ${dailyData.length} daily summaries`);
  } catch (error) {
    console.error('Error populating daily summaries:', error);
    throw error;
  }
}

async function populateMonthlySummaries() {
  try {
    console.log('Populating monthly summaries...');
    const currentPrice = await fetchCurrentBitcoinPrice();

    const result = await db.execute(sql`
      SELECT 
        TO_CHAR(summary_date, 'YYYY-MM') as year_month,
        miner_model,
        SUM(bitcoin_mined::numeric) as total_bitcoin,
        AVG(average_difficulty::numeric) as avg_difficulty
      FROM bitcoin_daily_summaries
      GROUP BY TO_CHAR(summary_date, 'YYYY-MM'), miner_model
      ORDER BY year_month DESC
    `);

    const monthlyData = result.rows as SummaryRecord[];

    for (const record of monthlyData) {
      if (!record.year_month) continue;

      await db.insert(bitcoinMonthlySummaries)
        .values({
          yearMonth: record.year_month,
          minerModel: record.miner_model,
          bitcoinMined: record.total_bitcoin,
          valueAtMining: (Number(record.total_bitcoin) * currentPrice).toString(),
          averageDifficulty: record.avg_difficulty,
          updatedAt: new Date()
        })
        .onConflictDoUpdate({
          target: [bitcoinMonthlySummaries.yearMonth, bitcoinMonthlySummaries.minerModel],
          set: {
            bitcoinMined: record.total_bitcoin,
            valueAtMining: (Number(record.total_bitcoin) * currentPrice).toString(),
            averageDifficulty: record.avg_difficulty,
            updatedAt: new Date()
          }
        });
    }

    console.log(`Populated ${monthlyData.length} monthly summaries`);
  } catch (error) {
    console.error('Error populating monthly summaries:', error);
    throw error;
  }
}

async function populateYearlySummaries() {
  try {
    console.log('Populating yearly summaries...');
    const currentPrice = await fetchCurrentBitcoinPrice();

    const result = await db.execute(sql`
      SELECT 
        TO_CHAR(summary_date, 'YYYY') as year,
        miner_model,
        SUM(bitcoin_mined::numeric) as total_bitcoin,
        AVG(average_difficulty::numeric) as avg_difficulty
      FROM bitcoin_daily_summaries
      GROUP BY TO_CHAR(summary_date, 'YYYY'), miner_model
      ORDER BY year DESC
    `);

    const yearlyData = result.rows as SummaryRecord[];

    for (const record of yearlyData) {
      if (!record.year) continue;

      await db.insert(bitcoinYearlySummaries)
        .values({
          year: record.year,
          minerModel: record.miner_model,
          bitcoinMined: record.total_bitcoin,
          valueAtMining: (Number(record.total_bitcoin) * currentPrice).toString(),
          averageDifficulty: record.avg_difficulty,
          updatedAt: new Date()
        })
        .onConflictDoUpdate({
          target: [bitcoinYearlySummaries.year, bitcoinYearlySummaries.minerModel],
          set: {
            bitcoinMined: record.total_bitcoin,
            valueAtMining: (Number(record.total_bitcoin) * currentPrice).toString(),
            averageDifficulty: record.avg_difficulty,
            updatedAt: new Date()
          }
        });
    }

    console.log(`Populated ${yearlyData.length} yearly summaries`);
  } catch (error) {
    console.error('Error populating yearly summaries:', error);
    throw error;
  }
}

async function populateAllSummaries() {
  try {
    console.log('\n=== Starting Bitcoin Summary Population ===\n');

    await populateDailySummaries();
    await populateMonthlySummaries();
    await populateYearlySummaries();

    console.log('\n=== Bitcoin Summary Population Complete ===\n');
  } catch (error) {
    console.error('Error in summary population:', error);
    process.exit(1);
  }
}

// Run the population script
if (import.meta.url === `file://${process.argv[1]}`) {
  populateAllSummaries();
}

export { populateAllSummaries };