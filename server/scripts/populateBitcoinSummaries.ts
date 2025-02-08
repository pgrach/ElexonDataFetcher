import { format } from 'date-fns';
import { db } from "@db";
import { 
  historicalBitcoinCalculations, 
  bitcoinDailySummaries,
  bitcoinMonthlySummaries,
  bitcoinYearlySummaries 
} from "@db/schema";
import { sql, and, eq } from "drizzle-orm";
import axios from 'axios';

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

    const dailyData = await db.execute(sql`
      WITH daily_totals AS (
        SELECT 
          settlement_date,
          miner_model,
          COALESCE(SUM(CAST(bitcoin_mined AS numeric)), 0) as total_bitcoin,
          COALESCE(AVG(CAST(difficulty AS numeric)), 0) as avg_difficulty
        FROM historical_bitcoin_calculations
        GROUP BY settlement_date, miner_model
      )
      SELECT * FROM daily_totals
      ORDER BY settlement_date DESC
    `);

    for (const record of dailyData.rows) {
      const bitcoinMined = parseFloat(record.total_bitcoin as string) || 0;
      const avgDifficulty = parseFloat(record.avg_difficulty as string) || 0;

      await db.insert(bitcoinDailySummaries)
        .values({
          summaryDate: record.settlement_date as string,
          minerModel: record.miner_model as string,
          bitcoinMined: bitcoinMined.toString(),
          valueAtMining: (bitcoinMined * currentPrice).toString(),
          averageDifficulty: avgDifficulty.toString(),
          updatedAt: new Date()
        })
        .onConflictDoUpdate({
          target: [bitcoinDailySummaries.summaryDate, bitcoinDailySummaries.minerModel],
          set: {
            bitcoinMined: bitcoinMined.toString(),
            valueAtMining: (bitcoinMined * currentPrice).toString(),
            averageDifficulty: avgDifficulty.toString(),
            updatedAt: new Date()
          }
        });
    }

    console.log(`Populated ${dailyData.rows.length} daily summaries`);
  } catch (error) {
    console.error('Error populating daily summaries:', error);
    throw error;
  }
}

async function populateMonthlySummaries() {
  try {
    console.log('Populating monthly summaries...');
    const currentPrice = await fetchCurrentBitcoinPrice();

    const monthlyData = await db.execute(sql`
      WITH monthly_totals AS (
        SELECT 
          TO_CHAR(settlement_date, 'YYYY-MM') as year_month,
          miner_model,
          COALESCE(SUM(CAST(bitcoin_mined AS numeric)), 0) as total_bitcoin,
          COALESCE(AVG(CAST(difficulty AS numeric)), 0) as avg_difficulty
        FROM historical_bitcoin_calculations
        GROUP BY TO_CHAR(settlement_date, 'YYYY-MM'), miner_model
      )
      SELECT * FROM monthly_totals
      ORDER BY year_month DESC
    `);

    for (const record of monthlyData.rows) {
      const bitcoinMined = parseFloat(record.total_bitcoin as string) || 0;
      const avgDifficulty = parseFloat(record.avg_difficulty as string) || 0;

      await db.insert(bitcoinMonthlySummaries)
        .values({
          yearMonth: record.year_month as string,
          minerModel: record.miner_model as string,
          bitcoinMined: bitcoinMined.toString(),
          valueAtMining: (bitcoinMined * currentPrice).toString(),
          averageDifficulty: avgDifficulty.toString(),
          updatedAt: new Date()
        })
        .onConflictDoUpdate({
          target: [bitcoinMonthlySummaries.yearMonth, bitcoinMonthlySummaries.minerModel],
          set: {
            bitcoinMined: bitcoinMined.toString(),
            valueAtMining: (bitcoinMined * currentPrice).toString(),
            averageDifficulty: avgDifficulty.toString(),
            updatedAt: new Date()
          }
        });
    }

    console.log(`Populated ${monthlyData.rows.length} monthly summaries`);
  } catch (error) {
    console.error('Error populating monthly summaries:', error);
    throw error;
  }
}

async function populateYearlySummaries() {
  try {
    console.log('Populating yearly summaries...');
    const currentPrice = await fetchCurrentBitcoinPrice();

    const yearlyData = await db.execute(sql`
      WITH yearly_totals AS (
        SELECT 
          TO_CHAR(settlement_date, 'YYYY') as year,
          miner_model,
          COALESCE(SUM(CAST(bitcoin_mined AS numeric)), 0) as total_bitcoin,
          COALESCE(AVG(CAST(difficulty AS numeric)), 0) as avg_difficulty
        FROM historical_bitcoin_calculations
        GROUP BY TO_CHAR(settlement_date, 'YYYY'), miner_model
      )
      SELECT * FROM yearly_totals
      ORDER BY year DESC
    `);

    for (const record of yearlyData.rows) {
      const bitcoinMined = parseFloat(record.total_bitcoin as string) || 0;
      const avgDifficulty = parseFloat(record.avg_difficulty as string) || 0;

      await db.insert(bitcoinYearlySummaries)
        .values({
          year: record.year as string,
          minerModel: record.miner_model as string,
          bitcoinMined: bitcoinMined.toString(),
          valueAtMining: (bitcoinMined * currentPrice).toString(),
          averageDifficulty: avgDifficulty.toString(),
          updatedAt: new Date()
        })
        .onConflictDoUpdate({
          target: [bitcoinYearlySummaries.year, bitcoinYearlySummaries.minerModel],
          set: {
            bitcoinMined: bitcoinMined.toString(),
            valueAtMining: (bitcoinMined * currentPrice).toString(),
            averageDifficulty: avgDifficulty.toString(),
            updatedAt: new Date()
          }
        });
    }

    console.log(`Populated ${yearlyData.rows.length} yearly summaries`);
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