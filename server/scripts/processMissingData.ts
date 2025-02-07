import { format, parseISO } from 'date-fns';
import { minerModels } from '../types/bitcoin';
import { processSingleDay, fetch2024Difficulties } from '../services/bitcoinService';
import { db } from "@db";
import { historicalBitcoinCalculations } from "@db/schema";
import { sql } from "drizzle-orm";

async function findMissingDates() {
  const query = `
    WITH RECURSIVE date_series AS (
      SELECT '2024-01-01'::date as date
      UNION ALL
      SELECT date + 1
      FROM date_series
      WHERE date < '2025-02-28'
    ),
    actual_data AS (
      SELECT 
        settlement_date,
        COUNT(DISTINCT miner_model) as miner_count
      FROM historical_bitcoin_calculations
      GROUP BY settlement_date
    )
    SELECT 
      ds.date::text as missing_date
    FROM date_series ds
    LEFT JOIN actual_data ad ON ds.date = ad.settlement_date
    WHERE ad.miner_count IS NULL OR ad.miner_count < 3
    ORDER BY ds.date;
  `;

  const result = await db.execute(sql.raw(query));
  return result.rows.map(row => row.missing_date);
}

async function processMissingDates() {
  try {
    console.log('\n=== Processing Missing Historical Data ===');
    
    // Pre-fetch difficulties
    console.log('\nPre-fetching difficulties...');
    await fetch2024Difficulties();
    console.log('Difficulties pre-fetch complete\n');

    // Get missing dates
    const missingDates = await findMissingDates();
    console.log(`Found ${missingDates.length} dates with missing data`);

    for (const date of missingDates) {
      console.log(`\n=== Processing Date: ${date} ===`);
      
      for (const minerModel of Object.keys(minerModels)) {
        try {
          console.log(`- Processing ${minerModel}`);
          await processSingleDay(date, minerModel);
          console.log(`✓ Completed ${minerModel} for ${date}`);
        } catch (error) {
          console.error(`× Error processing ${minerModel} for ${date}:`, error);
          throw error;
        }
      }
    }

    console.log('\n=== Missing Data Processing Complete ===');
    
  } catch (error) {
    console.error('Error processing missing dates:', error);
    process.exit(1);
  }
}

// Start processing
processMissingDates();
