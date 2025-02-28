/**
 * Single Period Reconciliation Script
 * 
 * This script focuses on reconciling a single period for a specific date.
 * It's designed to be run as a background process, making incremental progress.
 */

import pg from 'pg';
import { db } from './db';
import { eq, and } from 'drizzle-orm';
import { curtailmentRecords, historicalBitcoinCalculations } from './db/schema';
import { getDifficultyData } from './server/services/dynamodbService';

// Configuration
const TARGET_DATE = process.argv[2] || '2023-12-21';
const TARGET_PERIOD = parseInt(process.argv[3] || '1');
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

// Database connection
const dbUrl = process.env.DATABASE_URL;
if (!dbUrl) {
  console.error('DATABASE_URL environment variable is not set');
  process.exit(1);
}

const pool = new pg.Pool({
  connectionString: dbUrl,
  max: 3,
});

// Helper function to sleep
async function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Types
interface MissingCombo {
  farmId: string;
  minerModel: string;
}

interface PeriodStatus {
  farms: number;
  expected: number;
  actual: number;
  percentage: number;
  missingFarms: string[];
  missingCombos: MissingCombo[];
}

/**
 * Get the status for a specific period
 */
async function getPeriodStatus(date: string, period: number): Promise<PeriodStatus> {
  const client = await pool.connect();
  try {
    // First, get the basic stats
    const statsQuery = `
      SELECT 
        COUNT(DISTINCT cr.farm_id) AS farms,
        COUNT(DISTINCT cr.farm_id) * 3 AS expected_count,
        COUNT(DISTINCT hbc.id) AS actual_count
      FROM 
        curtailment_records cr
      LEFT JOIN 
        historical_bitcoin_calculations hbc ON cr.settlement_date = hbc.settlement_date
        AND cr.settlement_period = hbc.settlement_period
        AND cr.farm_id = hbc.farm_id
      WHERE 
        cr.settlement_date = $1
        AND cr.settlement_period = $2;
    `;
    
    const statsResult = await client.query(statsQuery, [date, period]);
    
    const farms = parseInt(statsResult.rows[0].farms);
    const expected = parseInt(statsResult.rows[0].expected_count);
    const actual = parseInt(statsResult.rows[0].actual_count);
    const percentage = expected > 0 ? (actual / expected) * 100 : 100;
    
    // Get the missing farm/model combinations
    const missingCombosQuery = `
      WITH farm_models AS (
        SELECT 
          cr.farm_id, 
          m.model_name
        FROM 
          curtailment_records cr
        CROSS JOIN (
          SELECT unnest(ARRAY['S19J_PRO', 'S9', 'M20S']) AS model_name
        ) m
        WHERE 
          cr.settlement_date = $1
          AND cr.settlement_period = $2
      ),
      existing_combos AS (
        SELECT 
          farm_id, 
          miner_model
        FROM 
          historical_bitcoin_calculations
        WHERE 
          settlement_date = $1
          AND settlement_period = $2
      )
      SELECT 
        fm.farm_id,
        fm.model_name AS miner_model
      FROM 
        farm_models fm
      LEFT JOIN 
        existing_combos ec ON fm.farm_id = ec.farm_id AND fm.model_name = ec.miner_model
      WHERE 
        ec.farm_id IS NULL
      ORDER BY 
        fm.farm_id, fm.model_name;
    `;
    
    const missingCombosResult = await client.query(missingCombosQuery, [date, period]);
    
    // Create the missing combos data structure
    const missingCombos = missingCombosResult.rows.map((row: any) => ({
      farmId: row.farm_id as string,
      minerModel: row.miner_model as string
    }));
    
    // Extract unique farm IDs from the missing combinations
    const missingFarms = [...new Set(missingCombos.map(combo => combo.farmId))];
    
    return {
      farms,
      expected,
      actual,
      percentage,
      missingFarms: missingFarms,
      missingCombos: missingCombos
    };
  } finally {
    client.release();
  }
}

/**
 * Process a specific period
 */
async function processPeriod(date: string, period: number): Promise<boolean> {
  const client = await pool.connect();
  try {
    console.log(`\n=== Processing ${date} Period ${period} ===\n`);
    
    // Get initial status
    const beforeStatus = await getPeriodStatus(date, period);
    console.log(`Initial Status: ${beforeStatus.actual}/${beforeStatus.expected} calculations (${beforeStatus.percentage.toFixed(2)}%)`);
    console.log(`Farms: ${beforeStatus.farms}, Missing combinations: ${beforeStatus.missingCombos.length}`);
    
    if (beforeStatus.percentage === 100) {
      console.log(`Period ${period} already at 100%, nothing to do.`);
      return true;
    }
    
    // Get the difficulty for this date
    const difficulty = await getDifficultyData(date);
    console.log(`Using difficulty: ${difficulty}`);
    
    // Process the missing combinations
    console.log(`Processing ${beforeStatus.missingCombos.length} missing combinations...`);
    
    let successCount = 0;
    let failureCount = 0;
    
    // Group the missing combinations by farm to reduce database lookups
    const missingCombosByFarm: Record<string, string[]> = {};
    for (const combo of beforeStatus.missingCombos) {
      if (!missingCombosByFarm[combo.farmId]) {
        missingCombosByFarm[combo.farmId] = [];
      }
      missingCombosByFarm[combo.farmId].push(combo.minerModel);
    }
    
    // Process each farm
    for (const farmId of Object.keys(missingCombosByFarm)) {
      try {
        // Get the curtailment record
        const curtailment = await db
          .select({
            volume: curtailmentRecords.volume,
            payment: curtailmentRecords.payment,
            leadPartyName: curtailmentRecords.leadPartyName
          })
          .from(curtailmentRecords)
          .where(
            and(
              eq(curtailmentRecords.settlementDate, date),
              eq(curtailmentRecords.settlementPeriod, period),
              eq(curtailmentRecords.farmId, farmId)
            )
          )
          .limit(1);
        
        if (curtailment.length === 0) {
          console.log(`No curtailment record found for ${farmId}`);
          continue;
        }
        
        const volume = Number(curtailment[0].volume);
        if (Math.abs(volume) < 0.01) {
          console.log(`Zero volume for ${farmId}, skipping...`);
          successCount += missingCombosByFarm[farmId].length;
          continue;
        }
        
        // Process each required miner model for this farm
        for (const minerModel of missingCombosByFarm[farmId]) {
          try {
            // Double check if record already exists to avoid duplicates
            const existing = await db
              .select({ id: historicalBitcoinCalculations.id })
              .from(historicalBitcoinCalculations)
              .where(
                and(
                  eq(historicalBitcoinCalculations.settlementDate, date),
                  eq(historicalBitcoinCalculations.settlementPeriod, period),
                  eq(historicalBitcoinCalculations.farmId, farmId),
                  eq(historicalBitcoinCalculations.minerModel, minerModel)
                )
              );
            
            if (existing.length > 0) {
              console.log(`Record already exists for ${farmId} with ${minerModel}`);
              successCount++;
              continue;
            }
            
            // Insert the record using snake_case column names
            const volumeMWh = Math.abs(volume);
            
            // Use direct SQL to avoid type issues
            const insertQuery = `
              INSERT INTO historical_bitcoin_calculations 
                (settlement_date, settlement_period, farm_id, miner_model, 
                 curtailed_energy, difficulty, bitcoin_mined, calculated_at, lead_party_name)
              VALUES 
                ($1, $2, $3, $4, $5, $6, $7, $8, $9)
              RETURNING id;
            `;
            
            const insertResult = await client.query(insertQuery, [
              date, 
              period, 
              farmId, 
              minerModel, 
              volumeMWh.toString(), 
              difficulty.toString(), 
              "0", // Placeholder, will be updated in batch
              new Date(),
              curtailment[0].leadPartyName || null
            ]);
            
            if (insertResult && insertResult.rowCount && insertResult.rowCount > 0) {
              successCount++;
              console.log(`Inserted record for ${farmId} with ${minerModel}`);
            } else {
              failureCount++;
              console.log(`Failed to insert record for ${farmId} with ${minerModel}`);
            }
          } catch (error) {
            failureCount++;
            console.error(`Error processing ${farmId} with ${minerModel}:`, error);
          }
          
          // Brief pause to prevent database overload
          await sleep(50);
        }
      } catch (error) {
        failureCount += missingCombosByFarm[farmId].length;
        console.error(`Error processing farm ${farmId}:`, error);
      }
    }
    
    // Get final status
    const afterStatus = await getPeriodStatus(date, period);
    console.log(`\nFinal Status: ${afterStatus.actual}/${afterStatus.expected} calculations (${afterStatus.percentage.toFixed(2)}%)`);
    console.log(`Processed ${successCount} records successfully, ${failureCount} failures`);
    
    return afterStatus.percentage === 100;
  } catch (error) {
    console.error(`Error processing period ${period}:`, error);
    return false;
  } finally {
    client.release();
  }
}

/**
 * Main function
 */
async function main(): Promise<void> {
  const client = await pool.connect();
  try {
    const success = await processPeriod(TARGET_DATE, TARGET_PERIOD);
    
    if (success) {
      console.log(`\n✅ Successfully processed ${TARGET_DATE} Period ${TARGET_PERIOD}`);
    } else {
      console.log(`\n⚠️ Partially processed ${TARGET_DATE} Period ${TARGET_PERIOD}`);
    }
  } catch (error) {
    console.error('Fatal error:', error);
  } finally {
    client.release();
    await pool.end();
  }
}

// Run the script
main()
  .then(() => {
    process.exit(0);
  })
  .catch(error => {
    console.error('Fatal error:', error);
    process.exit(1);
  });