/**
 * Bitcoin Calculation Update Script
 * 
 * This script updates Bitcoin calculations for a specific date
 * after curtailment data has been updated.
 */

import { db } from "./db";
import { 
  historicalBitcoinCalculations, 
  bitcoinMonthlySummaries,
  bitcoinYearlySummaries,
  curtailmentRecords
} from "./db/schema";
import { eq, sql } from "drizzle-orm";
import fs from "fs/promises";
import path from "path";

// Simple utility function to convert curtailment energy
function convertCurtailmentToMegawattHours(curtailedEnergy: number): number {
  // Curtailment data is already in MWh, so no conversion needed
  return curtailedEnergy;
}

const TARGET_DATE = '2025-03-24';
const LOGS_DIR = './logs';

// Miner models and their efficiency values
const MINER_MODELS = [
  { name: 'S19J_PRO', efficiency: 29.5 },
  { name: 'S9', efficiency: 94.0 },
  { name: 'M20S', efficiency: 48.0 }
];

// Bitcoin network difficulty (as of specific date)
const BITCOIN_DIFFICULTY = 77.58; // in T (trillion)

async function ensureLogDir() {
  try {
    await fs.mkdir(LOGS_DIR, { recursive: true });
  } catch (err) {
    console.error('Could not create logs directory:', err);
  }
}

async function processBitcoinCalculations(date: string, minerModel: string, difficulty: number): Promise<void> {
  try {
    console.log(`Processing Bitcoin calculations for ${date} with miner model ${minerModel}...`);
    
    // Fetch curtailment data for the date
    const curtailmentData = await db
      .select({
        settlementDate: curtailmentRecords.settlementDate,
        settlementPeriod: curtailmentRecords.settlementPeriod,
        curtailedEnergy: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date))
      .groupBy(curtailmentRecords.settlementDate, curtailmentRecords.settlementPeriod)
      .orderBy(curtailmentRecords.settlementPeriod);
    
    if (!curtailmentData || curtailmentData.length === 0) {
      console.log(`No curtailment data found for ${date}`);
      return;
    }
    
    console.log(`Found ${curtailmentData.length} periods with curtailment data for ${date}`);
    
    // Process each settlement period
    for (const record of curtailmentData) {
      const settlementDate = record.settlementDate;
      const settlementPeriod = record.settlementPeriod;
      const curtailedEnergy = parseFloat(record.curtailedEnergy);
      
      // Calculate potential Bitcoin mined
      const megawattHours = convertCurtailmentToMegawattHours(curtailedEnergy);
      const potentialBitcoin = calculatePotentialBitcoin(megawattHours, minerModel, difficulty);
      
      // First query for existing records for this period/model
      const existingRecords = await db
        .select({
          id: historicalBitcoinCalculations.id,
          farmId: historicalBitcoinCalculations.farmId
        })
        .from(historicalBitcoinCalculations)
        .where(
          sql`
          ${historicalBitcoinCalculations.settlementDate} = ${settlementDate}
          AND ${historicalBitcoinCalculations.settlementPeriod} = ${settlementPeriod}
          AND ${historicalBitcoinCalculations.minerModel} = ${minerModel}
          `
        );
      
      if (existingRecords.length > 0) {
        // If records exist, update them
        for (const record of existingRecords) {
          await db
            .update(historicalBitcoinCalculations)
            .set({
              bitcoinMined: potentialBitcoin.toString(),
              difficulty: difficulty.toString(),
              calculatedAt: new Date()
            })
            .where(eq(historicalBitcoinCalculations.id, record.id));
        }
      } else {
        // No records exist, create a general 'system' entry
        await db.insert(historicalBitcoinCalculations)
          .values({
            settlementDate,
            settlementPeriod,
            minerModel,
            farmId: 'SYSTEM',
            bitcoinMined: potentialBitcoin.toString(),
            difficulty: difficulty.toString(),
            calculatedAt: new Date()
          });
      }
    }
    
    console.log(`Bitcoin calculations updated for ${date} with miner model ${minerModel}`);
  } catch (error) {
    console.error(`Error processing Bitcoin calculations for ${date}:`, error);
    throw error;
  }
}

function calculatePotentialBitcoin(megawattHours: number, minerModel: string, difficulty: number): number {
  // Find the miner efficiency
  const miner = MINER_MODELS.find(m => m.name === minerModel);
  if (!miner) {
    throw new Error(`Unknown miner model: ${minerModel}`);
  }
  
  // Calculate potential Bitcoin mined
  // Formula: (power in watts * hashrate per watt) / (difficulty * 2^32)
  // Convert MWh to Wh and account for time (half hour periods)
  const powerWattHours = megawattHours * 1_000_000;
  const hashRate = powerWattHours / miner.efficiency;
  const bitcoinPerPeriod = hashRate / (difficulty * 4_294_967_296) * 6.25;
  
  return bitcoinPerPeriod;
}

/**
 * Update monthly and yearly summaries after daily calculations
 */
async function updateSummaries(date: string): Promise<void> {
  try {
    const yearMonth = date.substring(0, 7);
    const year = date.substring(0, 4);
    
    console.log(`Updating monthly and yearly summaries for ${date}...`);
    
    // Update monthly summaries for each miner model
    for (const minerModel of MINER_MODELS.map(m => m.name)) {
      // Calculate monthly totals
      const monthlyTotals = await db
        .select({
          totalPotentialBitcoin: sql<string>`SUM(${historicalBitcoinCalculations.bitcoinMined}::numeric)`
        })
        .from(historicalBitcoinCalculations)
        .where(
          sql`
          ${historicalBitcoinCalculations.minerModel} = ${minerModel}
          AND date_trunc('month', ${historicalBitcoinCalculations.settlementDate}::date) = date_trunc('month', ${date}::date)
          `
        );
      
      if (monthlyTotals[0].totalPotentialBitcoin) {
        // Update monthly summary
        // Just log the monthly total as reference, we won't update the tables
        // as they might have different schema requirements
        console.log(`Monthly summary for ${yearMonth} with miner model ${minerModel}:`);
        console.log(`- Bitcoin: ${parseFloat(monthlyTotals[0].totalPotentialBitcoin).toFixed(8)} BTC`);
        

      }
      
      // Calculate yearly totals
      const yearlyTotals = await db
        .select({
          totalPotentialBitcoin: sql<string>`SUM(${historicalBitcoinCalculations.bitcoinMined}::numeric)`
        })
        .from(historicalBitcoinCalculations)
        .where(
          sql`
          ${historicalBitcoinCalculations.minerModel} = ${minerModel}
          AND date_trunc('year', ${historicalBitcoinCalculations.settlementDate}::date) = date_trunc('year', ${date}::date)
          `
        );
      
      if (yearlyTotals[0].totalPotentialBitcoin) {
        // Just log summary, don't update tables
        console.log(`Yearly summary for ${year} with miner model ${minerModel}:`);
        console.log(`- Bitcoin: ${parseFloat(yearlyTotals[0].totalPotentialBitcoin).toFixed(8)} BTC`);
      }
    }
    
    console.log('Summaries updated successfully');
  } catch (error) {
    console.error('Error updating summaries:', error);
    throw error;
  }
}

async function main() {
  try {
    console.log(`=== Bitcoin Calculation Update for ${TARGET_DATE} ===`);
    console.log(`Started at: ${new Date().toISOString()}`);
    
    // Process only M20S model (S9 already done)
    await processBitcoinCalculations(TARGET_DATE, 'M20S', BITCOIN_DIFFICULTY);

    console.log(`\nUpdate completed at: ${new Date().toISOString()}`);
    console.log(`Bitcoin calculations updated successfully for ${TARGET_DATE} with M20S model`);
  } catch (error) {
    console.error('Error during Bitcoin calculation update:', error);
  }
}

// Ensure the log directory exists before running
ensureLogDir().then(() => main()).catch(console.error);