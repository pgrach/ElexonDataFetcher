/**
 * Script to populate missing Bitcoin calculations for 2025-03-24
 * 
 * This script identifies curtailment records that don't have corresponding
 * Bitcoin calculations and populates them for all miner models.
 */

import { db } from '@db';
import { 
  curtailmentRecords,
  historicalBitcoinCalculations
} from '@db/schema';
import { eq, and, not, exists, sql } from 'drizzle-orm';
import { calculateBitcoin } from '../utils/bitcoin';
import { getDifficultyData } from '../services/dynamodbService';
import { calculateMonthlyBitcoinSummary, manualUpdateYearlyBitcoinSummary } from '../services/bitcoinService';

const TARGET_DATE = '2025-03-24';
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

async function findMissingRecords() {
  // Get all curtailment records that don't have any historical Bitcoin calculations
  const missingRecords = await db
    .select({
      settlementDate: curtailmentRecords.settlementDate,
      settlementPeriod: curtailmentRecords.settlementPeriod,
      farmId: curtailmentRecords.farmId,
      volume: curtailmentRecords.volume
    })
    .from(curtailmentRecords)
    .where(
      and(
        eq(curtailmentRecords.settlementDate, TARGET_DATE),
        not(
          exists(
            db
              .select({ id: historicalBitcoinCalculations.id })
              .from(historicalBitcoinCalculations)
              .where(
                and(
                  eq(historicalBitcoinCalculations.settlementDate, curtailmentRecords.settlementDate),
                  eq(historicalBitcoinCalculations.settlementPeriod, curtailmentRecords.settlementPeriod),
                  eq(historicalBitcoinCalculations.farmId, curtailmentRecords.farmId)
                )
              )
          )
        )
      )
    );

  return missingRecords;
}

async function populateMissingCalculations() {
  try {
    console.log(`Looking for missing Bitcoin calculations for ${TARGET_DATE}...`);
    
    // Get missing records
    const missingRecords = await findMissingRecords();
    
    if (missingRecords.length === 0) {
      console.log('No missing Bitcoin calculations found.');
      return;
    }
    
    console.log(`Found ${missingRecords.length} curtailment records without Bitcoin calculations.`);
    
    // Get difficulty for the date
    const difficulty = await getDifficultyData(TARGET_DATE);
    console.log(`Using difficulty ${difficulty} for calculations`);
    
    // Populate calculations for each miner model
    for (const minerModel of MINER_MODELS) {
      console.log(`Processing missing calculations for ${minerModel}...`);
      
      const insertValues = [];
      
      for (const record of missingRecords) {
        // Convert volume to positive for calculation
        const mwh = Math.abs(Number(record.volume));
        
        // Skip records with zero or invalid volume
        if (mwh <= 0 || isNaN(mwh)) {
          continue;
        }
        
        // Calculate Bitcoin mined
        const bitcoinMined = calculateBitcoin(mwh, minerModel, difficulty);
        
        insertValues.push({
          settlementDate: record.settlementDate,
          settlementPeriod: Number(record.settlementPeriod),
          minerModel: minerModel,
          farmId: record.farmId,
          bitcoinMined: bitcoinMined.toString(),
          difficulty: difficulty.toString()
        });
      }
      
      if (insertValues.length > 0) {
        // Insert the calculations
        await db.insert(historicalBitcoinCalculations).values(insertValues);
        console.log(`Inserted ${insertValues.length} calculations for ${minerModel}`);
      }
    }
    
    // Update monthly summaries
    const yearMonth = TARGET_DATE.substring(0, 7); // YYYY-MM
    
    for (const minerModel of MINER_MODELS) {
      await calculateMonthlyBitcoinSummary(yearMonth, minerModel);
      console.log(`Updated monthly summary for ${yearMonth} and ${minerModel}`);
    }
    
    // Update yearly summaries
    const year = TARGET_DATE.substring(0, 4); // YYYY
    await manualUpdateYearlyBitcoinSummary(year);
    console.log(`Updated yearly summary for ${year}`);
    
    console.log('Successfully populated all missing Bitcoin calculations.');
  } catch (error) {
    console.error('Error populating missing calculations:', error);
    throw error;
  }
}

// Run the script
populateMissingCalculations()
  .then(() => {
    console.log('Script completed successfully');
    process.exit(0);
  })
  .catch(error => {
    console.error('Script failed:', error);
    process.exit(1);
  });