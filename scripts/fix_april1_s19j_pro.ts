/**
 * Fix Bitcoin calculations for April 1, 2025 (S19J_PRO only)
 */

import { db } from "../db";
import { historicalBitcoinCalculations, curtailmentRecords } from "../db/schema";
import { eq, and, sql } from "drizzle-orm";
import { calculateBitcoin } from "../server/utils/bitcoin";

// Target date and miner model
const TARGET_DATE = "2025-04-01";
const MINER_MODEL = "S19J_PRO";
const DIFFICULTY = 113757508810853; // Use the known difficulty value directly

async function main() {
  try {
    console.log(`\n===== FIXING S19J_PRO BITCOIN CALCULATIONS FOR ${TARGET_DATE} =====\n`);

    // Get summary of curtailment records
    const curtailmentStats = await db.select({
      totalRecords: sql<number>`COUNT(*)::int`,
      totalEnergy: sql<string>`SUM(ABS(volume))::text`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));

    console.log(`Curtailment records:`, {
      records: curtailmentStats[0].totalRecords,
      energy: Number(curtailmentStats[0].totalEnergy).toFixed(2) + " MWh"
    });

    // Check existing calculations
    const existingCalcs = await db.select({
      totalRecords: sql<number>`COUNT(*)::int`,
      totalBitcoin: sql<string>`SUM(bitcoin_mined::numeric)::text`
    })
    .from(historicalBitcoinCalculations)
    .where(
      and(
        eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
        eq(historicalBitcoinCalculations.minerModel, MINER_MODEL)
      )
    );

    console.log(`Current ${MINER_MODEL} calculations:`, {
      records: existingCalcs[0]?.totalRecords || 0,
      bitcoin: existingCalcs[0]?.totalBitcoin || "0"
    });

    // Delete existing calculations
    await db.delete(historicalBitcoinCalculations)
      .where(
        and(
          eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
          eq(historicalBitcoinCalculations.minerModel, MINER_MODEL)
        )
      );
    
    console.log(`Deleted existing calculations`);
    
    // Get all curtailment records for this date
    const records = await db.select({
      settlementPeriod: curtailmentRecords.settlementPeriod,
      farmId: curtailmentRecords.farmId,
      volume: curtailmentRecords.volume
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log(`Processing ${records.length} curtailment records`);
    
    // Calculate Bitcoin for each record
    let totalBitcoin = 0;
    const batch = [];
    
    for (const record of records) {
      const mwh = Math.abs(Number(record.volume));
      
      if (mwh <= 0 || isNaN(mwh)) {
        continue;
      }
      
      const bitcoinMined = calculateBitcoin(mwh, MINER_MODEL, DIFFICULTY);
      totalBitcoin += bitcoinMined;
      
      batch.push({
        settlementDate: TARGET_DATE,
        settlementPeriod: Number(record.settlementPeriod),
        minerModel: MINER_MODEL,
        farmId: record.farmId,
        bitcoinMined: bitcoinMined.toString(),
        difficulty: DIFFICULTY.toString()
      });
      
      // Insert in batches of 50 to avoid overwhelming the database
      if (batch.length >= 50) {
        await db.insert(historicalBitcoinCalculations).values(batch);
        console.log(`Inserted batch of ${batch.length} records`);
        batch.length = 0;
      }
    }
    
    // Insert any remaining records
    if (batch.length > 0) {
      await db.insert(historicalBitcoinCalculations).values(batch);
      console.log(`Inserted final batch of ${batch.length} records`);
    }
    
    // Verify the new calculations
    const newCalcs = await db.select({
      totalRecords: sql<number>`COUNT(*)::int`,
      totalBitcoin: sql<string>`SUM(bitcoin_mined::numeric)::text`
    })
    .from(historicalBitcoinCalculations)
    .where(
      and(
        eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
        eq(historicalBitcoinCalculations.minerModel, MINER_MODEL)
      )
    );
    
    console.log(`\n=== Results ===`);
    console.log(`Updated ${MINER_MODEL} calculations:`, {
      records: newCalcs[0]?.totalRecords || 0,
      bitcoin: newCalcs[0]?.totalBitcoin || "0"
    });
    
    console.log(`\n===== FIX COMPLETED =====`);
    
    process.exit(0);
  } catch (error) {
    console.error("ERROR FIXING BITCOIN CALCULATIONS:", error);
    process.exit(1);
  }
}

// Run the fix
main()
  .catch(error => {
    console.error("UNHANDLED ERROR:", error);
    process.exit(1);
  });