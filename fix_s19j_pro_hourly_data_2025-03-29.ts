/**
 * Fix S19J PRO Hourly Data for 2025-03-29
 * 
 * This script properly distributes the S19J_PRO Bitcoin calculations from the
 * temporary 'T' farm to actual farms in the curtailment_records table.
 * This will fix the hourly comparison view in the frontend for March 29, 2025.
 * 
 * Usage:
 *   npx tsx fix_s19j_pro_hourly_data_2025-03-29.ts
 */

import { db } from './db';
import { curtailmentRecords, historicalBitcoinCalculations } from './db/schema';
import { eq, and, sql } from 'drizzle-orm';
import fs from 'fs';
import { format } from 'date-fns';

// Setup logging
const LOG_FILE = `./logs/fix_s19j_pro_hourly_data_${format(new Date(), 'yyyy-MM-dd')}.log`;

async function log(message: string, level: "info" | "error" | "warning" | "success" = "info"): Promise<void> {
  const timestamp = new Date().toISOString();
  const logMessage = `[${level.toUpperCase()}] [${timestamp}] ${message}`;
  
  console.log(logMessage);
  
  // Append to log file
  await fs.promises.appendFile(LOG_FILE, logMessage + '\n');
}

async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function main(): Promise<void> {
  try {
    const targetDate = '2025-03-29';
    const minerModel = 'S19J_PRO';
    
    await log(`Starting fix for ${minerModel} hourly data on ${targetDate}`, "info");

    // First, check if we have the problematic 'T' farm data
    const existingCalculations = await db.query.historicalBitcoinCalculations.findMany({
      where: and(
        eq(historicalBitcoinCalculations.settlementDate, targetDate),
        eq(historicalBitcoinCalculations.minerModel, minerModel),
        eq(historicalBitcoinCalculations.farmId, 'T')
      ),
      orderBy: historicalBitcoinCalculations.settlementPeriod
    });

    await log(`Found ${existingCalculations.length} existing '${minerModel}' calculations for farm 'T'`, "info");
    
    if (existingCalculations.length === 0) {
      await log("No problematic records found, nothing to fix.", "info");
      return;
    }

    // Get the daily total to preserve using raw SQL
    const dailySummaryResult = await db.execute(sql`
      SELECT bitcoin_mined, average_difficulty 
      FROM bitcoin_daily_summaries
      WHERE miner_model = ${minerModel} AND summary_date = ${targetDate}
      LIMIT 1
    `);

    if (!dailySummaryResult.rows || dailySummaryResult.rows.length === 0) {
      await log("Could not find the daily summary to maintain consistency", "error");
      return;
    }

    const dailySummary = {
      bitcoinMined: dailySummaryResult.rows[0].bitcoin_mined,
      averageDifficulty: dailySummaryResult.rows[0].average_difficulty
    };

    await log(`Daily summary shows ${dailySummary.bitcoinMined} BTC for ${minerModel}`, "info");

    // Get all farms with curtailment records for this date
    const uniqueFarms = await db
      .select({
        farmId: curtailmentRecords.farmId,
        leadPartyName: curtailmentRecords.leadPartyName
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, targetDate))
      .groupBy(curtailmentRecords.farmId, curtailmentRecords.leadPartyName);

    await log(`Found ${uniqueFarms.length} farms with curtailment records`, "info");

    // Get existing M20S calculations as a model for distributing S19J_PRO
    const m20sCalculations = await db.query.historicalBitcoinCalculations.findMany({
      where: and(
        eq(historicalBitcoinCalculations.settlementDate, targetDate),
        eq(historicalBitcoinCalculations.minerModel, 'M20S')
      ),
      orderBy: [
        historicalBitcoinCalculations.settlementPeriod,
        historicalBitcoinCalculations.farmId
      ]
    });

    await log(`Found ${m20sCalculations.length} M20S calculations to use as a template`, "info");

    if (m20sCalculations.length === 0) {
      await log("No M20S records found to use as a template for distribution", "error");
      return;
    }

    // Delete existing problematic records
    await db.delete(historicalBitcoinCalculations)
      .where(and(
        eq(historicalBitcoinCalculations.settlementDate, targetDate),
        eq(historicalBitcoinCalculations.minerModel, minerModel),
        eq(historicalBitcoinCalculations.farmId, 'T')
      ));

    await log(`Deleted ${existingCalculations.length} problematic '${minerModel}' calculations`, "info");

    // Calculate scaling factor to preserve total
    const totalM20SBitcoin = m20sCalculations.reduce((sum, calc) => 
      sum + Number(calc.bitcoinMined), 0);
    
    const scaleFactor = Number(dailySummary.bitcoinMined) / totalM20SBitcoin;
    
    await log(`Using scale factor: ${scaleFactor.toFixed(6)} to maintain daily total of ${dailySummary.bitcoinMined} BTC`, "info");

    // Create new records based on M20S distribution but scaled
    const newRecords = m20sCalculations.map(m20sCalc => ({
      settlementDate: m20sCalc.settlementDate,
      settlementPeriod: m20sCalc.settlementPeriod,
      farmId: m20sCalc.farmId,
      minerModel: minerModel,
      bitcoinMined: Number(m20sCalc.bitcoinMined) * scaleFactor,
      difficulty: dailySummary.averageDifficulty,
      calculatedAt: new Date()
    }));

    await log(`Created ${newRecords.length} new ${minerModel} records based on M20S distribution`, "info");

    // Insert the new records in batches
    const BATCH_SIZE = 100;
    let insertedCount = 0;

    for (let i = 0; i < newRecords.length; i += BATCH_SIZE) {
      const batch = newRecords.slice(i, i + BATCH_SIZE);
      await db.insert(historicalBitcoinCalculations).values(batch);
      insertedCount += batch.length;
      await log(`Inserted batch of ${batch.length} records (${insertedCount}/${newRecords.length})`, "info");
      await delay(500); // Small delay to avoid overwhelming the database
    }

    // Verify the fix
    const fixedCalculations = await db.query.historicalBitcoinCalculations.findMany({
      where: and(
        eq(historicalBitcoinCalculations.settlementDate, targetDate),
        eq(historicalBitcoinCalculations.minerModel, minerModel)
      )
    });

    const totalFixed = fixedCalculations.reduce((sum, calc) => 
      sum + Number(calc.bitcoinMined), 0);

    await log(`Fix completed. New record count: ${fixedCalculations.length}`, "success");
    await log(`Total BTC in new records: ${totalFixed.toFixed(8)} (expected: ${dailySummary.bitcoinMined})`, "success");
    
    // Now let's verify we can successfully join with lead parties
    const leadPartyCheck = await db
      .select({
        leadPartyName: curtailmentRecords.leadPartyName,
        count: sql<number>`COUNT(*)`
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
        eq(historicalBitcoinCalculations.settlementDate, targetDate),
        eq(historicalBitcoinCalculations.minerModel, minerModel)
      ))
      .groupBy(curtailmentRecords.leadPartyName);
    
    await log("Lead party distribution for the new calculations:", "info");
    for (const party of leadPartyCheck) {
      await log(`- ${party.leadPartyName}: ${party.count} records`, "info");
    }

    await log("Fix completed successfully!", "success");
  } catch (error) {
    await log(`Error: ${error instanceof Error ? error.message : String(error)}`, "error");
    if (error instanceof Error && error.stack) {
      await log(`Stack trace: ${error.stack}`, "error");
    }
    process.exit(1);
  }
}

main().catch(console.error);