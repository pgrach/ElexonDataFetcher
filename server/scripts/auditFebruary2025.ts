import { format, eachDayOfInterval, parseISO } from 'date-fns';
import { db } from "@db";
import { historicalBitcoinCalculations } from "@db/schema";
import { processHistoricalCalculations } from "../services/bitcoinService";
import { sql, eq } from "drizzle-orm";
import pLimit from 'p-limit';

const START_DATE = '2025-02-01';
const END_DATE = '2025-02-28';
const BATCH_SIZE = 3;
const limit = pLimit(BATCH_SIZE);
const API_RATE_LIMIT = 250;

async function delay(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function verifyBitcoinCalculations(date: string): Promise<{ needsUpdate: boolean, error?: string }> {
  try {
    console.log(`\nVerifying Bitcoin calculations for ${date}...`);

    // Check if we have calculations for all periods and miner models
    const calculations = await db
      .select({
        periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
        minerCount: sql<number>`COUNT(DISTINCT miner_model)`
      })
      .from(historicalBitcoinCalculations)
      .where(eq(historicalBitcoinCalculations.settlementDate, date));

    if (!calculations[0] || calculations[0].periodCount === 0) {
      return { needsUpdate: true, error: 'No Bitcoin calculations found' };
    }

    console.log(`[${date}] Found ${calculations[0].periodCount} periods with ${calculations[0].minerCount} miner models`);

    // For now, assume we need minimum 48 periods * 4 miner models = 192 records
    const expectedMinRecords = 192;
    const needsUpdate = calculations[0].periodCount * calculations[0].minerCount < expectedMinRecords;

    return { needsUpdate };
  } catch (error) {
    console.error(`Error verifying Bitcoin calculations for ${date}:`, error);
    return { needsUpdate: true, error: String(error) };
  }
}

async function auditBitcoinCalculations() {
  try {
    console.log(`\n=== Starting February 2025 Bitcoin Calculations Audit ===\n`);

    const dates = eachDayOfInterval({
      start: parseISO(START_DATE),
      end: parseISO(END_DATE)
    }).map(date => format(date, 'yyyy-MM-dd'));

    let updatedDates: string[] = [];
    let errorDates: string[] = [];

    // Process dates in smaller batches
    for (let i = 0; i < dates.length; i += BATCH_SIZE) {
      const batchDates = dates.slice(i, i + BATCH_SIZE);

      // Process each date in the batch
      const results = await Promise.all(
        batchDates.map(date => limit(async () => {
          try {
            const verification = await verifyBitcoinCalculations(date);

            if (verification.error) {
              console.log(`[${date}] Error: ${verification.error}`);
              if (verification.error === 'No Bitcoin calculations found') {
                console.log(`[${date}] Processing Bitcoin calculations...`);
                await processHistoricalCalculations(date, date);
                updatedDates.push(date);
                return { date, status: 'updated' };
              } else {
                errorDates.push(date);
                return { date, status: 'error' };
              }
            }

            if (verification.needsUpdate) {
              console.log(`[${date}] ⚠️ Incomplete calculations - reprocessing...`);
              await processHistoricalCalculations(date, date);
              updatedDates.push(date);
              return { date, status: 'updated' };
            }

            console.log(`[${date}] ✓ Bitcoin calculations complete`);
            return { date, status: 'correct' };
          } catch (error) {
            console.error(`Error processing ${date}:`, error);
            errorDates.push(date);
            return { date, status: 'error' };
          }
        }))
      );

      // Print progress
      const progress = ((i + BATCH_SIZE) / dates.length * 100).toFixed(1);
      console.log(`\nProgress: ${progress}% (${i + BATCH_SIZE}/${dates.length} days)`);

      // Add delay between batches
      if (i + BATCH_SIZE < dates.length) {
        await delay(API_RATE_LIMIT * 2);
      }
    }

    // Print summary
    console.log('\n=== Audit Summary ===');
    console.log(`Total days processed: ${dates.length}`);
    console.log(`Days with complete calculations: ${dates.length - updatedDates.length - errorDates.length}`);
    console.log(`Days updated: ${updatedDates.length}`);
    console.log(`Days with errors: ${errorDates.length}`);

    if (updatedDates.length > 0) {
      console.log('\nUpdated dates:', updatedDates.join(', '));
    }
    if (errorDates.length > 0) {
      console.log('\nError dates:', errorDates.join(', '));
    }

    return {
      totalDays: dates.length,
      completeDays: dates.length - updatedDates.length - errorDates.length,
      updatedDays: updatedDates.length,
      errorDays: errorDates.length,
      updatedDates,
      errorDates
    };
  } catch (error) {
    console.error('Error during Bitcoin calculations audit:', error);
    throw error;
  }
}

// Run the audit if this script is executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
  auditBitcoinCalculations()
    .then(results => {
      console.log('\n=== Audit Complete ===');
      process.exit(0);
    })
    .catch(error => {
      console.error('Fatal error:', error);
      process.exit(1);
    });
}

export { auditBitcoinCalculations };