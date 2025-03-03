/**
 * Data Integrity Verification Tool
 * 
 * This script verifies data integrity between curtailment records and 
 * historical bitcoin calculations for a specified date range.
 * 
 * It identifies missing dates and automatically reprocesses them.
 * 
 * Usage:
 *   npx tsx verify_date_integrity.ts [--start YYYY-MM-DD] [--end YYYY-MM-DD] [--auto-fix]
 * 
 * Options:
 *   --start      Start date (default: 7 days ago)
 *   --end        End date (default: today)
 *   --auto-fix   Automatically fix missing data (default: false)
 */

import { db } from "@db";
import { curtailmentRecords, historicalBitcoinCalculations } from "@db/schema";
import { sql, eq, and, between } from "drizzle-orm";
import { format, parseISO, addDays, subDays } from 'date-fns';
import { reprocessDay } from "./server/scripts/reprocessDay";

interface DateStatusItem {
  date: string;
  curtailmentCount: number;
  calculationCount: number;
  status: 'complete' | 'missing_curtailment' | 'missing_calculations' | 'missing_both';
  curtailmentVolume: string | null;
  calculationBitcoin: string | null;
}

// Parse command line arguments
const args = process.argv.slice(2);
const autoFix = args.includes('--auto-fix');
const startArg = args.indexOf('--start');
const endArg = args.indexOf('--end');

const today = new Date();
let startDate = format(subDays(today, 7), 'yyyy-MM-dd');
let endDate = format(today, 'yyyy-MM-dd');

if (startArg !== -1 && args[startArg + 1]) {
  startDate = args[startArg + 1];
}

if (endArg !== -1 && args[endArg + 1]) {
  endDate = args[endArg + 1];
}

console.log(`\n=== Data Integrity Verification ===`);
console.log(`Date Range: ${startDate} to ${endDate}`);
console.log(`Auto-Fix: ${autoFix ? 'Enabled' : 'Disabled'}`);

async function checkDataIntegrity() {
  try {
    // Get curtailment records by date
    const curtailmentStats = await db
      .select({
        settlementDate: curtailmentRecords.settlementDate,
        recordCount: sql<number>`COUNT(*)`,
        totalVolume: sql<string>`SUM(ABS(volume::numeric))::text`
      })
      .from(curtailmentRecords)
      .where(between(curtailmentRecords.settlementDate, startDate, endDate))
      .groupBy(curtailmentRecords.settlementDate);
    
    // Get bitcoin calculation records by date
    const calculationStats = await db
      .select({
        settlementDate: historicalBitcoinCalculations.settlementDate,
        recordCount: sql<number>`COUNT(*)`,
        totalBitcoin: sql<string>`SUM(bitcoin_mined)::text`
      })
      .from(historicalBitcoinCalculations)
      .where(between(historicalBitcoinCalculations.settlementDate, startDate, endDate))
      .groupBy(historicalBitcoinCalculations.settlementDate);
    
    // Create a map of curtailment stats by date
    const curtailmentByDate = new Map<string, { count: number, volume: string | null }>();
    curtailmentStats.forEach(cs => {
      curtailmentByDate.set(cs.settlementDate, {
        count: cs.recordCount,
        volume: cs.totalVolume
      });
    });
    
    // Create a map of calculation stats by date
    const calculationsByDate = new Map<string, { count: number, bitcoin: string | null }>();
    calculationStats.forEach(cs => {
      calculationsByDate.set(cs.settlementDate, {
        count: cs.recordCount,
        bitcoin: cs.totalBitcoin
      });
    });
    
    // Generate a complete set of dates in the range
    const dates: string[] = [];
    let currentDate = parseISO(startDate);
    const finalDate = parseISO(endDate);
    
    while (currentDate <= finalDate) {
      dates.push(format(currentDate, 'yyyy-MM-dd'));
      currentDate = addDays(currentDate, 1);
    }
    
    // Analyze data integrity for each date
    const results: DateStatusItem[] = [];
    
    for (const date of dates) {
      const curtailment = curtailmentByDate.get(date);
      const calculations = calculationsByDate.get(date);
      
      let status: DateStatusItem['status'] = 'complete';
      if (!curtailment && !calculations) {
        status = 'missing_both';
      } else if (!curtailment) {
        status = 'missing_curtailment';
      } else if (!calculations) {
        status = 'missing_calculations';
      }
      
      results.push({
        date,
        curtailmentCount: curtailment?.count || 0,
        calculationCount: calculations?.count || 0,
        status,
        curtailmentVolume: curtailment?.volume || null,
        calculationBitcoin: calculations?.bitcoin || null
      });
    }
    
    // Display results
    console.log("\n--- Date Integrity Results ---");
    results.forEach(r => {
      const statusIcon = r.status === 'complete' ? '✅' : '❌';
      console.log(`${statusIcon} ${r.date}: ${r.curtailmentCount} curtailment records, ${r.calculationCount} calculation records`);
      
      if (r.status !== 'complete') {
        console.log(`   Status: ${r.status}`);
        if (r.curtailmentVolume) {
          console.log(`   Curtailment Volume: ${Number(r.curtailmentVolume).toFixed(2)} MWh`);
        }
        if (r.calculationBitcoin) {
          console.log(`   Calculated Bitcoin: ${Number(r.calculationBitcoin).toFixed(6)} BTC`);
        }
      }
    });
    
    // Process auto-fix if enabled
    const datesToFix = results.filter(r => r.status !== 'complete').map(r => r.date);
    
    if (datesToFix.length > 0) {
      console.log(`\n${datesToFix.length} dates require attention:`);
      datesToFix.forEach(d => console.log(`- ${d}`));
      
      if (autoFix) {
        console.log("\n--- Auto-fixing missing data ---");
        
        for (const date of datesToFix) {
          console.log(`Processing ${date}...`);
          try {
            await reprocessDay(date);
            console.log(`✅ Successfully reprocessed ${date}`);
          } catch (error) {
            console.error(`❌ Failed to reprocess ${date}:`, error);
          }
        }
        
        console.log("\n--- Auto-fix complete ---");
      } else {
        console.log("\nTo fix missing data, run:");
        console.log(`npx tsx verify_date_integrity.ts --start ${startDate} --end ${endDate} --auto-fix`);
      }
    } else {
      console.log("\n✅ All dates in the specified range have complete data!");
    }
    
  } catch (error) {
    console.error("Error checking data integrity:", error);
  }
}

checkDataIntegrity();