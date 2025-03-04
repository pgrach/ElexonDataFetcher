/**
 * Enhanced Data Integrity Verification Tool
 * 
 * This script verifies data integrity between curtailment records and 
 * historical bitcoin calculations for a specified date range.
 * 
 * It identifies missing dates, incomplete calculations, and automatically 
 * reprocesses them to ensure each curtailment record has all required bitcoin calculations.
 * 
 * Usage:
 *   npx tsx verify_date_integrity.ts [--start YYYY-MM-DD] [--end YYYY-MM-DD] [--auto-fix] [--verbose]
 * 
 * Options:
 *   --start      Start date (default: 7 days ago)
 *   --end        End date (default: today)
 *   --auto-fix   Automatically fix missing data (default: false)
 *   --verbose    Show detailed output for each date (default: false)
 *   --check-all  Check calculations for all miner models (default: false)
 */

import { db } from "./db";
import { curtailmentRecords, historicalBitcoinCalculations } from "./db/schema";
import { sql, eq, and, between } from "drizzle-orm";
import { format, parseISO, addDays, subDays } from 'date-fns';
import { exec, execSync } from 'child_process';

interface DateStatusItem {
  date: string;
  curtailmentCount: number;
  calculationCount: number;
  s19jProCount: number;
  m20sCount: number;
  s9Count: number;
  missingCalculations: number;
  status: 'complete' | 'missing_curtailment' | 'missing_calculations' | 'missing_both' | 'incomplete_calculations';
  curtailmentVolume: string | null;
  calculationBitcoin: string | null;
}

// Parse command line arguments
const args = process.argv.slice(2);
const autoFix = args.includes('--auto-fix');
const verbose = args.includes('--verbose');
const checkAll = args.includes('--check-all');
const updateConstraints = args.includes('--update-constraints');
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

// Define miner models
const MINER_MODELS = ['S19J_PRO', 'M20S', 'S9'];

console.log(`\n=== Enhanced Data Integrity Verification ===`);
console.log(`Date Range: ${startDate} to ${endDate}`);
console.log(`Auto-Fix: ${autoFix ? 'Enabled' : 'Disabled'}`);
console.log(`Verbose Output: ${verbose ? 'Enabled' : 'Disabled'}`);
console.log(`Check All Models: ${checkAll ? 'Enabled' : 'Disabled'}`);
console.log(`Update Constraints: ${updateConstraints ? 'Enabled' : 'Disabled'}`);
console.log(`Miner Models: ${MINER_MODELS.join(', ')}`);

// If update constraints flag is set, run the migration first
if (updateConstraints) {
  console.log(`\n--- Running Database Constraint Migration ---`);
  try {
    execSync('npx tsx migrate_constraints.ts', { stdio: 'inherit' });
    console.log(`✅ Successfully updated database constraints`);
  } catch (error) {
    console.error(`❌ Failed to update database constraints:`, error);
    process.exit(1);
  }
}

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
      
    // Get bitcoin calculation counts by date and miner model
    const minerModelStats = await db
      .select({
        settlementDate: historicalBitcoinCalculations.settlementDate,
        minerModel: historicalBitcoinCalculations.minerModel,
        recordCount: sql<number>`COUNT(*)`
      })
      .from(historicalBitcoinCalculations)
      .where(between(historicalBitcoinCalculations.settlementDate, startDate, endDate))
      .groupBy(historicalBitcoinCalculations.settlementDate, historicalBitcoinCalculations.minerModel);
    
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
    
    // Create a map of miner model counts by date
    const minerModelByDate = new Map<string, { 
      S19J_PRO: number, 
      M20S: number, 
      S9: number
    }>();
    
    minerModelStats.forEach(ms => {
      const dateStats = minerModelByDate.get(ms.settlementDate) || { S19J_PRO: 0, M20S: 0, S9: 0 };
      
      if (ms.minerModel === 'S19J_PRO') {
        dateStats.S19J_PRO = ms.recordCount;
      } else if (ms.minerModel === 'M20S') {
        dateStats.M20S = ms.recordCount;
      } else if (ms.minerModel === 'S9') {
        dateStats.S9 = ms.recordCount;
      }
      
      minerModelByDate.set(ms.settlementDate, dateStats);
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
      const minerModels = minerModelByDate.get(date) || { S19J_PRO: 0, M20S: 0, S9: 0 };
      
      // Calculate expected vs actual calculations
      const expectedCalculations = (curtailment?.count || 0) * 3; // 3 miner models per curtailment record
      const actualCalculations = calculations?.count || 0;
      const missingCalculations = Math.max(0, expectedCalculations - actualCalculations);
      
      let status: DateStatusItem['status'] = 'complete';
      if (!curtailment && !calculations) {
        status = 'missing_both';
      } else if (!curtailment) {
        status = 'missing_curtailment';
      } else if (!calculations) {
        status = 'missing_calculations';
      } else if (missingCalculations > 0) {
        status = 'incomplete_calculations';
      }
      
      results.push({
        date,
        curtailmentCount: curtailment?.count || 0,
        calculationCount: actualCalculations,
        s19jProCount: minerModels.S19J_PRO,
        m20sCount: minerModels.M20S,
        s9Count: minerModels.S9,
        missingCalculations,
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
        
        if (r.status === 'incomplete_calculations') {
          console.log(`   Expected: ${r.curtailmentCount * 3} calculations (${r.curtailmentCount} records × 3 miner models)`);
          console.log(`   Actual: ${r.calculationCount} calculations`);
          console.log(`   Missing: ${r.missingCalculations} calculations`);
          console.log(`   Miner Models: S19J_PRO (${r.s19jProCount}), M20S (${r.m20sCount}), S9 (${r.s9Count})`);
        }
        
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
          const dateResult = results.find(r => r.date === date);
          const isIncompleteCalculations = dateResult?.status === 'incomplete_calculations';
          
          console.log(`Processing ${date}...`);
          try {
            if (isIncompleteCalculations) {
              // For incomplete calculations, we need to regenerate them with our new tool
              console.log(`Using regenerate_bitcoin_calculations tool for incomplete calculations...`);
              await new Promise<void>((resolve, reject) => {
                exec(`npx tsx regenerate_bitcoin_calculations.ts --start ${date} --end ${date} --fix`, (error, stdout, stderr) => {
                  if (error) {
                    console.error(`Error executing regenerate_bitcoin_calculations: ${error.message}`);
                    reject(error);
                    return;
                  }
                  if (stderr) {
                    console.error(`regenerate_bitcoin_calculations stderr: ${stderr}`);
                  }
                  console.log(stdout);
                  resolve();
                });
              });
            } else {
              // For missing data, use the standard reprocessDay script
              await new Promise<void>((resolve, reject) => {
                exec(`npx tsx server/scripts/reprocessDay.ts ${date}`, (error, stdout, stderr) => {
                  if (error) {
                    console.error(`Error executing reprocessDay: ${error.message}`);
                    reject(error);
                    return;
                  }
                  if (stderr) {
                    console.error(`reprocessDay stderr: ${stderr}`);
                  }
                  console.log(stdout);
                  resolve();
                });
              });
            }
            
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