/**
 * Production Helper Service
 * 
 * This service provides production-safe equivalents for functions
 * that would normally depend on direct ts-node execution.
 * It eliminates the need for ts-node in production environments.
 */

import { reconcileDay, findDatesWithMissingCalculations } from './historicalReconciliation';
import { format, subDays } from 'date-fns';

/**
 * A production-safe alternative to the daily_reconciliation_check.ts script
 * This function performs the same tasks but without requiring ts-node
 */
export async function runDailyReconciliationCheck(days: number = 2, forceProcess: boolean = false) {
  console.log(`Starting daily reconciliation check for the last ${days} days...`);
  
  try {
    // Get recent dates
    const recentDates: string[] = [];
    const today = new Date();
    
    for (let i = 0; i < days; i++) {
      const date = subDays(today, i);
      recentDates.push(format(date, 'yyyy-MM-dd'));
    }
    
    console.log(`Checking ${recentDates.length} recent dates:`, recentDates);
    
    // Find dates with missing calculations
    const missingDates = await findDatesWithMissingCalculations(days * 2);
    const datesToProcess = new Set<string>();
    
    // Add all recent dates that have missing calculations
    for (const date of recentDates) {
      const hasMissingCalculations = missingDates.some(item => item.date === date);
      
      if (hasMissingCalculations || forceProcess) {
        datesToProcess.add(date);
      }
    }
    
    console.log(`Found ${datesToProcess.size} dates that need processing:`, Array.from(datesToProcess));
    
    // Process each date
    const fixedDates: string[] = [];
    for (const date of datesToProcess) {
      console.log(`Processing date: ${date}`);
      await reconcileDay(date);
      fixedDates.push(date);
    }
    
    return {
      dates: recentDates,
      missingDates: Array.from(datesToProcess),
      fixedDates,
      status: 'completed'
    };
  } catch (error) {
    console.error('Error during daily reconciliation check:', error);
    return {
      dates: [],
      missingDates: [],
      fixedDates: [],
      status: 'failed',
      error
    };
  }
}