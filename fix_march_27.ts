/**
 * Complete March 27 Data (Simple Version)
 * 
 * This script processes the remaining periods (17-48) for March 27, 2025
 * using the existing systems with appropriate API rate limiting.
 */

import { processFullCascade } from './process_bitcoin_optimized';
import { db } from './db';
import { dailySummaries } from './db/schema';
import { eq } from 'drizzle-orm';
import fs from 'fs';
import path from 'path';

// Configuration
const TARGET_DATE = '2025-03-27';
const START_PERIOD = 17; // Start from period 17 (we already processed 1-16)
const END_PERIOD = 24;   // Process a smaller batch (17-24) first
const DELAY_BETWEEN_PERIODS = 5000; // ms
const LOG_FILE = path.join('logs', `complete_march_27_${new Date().toISOString().replace(/:/g, '-')}.log`);

// Create a write stream for the log file
const logStream = fs.createWriteStream(LOG_FILE, { flags: 'a' });

// Override console.log and console.error to also write to the log file
const originalConsoleLog = console.log;
const originalConsoleError = console.error;

console.log = function(message: any, ...args: any[]) {
  const formattedMessage = args.length ? `${message} ${args.join(' ')}` : message;
  originalConsoleLog(formattedMessage);
  logStream.write(`${formattedMessage}\n`);
};

console.error = function(message: any, ...args: any[]) {
  const formattedMessage = args.length ? `${message} ${args.join(' ')}` : message;
  originalConsoleError(formattedMessage);
  logStream.write(`ERROR: ${formattedMessage}\n`);
};

// Sleep utility function
function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Process the hourly data for the remaining periods of March 27
async function processMarch27RemainingPeriods(): Promise<void> {
  try {
    console.log(`\n===== Starting to process remaining periods (${START_PERIOD}-${END_PERIOD}) for ${TARGET_DATE} =====\n`);
    
    // Check what's already in the database
    const existingRecords = await checkExistingData();
    console.log(`Found ${existingRecords.totalRecords} existing records for periods 1-${START_PERIOD-1}`);
    console.log(`Total volume so far: ${existingRecords.totalVolume.toFixed(2)} MWh`);
    console.log(`Total payment so far: £${existingRecords.totalPayment.toFixed(2)}`);
    
    // Process each remaining period individually with a delay between
    let totalNewRecords = 0;
    let totalNewVolume = 0;
    let totalNewPayment = 0;
    
    for (let period = START_PERIOD; period <= END_PERIOD; period++) {
      console.log(`\nProcessing period ${period}...`);
      
      try {
        // Execute a server script to process this period
        const command = `npx tsx server/scripts/processPeriod.ts ${TARGET_DATE} ${period}`;
        const result = await executeCommand(command);
        
        // Extract result information (basic parsing of the output)
        const recordsMatch = result.match(/Processed (\d+) records/i);
        const volumeMatch = result.match(/Total volume: ([\d\.]+) MWh/i);
        const paymentMatch = result.match(/Total payment: £([\d\.]+)/i);
        
        const periodRecords = recordsMatch ? parseInt(recordsMatch[1]) : 0;
        const periodVolume = volumeMatch ? parseFloat(volumeMatch[1]) : 0;
        const periodPayment = paymentMatch ? parseFloat(paymentMatch[1]) : 0;
        
        console.log(`Period ${period} complete: ${periodRecords} records, ${periodVolume.toFixed(2)} MWh, £${periodPayment.toFixed(2)}`);
        
        totalNewRecords += periodRecords;
        totalNewVolume += periodVolume;
        totalNewPayment += periodPayment;
      } catch (error) {
        console.error(`Error processing period ${period}:`, error);
      }
      
      // Wait between periods to avoid hitting API rate limits
      if (period < END_PERIOD) {
        console.log(`Waiting ${DELAY_BETWEEN_PERIODS}ms before next period...`);
        await sleep(DELAY_BETWEEN_PERIODS);
      }
    }
    
    // Update daily summary with the new totals
    const overallTotalVolume = existingRecords.totalVolume + totalNewVolume;
    const overallTotalPayment = existingRecords.totalPayment + totalNewPayment;
    
    console.log(`\n===== Processing complete for ${TARGET_DATE} =====`);
    console.log(`New records added: ${totalNewRecords}`);
    console.log(`New volume added: ${totalNewVolume.toFixed(2)} MWh`);
    console.log(`New payment added: £${totalNewPayment.toFixed(2)}`);
    console.log(`Overall total records: ${existingRecords.totalRecords + totalNewRecords}`);
    console.log(`Overall total volume: ${overallTotalVolume.toFixed(2)} MWh`);
    console.log(`Overall total payment: £${overallTotalPayment.toFixed(2)}`);
    
    // Update the daily summary
    await updateDailySummary(overallTotalVolume, overallTotalPayment);
    
    // Process Bitcoin calculations based on the complete dataset
    console.log('\nProcessing Bitcoin calculations...');
    await processBitcoinCalculations();
    
    console.log(`\n===== All processing complete for ${TARGET_DATE} =====`);
    console.log(`Log file: ${LOG_FILE}`);
  } catch (error) {
    console.error('Error in main process:', error);
  } finally {
    // Restore original console functions and close the log stream
    console.log = originalConsoleLog;
    console.error = originalConsoleError;
    logStream.end();
  }
}

// Check existing data in the database
async function checkExistingData(): Promise<{
  totalRecords: number;
  totalVolume: number;
  totalPayment: number;
}> {
  try {
    // Count the number of records and sum the volume and payment values
    // Using direct SQL query since there are issues with parameterized queries
    const query = `
      SELECT 
        COUNT(*) as total_records,
        SUM(ABS(volume::numeric)) as total_volume,
        SUM(ABS(payment::numeric)) as total_payment
      FROM curtailment_records
      WHERE settlement_date = '${TARGET_DATE}' AND settlement_period < ${START_PERIOD}
    `;
    
    const result = await db.execute(query);
    
    return {
      totalRecords: parseInt(result.rows[0].total_records) || 0,
      totalVolume: parseFloat(result.rows[0].total_volume) || 0,
      totalPayment: parseFloat(result.rows[0].total_payment) || 0
    };
  } catch (error) {
    console.error('Error checking existing data:', error);
    return { totalRecords: 0, totalVolume: 0, totalPayment: 0 };
  }
}

// Update the daily summary with the total values
async function updateDailySummary(totalVolume: number, totalPayment: number): Promise<void> {
  console.log(`\nUpdating daily summary for ${TARGET_DATE}...`);
  
  try {
    // Using direct SQL query since there are issues with parameterized queries
    const query = `
      UPDATE daily_summaries
      SET 
        total_curtailed_energy = '${totalVolume.toString()}',
        total_payment = '${totalPayment.toString()}',
        last_updated = NOW()
      WHERE summary_date = '${TARGET_DATE}'
    `;
    
    await db.execute(query);
    
    console.log(`Daily summary updated successfully: ${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)}`);
  } catch (error) {
    console.error('Error updating daily summary:', error);
  }
}

// Process Bitcoin calculations
async function processBitcoinCalculations(): Promise<void> {
  try {
    await processFullCascade(TARGET_DATE);
    console.log('Bitcoin calculations processed successfully');
  } catch (error) {
    console.error('Error processing Bitcoin calculations:', error);
  }
}

// Execute a command and return the result
async function executeCommand(command: string): Promise<string> {
  return new Promise((resolve, reject) => {
    // Use the Node.js child_process module
    import('child_process').then(({ exec }) => {
      exec(command, (error: Error | null, stdout: string, stderr: string) => {
        if (error) {
          console.error(`Error executing command: ${error.message}`);
          return reject(error);
        }
        
        if (stderr) {
          console.error(`Command stderr: ${stderr}`);
        }
        
        resolve(stdout);
      });
    }).catch(error => {
      console.error('Failed to import child_process:', error);
      reject(error);
    });
  });
}

// Run the script
processMarch27RemainingPeriods().catch(error => {
  console.error('Unhandled error:', error);
  process.exit(1);
});