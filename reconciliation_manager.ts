/**
 * Reconciliation Manager
 * 
 * A comprehensive solution for ensuring 100% reconciliation between 
 * curtailment_records and historical_bitcoin_calculations tables.
 * 
 * This tool integrates both efficient batch processing and comprehensive 
 * timeout diagnostics to provide a complete reconciliation solution.
 * 
 * Usage:
 * npx tsx reconciliation_manager.ts [command] [options]
 * 
 * Commands:
 *   status            - Show current reconciliation status
 *   analyze           - Analyze missing calculations and diagnose potential issues
 *   fix               - Fix missing calculations using optimized batch processing
 *   diagnose          - Run diagnostics on database connections and timeout issues
 *   schedule          - Schedule regular reconciliation checks
 */

import { spawn } from 'child_process';
import { format } from 'date-fns';
import fs from 'fs';
import path from 'path';
import readline from 'readline';

// Constants
const LOG_DIR = './logs';
const LOG_FILE = path.join(LOG_DIR, `reconciliation_${format(new Date(), 'yyyy-MM-dd')}.log`);
const TOOLS = {
  EFFICIENT: './efficient_reconciliation.ts',
  ANALYZER: './connection_timeout_analyzer.ts',
  SIMPLE: './simple_reconcile.ts'
};

// Ensure log directory exists
if (!fs.existsSync(LOG_DIR)) {
  fs.mkdirSync(LOG_DIR, { recursive: true });
}

/**
 * Log function with timestamp
 */
function log(message: string, level: 'info' | 'warning' | 'error' | 'success' = 'info'): void {
  const timestamp = new Date().toISOString();
  const formattedMessage = `[${timestamp}] [${level.toUpperCase()}] ${message}`;
  
  // Console output with colors
  switch (level) {
    case 'error':
      console.error('\x1b[31m%s\x1b[0m', formattedMessage);
      break;
    case 'warning':
      console.warn('\x1b[33m%s\x1b[0m', formattedMessage);
      break;
    case 'success':
      console.log('\x1b[32m%s\x1b[0m', formattedMessage);
      break;
    default:
      console.log('\x1b[36m%s\x1b[0m', formattedMessage);
  }
  
  // Append to log file
  fs.appendFileSync(LOG_FILE, formattedMessage + '\n');
}

/**
 * Run a child process with output logged to console and file
 */
async function runProcess(command: string, args: string[] = []): Promise<{ success: boolean; output: string[] }> {
  return new Promise((resolve) => {
    log(`Running: npx tsx ${command} ${args.join(' ')}`, 'info');
    
    const outputs: string[] = [];
    const child = spawn('npx', ['tsx', command, ...args], { stdio: ['inherit', 'pipe', 'pipe'] });
    
    child.stdout.on('data', (data) => {
      const output = data.toString().trim();
      console.log(output);
      outputs.push(output);
      fs.appendFileSync(LOG_FILE, output + '\n');
    });
    
    child.stderr.on('data', (data) => {
      const output = data.toString().trim();
      console.error('\x1b[31m%s\x1b[0m', output);
      outputs.push(`ERROR: ${output}`);
      fs.appendFileSync(LOG_FILE, `ERROR: ${output}\n`);
    });
    
    child.on('close', (code) => {
      const success = code === 0;
      if (success) {
        log(`Process completed successfully`, 'success');
      } else {
        log(`Process exited with code ${code}`, 'error');
      }
      resolve({ success, output: outputs });
    });
  });
}

/**
 * Check current reconciliation status
 */
async function checkStatus(): Promise<void> {
  log('=== Checking Reconciliation Status ===', 'info');
  
  const { success, output } = await runProcess(TOOLS.EFFICIENT, ['status']);
  
  if (success) {
    // Parse the output to extract reconciliation percentage
    const percentageMatch = output.join('\n').match(/Reconciliation: ([\d.]+)%/);
    const percentage = percentageMatch ? parseFloat(percentageMatch[1]) : 0;
    
    if (percentage === 100) {
      log('✅ Reconciliation status: 100% - All records are reconciled.', 'success');
    } else {
      log(`⚠️ Reconciliation status: ${percentage}% - Some records need reconciliation.`, 'warning');
      log('Run "npx tsx reconciliation_manager.ts fix" to fix missing calculations.', 'info');
    }
  } else {
    log('Failed to get reconciliation status.', 'error');
  }
}

/**
 * Analyze current reconciliation status and diagnose issues
 */
async function analyzeReconciliation(): Promise<void> {
  log('=== Analyzing Reconciliation Status ===', 'info');
  
  // First, get a detailed analysis from the efficient reconciliation tool
  await runProcess(TOOLS.EFFICIENT, ['analyze']);
  
  // Then, check for any connection issues
  log('\n=== Running Connection Diagnostics ===', 'info');
  await runProcess(TOOLS.ANALYZER, ['test']);
  
  log('\n=== Reconciliation Analysis Complete ===', 'info');
  log('To fix missing calculations, run: npx tsx reconciliation_manager.ts fix', 'info');
  log('For detailed connection diagnostics, run: npx tsx reconciliation_manager.ts diagnose', 'info');
}

/**
 * Fix missing calculations with optimized batch processing
 */
async function fixReconciliation(batchSize?: string): Promise<void> {
  log('=== Fixing Missing Calculations ===', 'info');
  
  const args = ['reconcile'];
  if (batchSize) {
    args.push(batchSize);
  }
  
  const result = await runProcess(TOOLS.EFFICIENT, args);
  
  if (result.success) {
    log('Reconciliation process completed.', 'success');
    
    // Check final status
    await checkStatus();
  } else {
    log('Reconciliation process failed or was interrupted.', 'error');
    log('You can resume the process by running: npx tsx efficient_reconciliation.ts resume', 'info');
  }
}

/**
 * Run thorough database connection diagnostics
 */
async function runDiagnostics(): Promise<void> {
  log('=== Running Database Connection Diagnostics ===', 'info');
  
  await runProcess(TOOLS.ANALYZER, ['analyze']);
  
  log('\n=== Connection Diagnostics Complete ===', 'info');
  log('Review the logs for timeout prevention recommendations.', 'info');
}

/**
 * Schedule regular reconciliation checks
 */
async function scheduleReconciliation(): Promise<void> {
  // Print instructions for scheduling with system tools instead of actually implementing
  // a scheduler, as that would require running continuously
  log('=== Reconciliation Scheduling Options ===', 'info');
  log('\nTo schedule regular reconciliation checks, you have several options:', 'info');
  
  log('\n1. Add to crontab (Linux/Mac):', 'info');
  log('   # Run daily reconciliation check at 2 AM', 'info');
  log('   0 2 * * * cd /path/to/project && npx tsx daily_reconciliation_check.ts >> /path/to/logs/reconciliation_$(date +\\%Y-\\%m-\\%d).log 2>&1', 'info');
  
  log('\n2. Add to Windows Task Scheduler:', 'info');
  log('   Create a .bat file with:', 'info');
  log('   cd C:\\path\\to\\project', 'info');
  log('   npx tsx daily_reconciliation_check.ts >> logs\\reconciliation_%date:~0,4%-%date:~5,2%-%date:~8,2%.log 2>&1', 'info');
  
  log('\n3. Set up dedicated server monitoring:', 'info');
  log('   npx tsx efficient_reconciliation.ts monitor', 'info');
  
  log('\nRecommended Schedule:', 'info');
  log('- Daily quick check: Run daily_reconciliation_check.ts every day at off-peak hours', 'info');
  log('- Weekly full reconciliation: Run reconciliation_manager.ts fix weekly', 'info');
  log('- Monthly diagnostics: Run reconciliation_manager.ts diagnose monthly', 'info');
  
  log('\nWould you like to set up a simple monitoring process now? (y/n)', 'info');
  
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
  });
  
  rl.question('', async (answer) => {
    if (answer.toLowerCase() === 'y' || answer.toLowerCase() === 'yes') {
      rl.close();
      log('Starting reconciliation monitoring...', 'info');
      await runProcess(TOOLS.EFFICIENT, ['monitor']);
    } else {
      rl.close();
      log('Scheduling skipped. You can manually set up scheduling as described above.', 'info');
    }
  });
}

/**
 * Fix a specific date
 */
async function fixDate(date: string): Promise<void> {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
    log('Error: Invalid date format. Use YYYY-MM-DD', 'error');
    return;
  }
  
  log(`=== Fixing Reconciliation for ${date} ===`, 'info');
  
  const result = await runProcess(TOOLS.EFFICIENT, ['date', date]);
  
  if (result.success) {
    log(`Successfully processed ${date}`, 'success');
  } else {
    log(`Failed to process ${date}`, 'error');
    log('Try running with smaller batch sizes or check diagnostics.', 'info');
  }
}

/**
 * Fix a date range
 */
async function fixDateRange(startDate: string, endDate: string, batchSize?: string): Promise<void> {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(startDate) || !/^\d{4}-\d{2}-\d{2}$/.test(endDate)) {
    log('Error: Invalid date format. Use YYYY-MM-DD', 'error');
    return;
  }
  
  log(`=== Fixing Reconciliation for Range ${startDate} to ${endDate} ===`, 'info');
  
  const args = ['range', startDate, endDate];
  if (batchSize) {
    args.push(batchSize);
  }
  
  const result = await runProcess(TOOLS.EFFICIENT, args);
  
  if (result.success) {
    log(`Successfully processed date range ${startDate} to ${endDate}`, 'success');
  } else {
    log(`Failed to completely process date range ${startDate} to ${endDate}`, 'error');
    log('Try running with smaller batch sizes or check diagnostics.', 'info');
  }
}

/**
 * Display help menu
 */
function showHelp(): void {
  log('\nReconciliation Manager - Comprehensive Reconciliation Solution', 'info');
  log('\nCommands:', 'info');
  log('  status                         - Show current reconciliation status', 'info');
  log('  analyze                        - Analyze missing calculations and diagnose issues', 'info');
  log('  fix [batch-size]               - Fix missing calculations with batch processing', 'info');
  log('  date YYYY-MM-DD                - Fix a specific date', 'info');
  log('  range YYYY-MM-DD YYYY-MM-DD [batch-size] - Fix a date range', 'info');
  log('  diagnose                       - Run diagnostics on database connections', 'info');
  log('  schedule                       - Get scheduling options for regular checks', 'info');
  log('  help                           - Show this help menu', 'info');
  log('\nExamples:', 'info');
  log('  npx tsx reconciliation_manager.ts status', 'info');
  log('  npx tsx reconciliation_manager.ts fix 5', 'info');
  log('  npx tsx reconciliation_manager.ts date 2023-12-25', 'info');
  log('  npx tsx reconciliation_manager.ts range 2023-12-01 2023-12-31 3', 'info');
  log('\nFor more detailed options, see the specific tool documentation:', 'info');
  log('  npx tsx efficient_reconciliation.ts help', 'info');
  log('  npx tsx connection_timeout_analyzer.ts help', 'info');
}

/**
 * Main function to handle command line arguments
 */
async function main() {
  try {
    const command = process.argv[2]?.toLowerCase() || 'status';
    const param1 = process.argv[3];
    const param2 = process.argv[4];
    const param3 = process.argv[5];
    
    log(`Starting reconciliation manager with command: ${command}`, 'info');
    
    switch (command) {
      case "status":
        await checkStatus();
        break;
        
      case "analyze":
        await analyzeReconciliation();
        break;
        
      case "fix":
        await fixReconciliation(param1);
        break;
        
      case "date":
        await fixDate(param1);
        break;
        
      case "range":
        await fixDateRange(param1, param2, param3);
        break;
        
      case "diagnose":
        await runDiagnostics();
        break;
        
      case "schedule":
        await scheduleReconciliation();
        break;
        
      case "help":
        showHelp();
        break;
        
      default:
        log(`Unknown command: ${command}`, 'error');
        showHelp();
    }
  } catch (error) {
    log(`Fatal error: ${error}`, 'error');
    throw error;
  }
}

// Run the main function if script is executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
  main()
    .then(() => {
      if (process.argv[2]?.toLowerCase() !== 'schedule') {
        log('\n=== Reconciliation Manager Complete ===', 'success');
      }
    })
    .catch(error => {
      log(`Fatal error: ${error}`, 'error');
      process.exit(1);
    });
}

export {
  checkStatus,
  analyzeReconciliation,
  fixReconciliation,
  runDiagnostics,
  fixDate,
  fixDateRange
};