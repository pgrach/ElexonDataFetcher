// Simple script to reprocess data for March 3, 2025
import { exec } from 'child_process';
import util from 'util';

const execPromise = util.promisify(exec);

async function reingestMarch3() {
  try {
    console.log('Starting data reingestion for March 3, 2025');
    
    console.log('Running ingestMonthlyData.ts for 2025-03 (day 3 only)');
    await execPromise('npx tsx server/scripts/ingestMonthlyData.ts 2025-03 3 3');
    
    console.log('Reingestion complete for March 3, 2025!');
  } catch (error) {
    console.error('Error during reingestion:', error);
    process.exit(1);
  }
}

reingestMarch3();