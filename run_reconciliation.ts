/**
 * Run Historical Reconciliation
 * 
 * This script manually runs the historical reconciliation for a specific date.
 */
import { processDate } from './server/services/historicalReconciliation';

// We'll process 2025-03-27
const date = '2025-03-27';

async function main() {
  console.log(`Running historical reconciliation for ${date}...`);
  
  try {
    const result = await processDate(date);
    
    if (result.success) {
      console.log(`✅ Reconciliation successful: ${result.message}`);
    } else {
      console.error(`⚠️ Reconciliation had issues: ${result.message}`);
    }
  } catch (error) {
    console.error(`❌ Error during reconciliation:`, error);
  }
}

main().catch(console.error);