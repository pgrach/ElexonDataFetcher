import { reconcileDay } from "../services/historicalReconciliation";

async function checkFebruaryData() {
  const dates = ['2025-02-25', '2025-02-26', '2025-02-27'];
  
  console.log('\n=== Starting February 2025 Data Check ===\n');
  
  for (const date of dates) {
    try {
      console.log(`\nChecking data for ${date}...`);
      await reconcileDay(date);
    } catch (error) {
      console.error(`Error processing ${date}:`, error);
    }
  }
  
  console.log('\n=== February 2025 Data Check Complete ===\n');
}

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
  checkFebruaryData()
    .then(() => {
      console.log('Processing complete');
      process.exit(0);
    })
    .catch(error => {
      console.error('Fatal error:', error);
      process.exit(1);
    });
}
