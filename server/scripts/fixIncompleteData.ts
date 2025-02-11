import { reprocessDay } from "../services/historicalReconciliation";

const DATE_TO_FIX = '2025-02-11';

console.log(`\n=== Starting Data Fix for ${DATE_TO_FIX} ===`);

reprocessDay(DATE_TO_FIX)
  .then(() => {
    console.log(`\n=== Completed Data Fix for ${DATE_TO_FIX} ===`);
    process.exit(0);
  })
  .catch(error => {
    console.error('Error fixing data:', error);
    process.exit(1);
  });