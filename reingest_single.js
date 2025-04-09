import { runFullUpdate } from './server/scripts/update_2025_04_01_complete.ts';

console.log("Starting reingestion process for 2025-04-01...");
runFullUpdate().then(() => {
  console.log("Reingestion process completed successfully.");
}).catch(error => {
  console.error("Reingestion process failed:", error);
  process.exit(1);
});
