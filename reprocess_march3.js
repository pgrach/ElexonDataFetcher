import { db } from "./db/index.js";
import { curtailmentRecords } from "./db/schema.js";
import { processDailyCurtailment } from "./server/services/curtailment.js";
import { eq } from "drizzle-orm";

const TARGET_DATE = '2025-03-03';

// Simple script to check if March 3 data exists and 
// reprocess it if needed
async function reprocess() {
  try {
    console.log(`\n=== Checking data for ${TARGET_DATE} ===\n`);

    // Check existing records
    const existingCount = await db
      .select({ count: db.fn.count() })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log(`Found ${existingCount[0].count} existing records for ${TARGET_DATE}`);
    
    console.log(`\n=== Reprocessing data for ${TARGET_DATE} ===\n`);
    console.log('This will delete existing records and fetch fresh data from Elexon API');
    
    // Reprocess the data
    await processDailyCurtailment(TARGET_DATE);
    
    // Verify the update
    const newCount = await db
      .select({ count: db.fn.count() })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log(`\n=== Update Complete ===`);
    console.log(`Now have ${newCount[0].count} records for ${TARGET_DATE}`);
    
    const recordDifference = Number(newCount[0].count) - Number(existingCount[0].count);
    if (recordDifference > 0) {
      console.log(`Added ${recordDifference} new records`);
    } else if (recordDifference < 0) {
      console.log(`Removed ${Math.abs(recordDifference)} invalid records`);
    } else {
      console.log(`No change in record count`);
    }
    
    console.log('\nReprocessing complete!');
    process.exit(0);
  } catch (error) {
    console.error('Error during reprocessing:', error);
    process.exit(1);
  }
}

reprocess();