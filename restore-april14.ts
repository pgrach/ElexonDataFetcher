/**
 * Direct Restoration Script for April 14, 2025
 * 
 * This script directly restores the daily summary for April 14, 2025
 * without going through the full reprocessing workflow.
 * 
 * Run with: npx tsx restore-april14.ts
 */

import { db } from './db';
import { dailySummaries, bitcoinDailySummaries } from './db/schema';

// Constants
const TARGET_DATE = '2025-04-14';
const TOTAL_VOLUME = 18584.63;
const TOTAL_PAYMENT = 410620.51;
const BITCOIN_VALUES = {
  'S19J_PRO': 0.004345806744578676,
  'M20S': 0.0025945500981026277,
  'S9': 0.001308176520051745
};

async function restoreData() {
  console.log(`Restoring April 14 data...`);
  
  try {
    // Restore daily summary
    console.log(`Restoring daily summary for ${TARGET_DATE}...`);
    
    await db.insert(dailySummaries).values({
      summaryDate: TARGET_DATE,
      totalCurtailedEnergy: TOTAL_VOLUME.toString(),
      totalPayment: (-TOTAL_PAYMENT).toString(), // Payment is stored as negative in daily summaries
      lastUpdated: new Date()
    }).onConflictDoUpdate({
      target: [dailySummaries.summaryDate],
      set: {
        totalCurtailedEnergy: TOTAL_VOLUME.toString(),
        totalPayment: (-TOTAL_PAYMENT).toString(),
        lastUpdated: new Date()
      }
    });
    
    console.log(`Restored daily summary: ${TOTAL_VOLUME.toFixed(2)} MWh, £${TOTAL_PAYMENT.toFixed(2)}`);
    
    // Restore Bitcoin daily summaries
    console.log(`Restoring Bitcoin daily summaries for ${TARGET_DATE}...`);
    
    for (const [minerModel, bitcoinValue] of Object.entries(BITCOIN_VALUES)) {
      await db.insert(bitcoinDailySummaries).values({
        summaryDate: TARGET_DATE,
        minerModel: minerModel,
        bitcoinMined: bitcoinValue.toString(),
        createdAt: new Date(),
        updatedAt: new Date()
      }).onConflictDoUpdate({
        target: [bitcoinDailySummaries.summaryDate, bitcoinDailySummaries.minerModel],
        set: {
          bitcoinMined: bitcoinValue.toString(),
          updatedAt: new Date()
        }
      });
      
      console.log(`Restored Bitcoin daily summary for ${minerModel}: ${bitcoinValue} BTC`);
    }
    
    console.log('Data restoration completed successfully');
    
  } catch (error: any) {
    console.error(`Error during data restoration: ${error.message}`);
    process.exit(1);
  }
}

// Run the script
restoreData().then(() => {
  console.log('Script execution completed');
}).catch(error => {
  console.error(`Script execution error: ${error}`);
  process.exit(1);
});