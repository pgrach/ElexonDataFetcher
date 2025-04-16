/**
 * Update Lead Party Names for 2025-04-14
 * 
 * This script updates the lead party names for curtailment records on April 14, 2025.
 * It uses a lookup table from April 10, 2025 records to determine the correct 
 * lead party names for each farm ID.
 * 
 * Run with: npx tsx update-lead-party-names.ts
 */

import { db } from './db';
import { curtailmentRecords } from './db/schema';
import { eq, and, sql } from 'drizzle-orm';
import { format } from 'date-fns';

// The date we want to update
const TARGET_DATE = '2025-04-14';
// The reference date with correct lead party names
const REFERENCE_DATE = '2025-04-10';

async function updateLeadPartyNames() {
  console.log(`\n=== Updating Lead Party Names for ${TARGET_DATE} ===`);
  console.log(`Start Time: ${format(new Date(), "yyyy-MM-dd HH:mm:ss")}`);
  
  try {
    // Step 1: Get existing farm IDs with Unknown lead party names for April 14
    const unknownFarms = await db
      .select({
        farmId: curtailmentRecords.farmId,
        leadPartyName: curtailmentRecords.leadPartyName
      })
      .from(curtailmentRecords)
      .where(and(
        eq(curtailmentRecords.settlementDate, TARGET_DATE),
        eq(curtailmentRecords.leadPartyName, 'Unknown')
      ))
      .groupBy(curtailmentRecords.farmId, curtailmentRecords.leadPartyName);
    
    console.log(`Found ${unknownFarms.length} farms with 'Unknown' lead party name for ${TARGET_DATE}`);
    
    // Step 2: Get lead party names from April 10 (a date with correct data)
    const referenceLeadParties = await db
      .select({
        farmId: curtailmentRecords.farmId,
        leadPartyName: curtailmentRecords.leadPartyName
      })
      .from(curtailmentRecords)
      .where(and(
        eq(curtailmentRecords.settlementDate, REFERENCE_DATE),
        sql`${curtailmentRecords.leadPartyName} IS NOT NULL AND ${curtailmentRecords.leadPartyName} != 'Unknown'`
      ))
      .groupBy(curtailmentRecords.farmId, curtailmentRecords.leadPartyName);
    
    // Create a lookup Map: farmId -> leadPartyName
    const leadPartyLookup = new Map();
    for (const record of referenceLeadParties) {
      leadPartyLookup.set(record.farmId, record.leadPartyName);
    }
    
    // A record of which farms we updated
    const updatedFarms = new Map<string, string>();
    let totalUpdates = 0;
    
    // Step 3: Update each farm's records
    for (const farm of unknownFarms) {
      const correctLeadParty = leadPartyLookup.get(farm.farmId);
      
      if (!correctLeadParty) {
        console.log(`No reference lead party found for ${farm.farmId}, skipping...`);
        continue;
      }
      
      // Update all records for this farm on the target date
      const updateResult = await db.update(curtailmentRecords)
        .set({ leadPartyName: correctLeadParty })
        .where(and(
          eq(curtailmentRecords.settlementDate, TARGET_DATE),
          eq(curtailmentRecords.farmId, farm.farmId)
        ));
      
      updatedFarms.set(farm.farmId, correctLeadParty);
      console.log(`Updated lead party for ${farm.farmId} to "${correctLeadParty}"`);
      totalUpdates++;
    }
    
    // Step 4: Verify updates
    console.log(`\nUpdated lead party names for ${totalUpdates} farms:`);
    for (const [farmId, leadPartyName] of updatedFarms.entries()) {
      console.log(`  ${farmId}: ${leadPartyName}`);
    }
    
    // Step 5: Count how many 'Unknown' lead party names remain
    const remainingUnknown = await db
      .select({ 
        count: sql`COUNT(*)` 
      })
      .from(curtailmentRecords)
      .where(and(
        eq(curtailmentRecords.settlementDate, TARGET_DATE),
        eq(curtailmentRecords.leadPartyName, 'Unknown')
      ));
    
    console.log(`\nRemaining records with 'Unknown' lead party name: ${remainingUnknown[0]?.count || 0}`);
    
    console.log(`\n=== Update Complete ===`);
    console.log(`End Time: ${format(new Date(), "yyyy-MM-dd HH:mm:ss")}`);
    
  } catch (error) {
    console.error("Error updating lead party names:", error);
  }
}

// Run the update script
updateLeadPartyNames().catch(error => {
  console.error("Script execution error:", error);
  process.exit(1);
});