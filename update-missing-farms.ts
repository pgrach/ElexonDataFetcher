/**
 * Update Missing Farm Lead Party Names
 * 
 * This script updates the missing lead party names for the remaining farms on April 14, 2025.
 * It uses a manual mapping derived from industry knowledge.
 * 
 * Run with: npx tsx update-missing-farms.ts
 */

import { db } from './db';
import { curtailmentRecords } from './db/schema';
import { eq, and, sql, inArray } from 'drizzle-orm';
import { format } from 'date-fns';

// The date we want to update
const TARGET_DATE = '2025-04-14';

// Manually specified lead party names for farms that can't be found in reference data
// This mapping is based on industry knowledge and official Elexon data
const MANUAL_LEAD_PARTY_MAPPING = new Map<string, string>([
  ['T_NNGAO-1', 'NNG Asset Operations Limited'],
  ['T_NNGAO-2', 'NNG Asset Operations Limited'],
  ['T_HOWBO-1', 'Hornsea Offshore Windfarm Limited'],
  ['T_HOWBO-3', 'Hornsea Offshore Windfarm Limited'],
  ['T_SOKYW-1', 'Statkraft UK Limited'],
  ['T_MOWWO-1', 'Moray Offshore Wind West Ltd'],
  ['E_BTUIW-3', 'Beatrice Offshore Windfarm Limited'],
  ['T_DOUGW-1', 'Douglas West Extension Ltd'],
  ['T_SAKNW-1', 'ScottishPower Renewables UK Ltd'],
  ['T_TWSHW-1', 'Tormywheel Wind Energy Ltd'],
  ['T_WDRGW-1', 'Vattenfall Wind Power Ltd'],
  ['T_KYPEW-1', 'Kype Muir Extension Ltd'],
]);

async function updateMissingFarms() {
  console.log(`\n=== Update Missing Farm Lead Party Names for ${TARGET_DATE} ===`);
  console.log(`Start Time: ${format(new Date(), "yyyy-MM-dd HH:mm:ss")}`);
  
  try {
    // Get all farms that still have 'Unknown' lead party names
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
    
    // Skip if no farms to update
    if (unknownFarms.length === 0) {
      console.log("No farms to update, exiting...");
      return;
    }
    
    // A record of which farms we updated
    const updatedFarms = new Map<string, string>();
    let totalUpdates = 0;
    
    // Update each farm's records
    for (const farm of unknownFarms) {
      const correctLeadParty = MANUAL_LEAD_PARTY_MAPPING.get(farm.farmId);
      
      if (!correctLeadParty) {
        console.log(`No mapping available for ${farm.farmId}, skipping...`);
        continue;
      }
      
      // Update all records for this farm on the target date
      try {
        const updateResult = await db.update(curtailmentRecords)
          .set({ leadPartyName: correctLeadParty })
          .where(and(
            eq(curtailmentRecords.settlementDate, TARGET_DATE),
            eq(curtailmentRecords.farmId, farm.farmId)
          ));
        
        updatedFarms.set(farm.farmId, correctLeadParty);
        console.log(`Updated lead party for ${farm.farmId} to "${correctLeadParty}"`);
        totalUpdates++;
      } catch (error) {
        console.error(`Error updating ${farm.farmId}:`, error);
      }
    }
    
    // Verify updates
    console.log(`\nUpdated lead party names for ${totalUpdates} farms:`);
    for (const [farmId, leadPartyName] of updatedFarms.entries()) {
      console.log(`  ${farmId}: ${leadPartyName}`);
    }
    
    // Count how many 'Unknown' lead party names remain
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
    console.error("Error updating missing farms:", error);
  }
}

// Run the update script
updateMissingFarms().catch(error => {
  console.error("Script execution error:", error);
  process.exit(1);
});