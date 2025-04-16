/**
 * Advanced Lead Party Name Update Script
 * 
 * This script updates the lead party names for curtailment records on April 14, 2025.
 * It uses multiple reference dates to maximize coverage of correct lead party names.
 * 
 * Run with: npx tsx update-lead-party-names-advanced.ts
 */

import { db } from './db';
import { curtailmentRecords } from './db/schema';
import { eq, and, sql, inArray, not } from 'drizzle-orm';
import { format, subDays, parse } from 'date-fns';

// The date we want to update
const TARGET_DATE = '2025-04-14';

// Multiple reference dates to maximize coverage
const REFERENCE_DATES = [
  '2025-04-10', // Primary reference date
  '2025-04-09',
  '2025-04-08',
  '2025-04-07',
  '2025-04-06',
  '2025-04-05',
  '2025-04-04',
  '2025-04-03',
  '2025-04-02',
  '2025-04-01',
  '2025-03-31',
  '2025-03-28',
  '2025-03-25',
  '2025-03-20',
  '2025-03-15',
  '2025-03-10',
  '2025-03-05',
  '2025-03-01',
];

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

async function updateLeadPartyNames() {
  console.log(`\n=== Advanced Lead Party Name Update for ${TARGET_DATE} ===`);
  console.log(`Start Time: ${format(new Date(), "yyyy-MM-dd HH:mm:ss")}`);
  
  try {
    // Step 1: Get existing farm IDs with Unknown lead party names for the target date
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
    
    // A list of farm IDs that need to be updated
    const farmIdsToUpdate = unknownFarms.map(farm => farm.farmId);
    
    // Create a master lookup Map: farmId -> leadPartyName
    const leadPartyLookup = new Map<string, string>();
    
    // Step 2: Look through each reference date to build a comprehensive mapping
    console.log(`Looking up lead party names from ${REFERENCE_DATES.length} reference dates...`);
    
    let referenceDatesChecked = 0;
    let farmsMappedTotal = 0;
    
    for (const refDate of REFERENCE_DATES) {
      // Skip if we've already found mappings for all farms
      if (leadPartyLookup.size >= farmIdsToUpdate.length) {
        console.log(`All ${farmIdsToUpdate.length} farms mapped after checking ${referenceDatesChecked} reference dates.`);
        break;
      }
      
      // Get lead party names from the reference date
      const referenceLeadParties = await db
        .select({
          farmId: curtailmentRecords.farmId,
          leadPartyName: curtailmentRecords.leadPartyName
        })
        .from(curtailmentRecords)
        .where(and(
          eq(curtailmentRecords.settlementDate, refDate),
          inArray(curtailmentRecords.farmId, farmIdsToUpdate),
          not(eq(curtailmentRecords.leadPartyName, 'Unknown')),
          sql`${curtailmentRecords.leadPartyName} IS NOT NULL`
        ))
        .groupBy(curtailmentRecords.farmId, curtailmentRecords.leadPartyName);
      
      // Add any new mappings (don't overwrite existing ones)
      let farmsMappedThisDate = 0;
      for (const record of referenceLeadParties) {
        if (!leadPartyLookup.has(record.farmId)) {
          leadPartyLookup.set(record.farmId, record.leadPartyName);
          farmsMappedThisDate++;
        }
      }
      
      farmsMappedTotal += farmsMappedThisDate;
      referenceDatesChecked++;
      
      console.log(`[${refDate}] Found ${farmsMappedThisDate} new farm mappings. Total mapped: ${leadPartyLookup.size} of ${farmIdsToUpdate.length}`);
    }
    
    // Step 3: Add manual mappings for any remaining farms
    let manualMappingsAdded = 0;
    
    for (const farmId of farmIdsToUpdate) {
      if (!leadPartyLookup.has(farmId) && MANUAL_LEAD_PARTY_MAPPING.has(farmId)) {
        leadPartyLookup.set(farmId, MANUAL_LEAD_PARTY_MAPPING.get(farmId)!);
        manualMappingsAdded++;
      }
    }
    
    console.log(`Added ${manualMappingsAdded} manual lead party mappings.`);
    
    // A record of which farms we updated
    const updatedFarms = new Map<string, string>();
    let totalUpdates = 0;
    
    // Step 4: Update each farm's records
    console.log(`\nUpdating lead party names for ${leadPartyLookup.size} farms...`);
    
    for (const farm of unknownFarms) {
      const correctLeadParty = leadPartyLookup.get(farm.farmId);
      
      if (!correctLeadParty) {
        console.log(`No reference lead party found for ${farm.farmId}, skipping...`);
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
    
    // Step 5: Verify updates
    console.log(`\nUpdated lead party names for ${totalUpdates} farms:`);
    for (const [farmId, leadPartyName] of updatedFarms.entries()) {
      console.log(`  ${farmId}: ${leadPartyName}`);
    }
    
    // Step 6: Count how many 'Unknown' lead party names remain
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