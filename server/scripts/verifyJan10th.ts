import { db } from "@db";
import { curtailmentRecords } from "@db/schema";
import { eq, sql } from "drizzle-orm";
import { fetchBidsOffers } from "../services/elexon";

async function verifyJan10thData() {
  try {
    console.log('Starting detailed verification of January 10th, 2025 data...');
    
    // Sample a few specific periods for detailed comparison
    const periodsToCheck = [1, 24, 48]; // Beginning, middle, and end of day
    
    for (const period of periodsToCheck) {
      console.log(`\nAnalyzing Period ${period}:`);
      
      // Get current DB records
      const dbRecords = await db
        .select({
          farmId: curtailmentRecords.farmId,
          volume: curtailmentRecords.volume,
          payment: curtailmentRecords.payment,
          originalPrice: curtailmentRecords.originalPrice,
          soFlag: curtailmentRecords.soFlag
        })
        .from(curtailmentRecords)
        .where(sql`${curtailmentRecords.settlementDate} = '2025-01-10' AND ${curtailmentRecords.settlementPeriod} = ${period}`);
      
      console.log(`DB Records for Period ${period}:`, dbRecords.length);
      
      // Fetch fresh API data
      console.log(`\nFetching fresh API data for period ${period}...`);
      const apiRecords = await fetchBidsOffers('2025-01-10', period);
      
      // Filter API records using same logic as reference implementation
      const validApiRecords = apiRecords.filter(record => 
        record.volume < 0 && // Only negative volumes (curtailment)
        record.soFlag &&     // System operator flagged
        (record.id.startsWith('T_') || record.id.startsWith('E_')) // Wind farm BMUs
      );
      
      console.log(`Valid API Records for Period ${period}:`, validApiRecords.length);
      
      // Compare totals
      const dbVolume = dbRecords.reduce((sum, r) => sum + Number(r.volume), 0);
      const apiVolume = validApiRecords.reduce((sum, r) => sum + Math.abs(r.volume), 0);
      
      console.log('\nComparison:');
      console.log(`DB Volume: ${dbVolume}`);
      console.log(`API Volume: ${apiVolume}`);
      
      // Log details of any mismatches
      if (Math.abs(dbVolume - apiVolume) > 0.01) {
        console.log('\nDetailed record comparison:');
        const dbFarms = new Set(dbRecords.map(r => r.farmId));
        const apiFarms = new Set(validApiRecords.map(r => r.id));
        
        console.log('Farms in DB but not in API:', 
          [...dbFarms].filter(f => !apiFarms.has(f)));
        console.log('Farms in API but not in DB:', 
          [...apiFarms].filter(f => !dbFarms.has(f)));
          
        // Detailed volume comparison for each farm
        validApiRecords.forEach(apiRecord => {
          const dbRecord = dbRecords.find(r => r.farmId === apiRecord.id);
          if (dbRecord) {
            const apiVolume = Math.abs(apiRecord.volume);
            const dbVolume = Number(dbRecord.volume);
            if (Math.abs(apiVolume - dbVolume) > 0.01) {
              console.log(`\nMismatch for farm ${apiRecord.id}:`);
              console.log(`API Volume: ${apiVolume}`);
              console.log(`DB Volume: ${dbVolume}`);
            }
          }
        });
      }
    }
  } catch (error) {
    console.error('Verification failed:', error);
    process.exit(1);
  }
}

// Run the verification
verifyJan10thData();
