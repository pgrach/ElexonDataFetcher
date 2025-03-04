/**
 * Script to compare Elexon API data with database data for a specific date
 */
import { fetchBidsOffers } from './server/services/elexon';
import { delay } from './server/services/elexon';
import { db } from './db';
import { curtailmentRecords } from './db/schema';
import { eq, and } from 'drizzle-orm';

async function fetchElexonDataForDate(date: string): Promise<any> {
  console.log(`Fetching Elexon data for ${date}`);
  
  // Using period 16 (missing period) as a test case
  const missingPeriods = [16];
  const results: any = {};
  
  for (const period of missingPeriods) {
    console.log(`Fetching period ${period}`);
    try {
      const elexonData = await fetchBidsOffers(date, period);
      console.log(`Period ${period}: Found ${elexonData.length} records from Elexon API`);
      
      // Get database data for comparison
      const dbData = await db.select()
        .from(curtailmentRecords)
        .where(
          and(
            eq(curtailmentRecords.settlementDate, date),
            eq(curtailmentRecords.settlementPeriod, period)
          )
        );
      
      console.log(`Period ${period}: Found ${dbData.length} records in database`);
      
      // Compare the data
      const elexonBMUs = new Set(elexonData.map(item => item.bmUnit));
      const dbBMUs = new Set(dbData.map(item => item.farmId));
      
      const missingFromDB = [...elexonBMUs].filter(bmu => !dbBMUs.has(bmu));
      const extraInDB = [...dbBMUs].filter(bmu => !elexonBMUs.has(bmu));
      
      console.log(`BMUs missing from database: ${missingFromDB.length}`);
      if (missingFromDB.length > 0) {
        console.log(`Missing BMUs: ${missingFromDB.join(', ')}`);
        
        // Show the elexon data for missing BMUs
        const missingRecords = elexonData.filter(item => missingFromDB.includes(item.bmUnit));
        console.log('Missing records details:');
        console.log(JSON.stringify(missingRecords, null, 2));
      }
      
      console.log(`Extra BMUs in database: ${extraInDB.length}`);
      if (extraInDB.length > 0) {
        console.log(`Extra BMUs: ${extraInDB.join(', ')}`);
      }
      
      results[period] = {
        elexonCount: elexonData.length,
        dbCount: dbData.length,
        missingFromDB,
        extraInDB
      };
    } catch (error) {
      console.error(`Error fetching period ${period}:`, error);
    }
    
    // Add a delay to avoid rate limiting
    await delay(1000);
  }
  
  return results;
}

async function main() {
  try {
    const date = '2025-03-02';
    const results = await fetchElexonDataForDate(date);
    console.log('\nSummary:');
    console.log(JSON.stringify(results, null, 2));
  } catch (error) {
    console.error('Error:', error);
  }
}

main();