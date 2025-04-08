/**
 * Test script to fetch data from Elexon API for a specific date and period
 * 
 * This script fetches data from Elexon API for a specific date and period
 * and compares it with the database records.
 */

import { fetchBidsOffers } from './server/services/elexon';

async function main() {
  try {
    // Get the date and period from command-line arguments or use default
    const date = process.argv[2] || '2025-03-31';
    const period = parseInt(process.argv[3] || '1');
    
    console.log(`\n=== Fetching Elexon API data for ${date} Period ${period} ===\n`);
    
    // Fetch data from Elexon API
    const records = await fetchBidsOffers(date, period);
    
    console.log(`\n=== Elexon API Results for ${date} Period ${period} ===\n`);
    console.log(`Total Records: ${records.length}`);
    
    if (records.length > 0) {
      const totalVolume = records.reduce((sum, r) => sum + Math.abs(r.volume), 0);
      console.log(`Total Volume: ${totalVolume.toFixed(2)} MWh`);
      
      // Show detailed records
      console.log("\nDetailed Records:");
      records.forEach((record, index) => {
        console.log(`Record #${index + 1}:`);
        console.log(`  ID: ${record.id}`);
        console.log(`  Volume: ${record.volume} MWh`);
        console.log(`  Original Price: ${record.originalPrice}`);
        console.log(`  Final Price: ${record.finalPrice}`);
        console.log(`  SO Flag: ${record.soFlag}`);
        console.log(`  CADL Flag: ${record.cadlFlag}`);
        console.log(`  Lead Party: ${record.leadPartyName || 'N/A'}`);
        console.log();
      });
    } else {
      console.log("No records found");
    }
  } catch (error) {
    console.error('Error fetching Elexon API data:', error);
    process.exit(1);
  }
}

main();