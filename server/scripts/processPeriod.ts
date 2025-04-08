/**
 * Process a single period of curtailment data
 * 
 * This script provides a command-line interface to process a single period
 * of curtailment data for a specific date.
 * 
 * Usage:
 *   npx tsx server/scripts/processPeriod.ts <date> <period>
 * 
 * Example:
 *   npx tsx server/scripts/processPeriod.ts 2025-03-27 17
 */

import { db } from "@db";
import { curtailmentRecords } from "@db/schema";
import { fetchBidsOffers } from "../services/elexon";
import { eq } from "drizzle-orm";
import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const BMU_MAPPING_PATH = path.join(__dirname, "../../data/bmu_mapping.json");

// Execute a command and return the result
async function executeCommand(command: string): Promise<string> {
  return new Promise((resolve, reject) => {
    // Use the Node.js child_process module with dynamic import for ES modules
    import('child_process').then(({ exec }) => {
      exec(command, (error: Error | null, stdout: string, stderr: string) => {
        if (error) {
          console.error(`Error executing command: ${error.message}`);
          return reject(error);
        }
        
        if (stderr) {
          console.error(`Command stderr: ${stderr}`);
        }
        
        resolve(stdout);
      });
    }).catch(error => {
      console.error('Failed to import child_process:', error);
      reject(error);
    });
  });
}

// Process a single period of data
async function processPeriod(date: string, period: number): Promise<{ records: number, volume: number, payment: number }> {
  console.log(`Processing period ${period} for date ${date}`);
  
  try {
    // Load BMU mapping
    const mappingContent = await fs.readFile(BMU_MAPPING_PATH, 'utf8');
    const bmuMapping = JSON.parse(mappingContent);
    
    // Create a set of valid wind farm BMU IDs for faster lookups
    const validWindFarmIds = new Set(
      bmuMapping
        .filter((bmu: any) => bmu.fuelType === "WIND")
        .map((bmu: any) => bmu.elexonBmUnit)
    );
    
    // Create a map of BMU IDs to lead party names
    const bmuLeadPartyMap = new Map(
      bmuMapping
        .filter((bmu: any) => bmu.fuelType === "WIND")
        .map((bmu: any) => [bmu.elexonBmUnit, bmu.leadPartyName || 'Unknown'])
    );
    
    console.log(`Found ${validWindFarmIds.size} valid wind farm BMUs`);
    
    // Fetch data from Elexon API
    const records = await fetchBidsOffers(date, period);
    const validRecords = records.filter(record => 
      record.volume < 0 && 
      (record.soFlag || record.cadlFlag) && 
      validWindFarmIds.has(record.id)
    );
    
    console.log(`Found ${validRecords.length} valid curtailment records for period ${period}`);
    
    // Delete any existing records for this date and period
    // Using direct SQL to avoid parameterized query issues
    const deleteQuery = `
      DELETE FROM curtailment_records 
      WHERE settlement_date = '${date}' AND settlement_period = ${period}
    `;
    await db.execute(deleteQuery);
    
    // Insert the records into the database
    let totalVolume = 0;
    let totalPayment = 0;
    
    for (const record of validRecords) {
      const volume = Math.abs(record.volume);
      const payment = volume * record.originalPrice;
      
      // Using direct SQL to avoid parameterized query issues
      const insertQuery = `
        INSERT INTO curtailment_records 
        (settlement_date, settlement_period, farm_id, lead_party_name, 
         volume, payment, original_price, final_price, so_flag, cadl_flag)
        VALUES (
          '${date}', 
          ${period}, 
          '${record.id}', 
          '${(bmuLeadPartyMap.get(record.id) || 'Unknown').replace(/'/g, "''")}',
          '${record.volume.toString()}', 
          '${payment.toString()}',
          '${record.originalPrice.toString()}',
          '${record.finalPrice.toString()}',
          ${record.soFlag},
          ${record.cadlFlag}
        )
      `;
      await db.execute(insertQuery);
      
      totalVolume += volume;
      totalPayment += payment;
      
      console.log(`Added record for ${record.id}: ${volume.toFixed(2)} MWh, £${payment.toFixed(2)}`);
    }
    
    console.log(`Processed ${validRecords.length} records`);
    console.log(`Total volume: ${totalVolume.toFixed(2)} MWh`);
    console.log(`Total payment: £${totalPayment.toFixed(2)}`);
    
    return { 
      records: validRecords.length, 
      volume: totalVolume, 
      payment: totalPayment 
    };
  } catch (error) {
    console.error(`Error processing period ${period} for date ${date}:`, error);
    return { records: 0, volume: 0, payment: 0 };
  }
}

// Handle command line execution
async function main() {
  const args = process.argv.slice(2);
  
  if (args.length < 2) {
    console.error('Usage: npx tsx server/scripts/processPeriod.ts <date> <period>');
    console.error('Example: npx tsx server/scripts/processPeriod.ts 2025-03-27 17');
    process.exit(1);
  }
  
  const date = args[0];
  const period = parseInt(args[1], 10);
  
  if (isNaN(period) || period < 1 || period > 48) {
    console.error('Period must be a number between 1 and 48');
    process.exit(1);
  }
  
  try {
    await processPeriod(date, period);
    console.log(`Successfully processed period ${period} for ${date}`);
  } catch (error) {
    console.error(`Failed to process period ${period} for ${date}:`, error);
    process.exit(1);
  }
}

// In ES modules, there's no require.main === module
// We'll use import.meta.url to detect direct execution
const isMainModule = import.meta.url.endsWith('processPeriod.ts');

// Only run main when script is executed directly
if (isMainModule) {
  main().catch(console.error);
}

export { processPeriod };