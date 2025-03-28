/**
 * Simple script to process missing periods for 2025-03-27
 */

import { exec } from 'child_process';
import fs from 'fs';

// Target date and periods
const targetDate = '2025-03-27';
const startPeriod = 35;
const endPeriod = 48;

// Function to execute a command and return a promise
function executeCommand(command) {
  return new Promise((resolve, reject) => {
    console.log(`Executing: ${command}`);
    
    const process = exec(command);
    
    // Forward stdout and stderr to the console
    process.stdout.on('data', (data) => {
      console.log(data.toString());
    });
    
    process.stderr.on('data', (data) => {
      console.error(data.toString());
    });
    
    process.on('close', (code) => {
      if (code === 0) {
        resolve();
      } else {
        reject(new Error(`Command failed with code ${code}`));
      }
    });
  });
}

// Main function
async function main() {
  try {
    console.log(`=== Processing missing periods (${startPeriod}-${endPeriod}) for ${targetDate} ===`);
    
    // Step 1: Clear any existing records for these periods
    await executeCommand(`npx tsx -e "
      import { db } from './db';
      import { and, between, eq } from 'drizzle-orm';
      import { curtailmentRecords } from './db/schema';
      
      async function run() {
        try {
          const result = await db.delete(curtailmentRecords)
            .where(
              and(
                eq(curtailmentRecords.settlementDate, '${targetDate}'),
                between(curtailmentRecords.settlementPeriod, ${startPeriod}, ${endPeriod})
              )
            );
          console.log('Records deleted successfully');
        } catch (error) {
          console.error('Error:', error);
        }
        process.exit(0);
      }
      
      run().catch(console.error);
    "`);
    
    // Step 2: Process each period individually
    for (let period = startPeriod; period <= endPeriod; period++) {
      console.log(`\n=== Processing period ${period} ===`);
      try {
        await executeCommand(`npx tsx -e "
          import { db } from './db';
          import { and, eq } from 'drizzle-orm';
          import { curtailmentRecords } from './db/schema';
          import axios from 'axios';
          import path from 'path';
          import fs from 'fs/promises';
          import { fileURLToPath } from 'url';
          
          const __filename = fileURLToPath(import.meta.url);
          const __dirname = path.dirname(__filename);
          
          async function run() {
            try {
              // Load BMU mapping
              const BMU_MAPPING_PATH = path.join(__dirname, 'server', 'data', 'bmuMapping.json');
              const data = await fs.readFile(BMU_MAPPING_PATH, 'utf8');
              const bmuMapping = JSON.parse(data);
              
              const windFarmIds = new Set();
              const bmuLeadPartyMap = new Map();
              
              for (const bmu of bmuMapping) {
                windFarmIds.add(bmu.id);
                bmuLeadPartyMap.set(bmu.id, bmu.leadPartyName);
              }
              
              console.log(\`Found \${windFarmIds.size} wind farm BMUs\`);
              
              // Fetch data from Elexon API
              const API_BASE_URL = 'https://data.elexon.co.uk/bmrs/api/v1';
              const response = await axios.get(\`\${API_BASE_URL}/balancing/bid-offer/accepted/settlement-period/${period}/settlement-date/${targetDate}\`);
              const apiData = response.data.data || [];
              
              // Filter to keep only valid wind farm records
              const validRecords = apiData.filter(record => {
                return windFarmIds.has(record.id) && record.volume < 0; // Negative volume indicates curtailment
              });
              
              console.log(\`Found \${validRecords.length} valid records for period ${period}\`);
              
              if (validRecords.length > 0) {
                // Prepare records for insertion
                const recordsToInsert = validRecords.map(record => {
                  const volume = Math.abs(record.volume);
                  const payment = volume * record.originalPrice;
                  
                  return {
                    settlementDate: '${targetDate}',
                    settlementPeriod: ${period},
                    farmId: record.id,
                    leadPartyName: bmuLeadPartyMap.get(record.id) || 'Unknown',
                    volume: record.volume.toString(), // Keep negative value
                    payment: payment.toString(),
                    originalPrice: record.originalPrice.toString(),
                    finalPrice: record.finalPrice.toString(),
                    soFlag: record.soFlag,
                    cadlFlag: record.cadlFlag
                  };
                });
                
                // Insert records
                await db.insert(curtailmentRecords).values(recordsToInsert);
                console.log(\`Inserted \${recordsToInsert.length} records for period ${period}\`);
                
                // Log individual records
                for (const record of validRecords) {
                  const volume = Math.abs(record.volume);
                  const payment = volume * record.originalPrice;
                  console.log(\`Added \${record.id}: \${volume.toFixed(2)} MWh, £\${payment.toFixed(2)}\`);
                }
              }
            } catch (error) {
              console.error('Error:', error);
            }
            process.exit(0);
          }
          
          run().catch(console.error);
        "`);
      } catch (error) {
        console.error(`Error processing period ${period}:`, error);
      }
      
      // Add a delay between API calls to avoid rate limiting
      console.log(`Waiting 2 seconds before next period...`);
      await new Promise(resolve => setTimeout(resolve, 2000));
    }
    
    // Step 3: Update Bitcoin calculations
    console.log('\n=== Updating Bitcoin calculations ===');
    await executeCommand(`npx tsx -e "
      import { spawn } from 'child_process';
      
      function runBitcoinCalculations() {
        return new Promise((resolve, reject) => {
          const process = spawn('npx', ['tsx', 'server/scripts/data/updateBitcoinCalculations.js', '${targetDate}', '${targetDate}']);
          
          process.stdout.on('data', (data) => {
            console.log(data.toString());
          });
          
          process.stderr.on('data', (data) => {
            console.error(data.toString());
          });
          
          process.on('close', (code) => {
            if (code === 0) {
              resolve();
            } else {
              console.log('Process exited with code ' + code);
              resolve(); // Resolve anyway to continue
            }
          });
        });
      }
      
      async function run() {
        try {
          await runBitcoinCalculations();
          console.log('Bitcoin calculations update completed');
        } catch (error) {
          console.error('Error:', error);
        }
        process.exit(0);
      }
      
      run().catch(console.error);
    "`);
    
    // Final step: Verify data
    console.log('\n=== Verification ===');
    await executeCommand(`npx tsx -e "
      import { db } from './db';
      import { sql } from 'drizzle-orm';
      
      async function run() {
        try {
          // Get count of records by period
          const result = await db.execute(sql\`
            SELECT 
              settlement_period, 
              COUNT(*) as record_count
            FROM 
              curtailment_records
            WHERE 
              settlement_date = '${targetDate}'
            GROUP BY 
              settlement_period
            ORDER BY 
              settlement_period
          \`);
          
          console.log('Periods with data:');
          const periods = result.rows.map(row => parseInt(row.settlement_period.toString()));
          
          // Check for missing periods
          const allPeriods = Array.from({ length: 48 }, (_, i) => i + 1);
          const missingPeriods = allPeriods.filter(p => !periods.includes(p));
          
          if (missingPeriods.length > 0) {
            console.log('⚠️ Missing periods:', missingPeriods.join(', '));
          } else {
            console.log('✅ All 48 periods have data');
          }
          
          // Get total statistics
          const totalsResult = await db.execute(sql\`
            SELECT 
              COUNT(*) as record_count,
              COUNT(DISTINCT settlement_period) as period_count,
              SUM(volume) as total_volume,
              SUM(payment) as total_payment
            FROM 
              curtailment_records
            WHERE 
              settlement_date = '${targetDate}'
          \`);
          
          const stats = totalsResult.rows[0];
          console.log(\`
            Record count: \${stats.record_count}
            Period count: \${stats.period_count}/48
            Total volume: \${parseFloat(stats.total_volume).toFixed(2)} MWh
            Total payment: £\${parseFloat(stats.total_payment).toFixed(2)}
          \`);
        } catch (error) {
          console.error('Error:', error);
        }
        process.exit(0);
      }
      
      run().catch(console.error);
    "`);
    
    console.log('=== Process completed successfully ===');
    
  } catch (error) {
    console.error('Process failed:', error);
    process.exit(1);
  }
}

// Run the script
main().catch(console.error);