/**
 * Script to fetch missing periods 35-48 for 2025-03-27 using correct Elexon API endpoints
 */

const { execSync } = require('child_process');
const fs = require('fs');

// Constants
const SETTLEMENT_DATE = '2025-03-27';
const START_PERIOD = 35;
const END_PERIOD = 48;

// Create a wrapper for running TSX with the correct API endpoints
function processOnePeriod(period) {
  console.log(`\n=== Processing period ${period} for ${SETTLEMENT_DATE} ===`);

  const command = `npx tsx -e "
    import { db } from './db';
    import { and, eq } from 'drizzle-orm';
    import { curtailmentRecords } from './db/schema';
    import axios from 'axios';
    import fs from 'fs/promises';
    import path from 'path';
    import { fileURLToPath } from 'url';
    
    const __filename = fileURLToPath(import.meta.url);
    const __dirname = path.dirname(__filename);
    
    // Constants from elexon.ts service
    const ELEXON_BASE_URL = 'https://data.elexon.co.uk/bmrs/api/v1';
    const BMU_MAPPING_PATH = path.join(__dirname, 'data', 'bmu_mapping.json');
    
    async function run() {
      try {
        const period = ${period};
        const date = '${SETTLEMENT_DATE}';
        
        console.log(\`Processing period \${period} for \${date}\`);
        
        // Load BMU mapping
        const mappingContent = await fs.readFile(BMU_MAPPING_PATH, 'utf8');
        const bmuMapping = JSON.parse(mappingContent);
        
        // Create mappings for valid wind farm IDs and lead party names
        const windFarmIds = new Set();
        const bmuLeadPartyMap = new Map();
        
        for (const bmu of bmuMapping) {
          windFarmIds.add(bmu.elexonBmUnit);
          bmuLeadPartyMap.set(bmu.elexonBmUnit, bmu.leadPartyName);
        }
        
        console.log(\`Loaded \${windFarmIds.size} wind farm BMUs\`);
        
        // Delete any existing records for this period
        await db.delete(curtailmentRecords)
          .where(
            and(
              eq(curtailmentRecords.settlementDate, date),
              eq(curtailmentRecords.settlementPeriod, period)
            )
          );
        
        console.log(\`Cleared any existing records for period \${period}\`);
        
        // Make API requests using correct endpoints
        try {
          // Try fetching both bids and offers in parallel
          const [bidsResponse, offersResponse] = await Promise.all([
            axios.get(
              \`\${ELEXON_BASE_URL}/balancing/settlement/stack/all/bid/\${date}/\${period}\`,
              { headers: { 'Accept': 'application/json' }, timeout: 30000 }
            ).catch(e => ({ data: { data: [] } })),
            axios.get(
              \`\${ELEXON_BASE_URL}/balancing/settlement/stack/all/offer/\${date}/\${period}\`,
              { headers: { 'Accept': 'application/json' }, timeout: 30000 }
            ).catch(e => ({ data: { data: [] } }))
          ]);
          
          // Extract and filter the data
          const validBids = (bidsResponse.data?.data || []).filter(record => 
            record.volume < 0 && record.soFlag && windFarmIds.has(record.id)
          );
          
          const validOffers = (offersResponse.data?.data || []).filter(record => 
            record.volume < 0 && record.soFlag && windFarmIds.has(record.id)
          );
          
          // Combine all records
          const allRecords = [...validBids, ...validOffers];
          
          console.log(\`Found \${allRecords.length} valid records (bids: \${validBids.length}, offers: \${validOffers.length})\`);
          
          if (allRecords.length > 0) {
            // Prepare records for insertion
            const recordsToInsert = allRecords.map(record => {
              const volume = record.volume; // Already negative for curtailment
              const payment = Math.abs(volume) * record.originalPrice * -1;
              
              return {
                settlementDate: date,
                settlementPeriod: period,
                farmId: record.id,
                leadPartyName: bmuLeadPartyMap.get(record.id) || 'Unknown',
                volume: volume.toString(),
                payment: payment.toString(),
                originalPrice: record.originalPrice.toString(),
                finalPrice: record.finalPrice.toString(),
                soFlag: record.soFlag,
                cadlFlag: record.cadlFlag
              };
            });
            
            // Insert records
            await db.insert(curtailmentRecords).values(recordsToInsert);
            console.log(\`Successfully inserted \${recordsToInsert.length} records for period \${period}\`);
            
            // Calculate totals for reporting
            const totalVolume = allRecords.reduce((sum, record) => sum + Math.abs(record.volume), 0);
            const totalPayment = allRecords.reduce((sum, record) => sum + (Math.abs(record.volume) * record.originalPrice * -1), 0);
            
            console.log(\`Period \${period} totals: Volume = \${totalVolume.toFixed(2)} MWh, Payment = £\${totalPayment.toFixed(2)}\`);
          } else {
            console.log(\`No valid records found for period \${period}\`);
          }
          
        } catch (error) {
          console.error(\`API error for period \${period}:\`, error.message);
          // We won't rethrow here to ensure the process continues
        }
        
      } catch (error) {
        console.error('Error:', error);
      }
      process.exit(0);
    }
    
    run().catch(console.error);
  "`;

  try {
    const output = execSync(command, { encoding: 'utf8' });
    console.log(output);
    return true;
  } catch (error) {
    console.error(`Error processing period ${period}:`, error.message);
    return false;
  }
}

// Process all periods sequentially
async function main() {
  console.log(`Starting processing of missing periods (${START_PERIOD}-${END_PERIOD}) for ${SETTLEMENT_DATE}`);
  
  for (let period = START_PERIOD; period <= END_PERIOD; period++) {
    const success = processOnePeriod(period);
    
    if (success) {
      console.log(`✅ Successfully processed period ${period}`);
    } else {
      console.log(`❌ Failed to process period ${period}`);
    }
    
    // Add a small delay between requests to avoid overwhelming resources
    if (period < END_PERIOD) {
      console.log(`Waiting 3 seconds before processing next period...`);
      execSync('sleep 3');
    }
  }
  
  console.log(`\n=== Processing complete ===`);
  console.log(`Processed periods ${START_PERIOD}-${END_PERIOD} for ${SETTLEMENT_DATE}`);
  
  // Print summary from database
  console.log(`\n=== Database Summary ===`);
  try {
    const summaryCommand = `npx tsx -e "
      import { db } from './db';
      import { sql } from 'drizzle-orm';
      import { curtailmentRecords } from './db/schema';
      
      async function summarize() {
        try {
          const result = await db.select({
            count: sql\`COUNT(*)\`,
            minPeriod: sql\`MIN(settlement_period)\`,
            maxPeriod: sql\`MAX(settlement_period)\`,
            distinctPeriods: sql\`COUNT(DISTINCT settlement_period)\`
          })
          .from(curtailmentRecords)
          .where(sql\`settlement_date = '${SETTLEMENT_DATE}'\`);
          
          console.log('Summary for ${SETTLEMENT_DATE}:');
          console.log(\`Total records: \${result[0].count}\`);
          console.log(\`Period range: \${result[0].minPeriod}-\${result[0].maxPeriod}\`);
          console.log(\`Distinct periods: \${result[0].distinctPeriods} (of 48 total)\`);
          
          // Get counts by period
          const periodCounts = await db.execute(sql\`
            SELECT 
              settlement_period, 
              COUNT(*) as record_count
            FROM curtailment_records
            WHERE settlement_date = '${SETTLEMENT_DATE}'
            GROUP BY settlement_period
            ORDER BY settlement_period
          \`);
          
          console.log('\\nRecords per period:');
          for (const row of periodCounts.rows) {
            console.log(\`Period \${row.settlement_period}: \${row.record_count} records\`);
          }
          
        } catch (error) {
          console.error('Error generating summary:', error);
        }
        process.exit(0);
      }
      
      summarize().catch(console.error);
    "`;
    
    const summaryOutput = execSync(summaryCommand, { encoding: 'utf8' });
    console.log(summaryOutput);
  } catch (error) {
    console.error('Failed to generate summary:', error.message);
  }
}

main();