/**
 * Script to compare Elexon API data with database data for a specific date
 */
import { fetchBidsOffers } from './server/services/elexon';
import { delay } from './server/services/elexon';
import { db } from './db';
import { curtailmentRecords } from './db/schema';
import { eq, and, isNull } from 'drizzle-orm';

interface ElexonRecord {
  settlementDate: string;
  settlementPeriod: number;
  id: string;
  bmUnit?: string;
  volume: number;
  originalPrice: number;
  finalPrice: number;
}

// Function to insert missing records to database
async function insertMissingRecords(records: ElexonRecord[]): Promise<void> {
  console.log(`Inserting ${records.length} missing records...`);
  
  for (const record of records) {
    if (!record.bmUnit) {
      console.log(`Skipping record with null bmUnit: ${record.id}`);
      continue;
    }
    
    try {
      // Calculate payment as volume * finalPrice
      const payment = record.volume * record.finalPrice;
      
      // Use the column names as defined in the schema
      await db.insert(curtailmentRecords).values({
        settlement_date: record.settlementDate,
        settlement_period: record.settlementPeriod,
        farm_id: record.bmUnit!, // We've already checked it's not null
        volume: Math.abs(record.volume), // Store as positive number
        payment: payment,
        original_price: record.originalPrice,
        final_price: record.finalPrice,
        created_at: new Date()
      });
      
      console.log(`Added record for ${record.bmUnit}: ${Math.abs(record.volume)} MWh, £${payment}`);
    } catch (error) {
      console.error(`Error inserting record for ${record.bmUnit}:`, error);
    }
  }
}

async function checkMultiplePeriods() {
  const date = '2025-03-02';
  const periodsToCheck = Array.from({ length: 10 }, (_, i) => i + 1); // Check periods 1-10
  const results: Record<number, any> = {};
  
  console.log(`Checking multiple periods for date ${date}...`);
  
  for (const period of periodsToCheck) {
    console.log(`\n=== Checking period ${period} ===`);
    
    try {
      // 1. Get Elexon data
      const elexonData = await fetchBidsOffers(date, period);
      console.log(`Fetched ${elexonData.length} records from Elexon API for period ${period}`);
      
      // 2. Get database data
      const dbData = await db.select()
        .from(curtailmentRecords)
        .where(
          and(
            eq(curtailmentRecords.settlementDate, date),
            eq(curtailmentRecords.settlementPeriod, period)
          )
        );
      
      console.log(`Found ${dbData.length} records in database for period ${period}`);
      
      // 3. Compare BMUs between Elexon and database
      const elexonBMUs = new Set(elexonData.filter(item => item.bmUnit).map(item => item.bmUnit));
      const dbBMUs = new Set(dbData.map(item => item.farmId));
      
      // Find BMUs in Elexon but not in DB
      const missingBMUs = [...elexonBMUs].filter(bmu => bmu && !dbBMUs.has(bmu));
      
      console.log(`Missing ${missingBMUs.length} BMUs in database: ${missingBMUs.join(', ')}`);
      
      // Find missing records
      const missingRecords = elexonData.filter(
        item => item.bmUnit && missingBMUs.includes(item.bmUnit)
      );
      
      if (missingRecords.length > 0) {
        console.log(`Missing ${missingRecords.length} records in database`);
        console.log('Missing records details:');
        console.log(JSON.stringify(missingRecords.slice(0, 3), null, 2)); // Show first 3 for brevity
        
        // Insert the missing records
        await insertMissingRecords(missingRecords);
        
        // Re-check after insertion
        const updatedDbData = await db.select()
          .from(curtailmentRecords)
          .where(
            and(
              eq(curtailmentRecords.settlementDate, date),
              eq(curtailmentRecords.settlementPeriod, period)
            )
          );
        
        console.log(`After insertion: ${updatedDbData.length} records in database for period ${period}`);
      }
      
      results[period] = {
        elexonCount: elexonData.length,
        dbCount: dbData.length,
        missingBMUs: missingBMUs,
        missingRecordsCount: missingRecords.length,
        fixed: missingRecords.length > 0
      };
    } catch (error) {
      console.error(`Error checking period ${period}:`, error);
      results[period] = { error: "Failed to process" };
    }
    
    // Add a delay to avoid rate limiting
    await delay(1000);
  }
  
  return results;
}

async function main() {
  try {
    const results = await checkMultiplePeriods();
    console.log('\nSummary:');
    console.log(JSON.stringify(results, null, 2));
    
    // Get total records after fixes
    const date = '2025-03-02';
    const dbData = await db.select()
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date));
    
    console.log(`\nTotal records for ${date} after fixes: ${dbData.length}`);
    
    // Trigger Bitcoin calculation updates if we fixed any records
    let fixesApplied = false;
    for (const period in results) {
      if (results[period].fixed) {
        fixesApplied = true;
        break;
      }
    }
    
    if (fixesApplied) {
      console.log('\nFixes were applied. Updating Bitcoin calculations...');
      // In a real implementation, we would call the reconciliation system here
      console.log('To update Bitcoin calculations, please run:');
      console.log('npx tsx daily_reconciliation_check.ts 3 true');
    }
  } catch (error) {
    console.error('Error:', error);
  }
}

main();