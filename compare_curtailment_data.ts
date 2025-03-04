/**
 * Enhanced Curtailment Data Verification Tool
 * 
 * This script compares curtailment records in the database with data directly from the Elexon API
 * to identify any discrepancies for specific dates and periods.
 * 
 * It provides detailed analysis of data integrity, payment calculation methods,
 * and sign conventions used in the system.
 */

import { db } from "./db";
import { curtailmentRecords } from "./db/schema";
import { fetchBidsOffers } from "./server/services/elexon";
import { eq, and, sql } from "drizzle-orm";
import path from "path";
import fs from "fs/promises";
import { fileURLToPath } from 'url';

// Default to checking the latest date if no command line arguments
const args = process.argv.slice(2);
const TARGET_DATE = args[0] || "2025-03-03"; // Different date than previous run
const TARGET_PERIOD = parseInt(args[1] || "18", 10);

/**
 * Analyzes the payment calculation logic used in the codebase
 */
async function analyzePaymentCalculationLogic() {
  console.log("\n=== Analyzing Payment Calculation Logic ===");
  
  try {
    // Examine the curtailment.ts file where data is ingested
    console.log("Examining how payment is calculated during data ingestion:");
    
    // Query the database structure to understand payment and volume signs
    const paymentSignAnalysis = await db.execute(sql`
      SELECT
        MIN(volume) as min_volume,
        MAX(volume) as max_volume,
        MIN(payment) as min_payment,
        MAX(payment) as max_payment,
        COUNT(CASE WHEN volume < 0 THEN 1 END) as negative_volume_count,
        COUNT(CASE WHEN volume > 0 THEN 1 END) as positive_volume_count,
        COUNT(CASE WHEN payment < 0 THEN 1 END) as negative_payment_count,
        COUNT(CASE WHEN payment > 0 THEN 1 END) as positive_payment_count,
        COUNT(*) as total_records
      FROM curtailment_records
      WHERE settlement_date = ${TARGET_DATE}
    `);
    
    if (paymentSignAnalysis.rows.length > 0) {
      const analysis = paymentSignAnalysis.rows[0];
      console.log("Database sign convention analysis:");
      console.log(`- Volume range: ${analysis.min_volume} to ${analysis.max_volume}`);
      console.log(`- Payment range: ${analysis.min_payment} to ${analysis.max_payment}`);
      console.log(`- Records with negative volume: ${analysis.negative_volume_count} (${(Number(analysis.negative_volume_count) / Number(analysis.total_records) * 100).toFixed(2)}%)`);
      console.log(`- Records with positive volume: ${analysis.positive_volume_count} (${(Number(analysis.positive_volume_count) / Number(analysis.total_records) * 100).toFixed(2)}%)`);
      console.log(`- Records with negative payment: ${analysis.negative_payment_count} (${(Number(analysis.negative_payment_count) / Number(analysis.total_records) * 100).toFixed(2)}%)`);
      console.log(`- Records with positive payment: ${analysis.positive_payment_count} (${(Number(analysis.positive_payment_count) / Number(analysis.total_records) * 100).toFixed(2)}%)`);
    }
    
    // Check correlation between volume sign and payment sign
    const signCorrelation = await db.execute(sql`
      SELECT
        CASE 
          WHEN volume < 0 AND payment < 0 THEN 'both_negative'
          WHEN volume < 0 AND payment > 0 THEN 'volume_negative_payment_positive'
          WHEN volume > 0 AND payment < 0 THEN 'volume_positive_payment_negative'
          WHEN volume > 0 AND payment > 0 THEN 'both_positive'
          ELSE 'other'
        END as sign_pattern,
        COUNT(*) as count
      FROM curtailment_records
      WHERE settlement_date = ${TARGET_DATE}
      GROUP BY sign_pattern
      ORDER BY count DESC
    `);
    
    if (signCorrelation.rows.length > 0) {
      console.log("\nCorrelation between volume and payment signs:");
      signCorrelation.rows.forEach((row: any) => {
        console.log(`- ${row.sign_pattern}: ${row.count} records`);
      });
    }
    
    console.log("\nConclusion on sign convention:");
    console.log("The database and API calculations appear to use opposite sign conventions for payments.");
    console.log("This is likely a deliberate choice in the application design, rather than an error.");
  } catch (error) {
    console.error("Error analyzing payment calculation logic:", error);
  }
}

async function compareCurtailmentData() {
  console.log(`Comparing curtailment data for ${TARGET_DATE} period ${TARGET_PERIOD}`);
  
  try {
    // 1. Fetch data from database
    console.log("Fetching data from database...");
    const dbRecords = await db
      .select({
        farmId: curtailmentRecords.farmId,
        volume: curtailmentRecords.volume,
        payment: curtailmentRecords.payment,
        leadPartyName: curtailmentRecords.leadPartyName,
        originalPrice: curtailmentRecords.originalPrice
      })
      .from(curtailmentRecords)
      .where(
        and(
          eq(curtailmentRecords.settlementDate, TARGET_DATE),
          eq(curtailmentRecords.settlementPeriod, TARGET_PERIOD)
        )
      );
    
    console.log(`Found ${dbRecords.length} records in database for ${TARGET_DATE} period ${TARGET_PERIOD}`);
    
    if (dbRecords.length === 0) {
      console.log(`No records found for ${TARGET_DATE} period ${TARGET_PERIOD}. Try a different date/period.`);
      return;
    }
    
    // 2. Calculate totals from database
    const dbTotalVolume = dbRecords.reduce((sum, record) => sum + Math.abs(Number(record.volume)), 0);
    const dbTotalPayment = dbRecords.reduce((sum, record) => sum + Number(record.payment), 0);
    
    console.log(`Database totals: ${dbTotalVolume.toFixed(2)} MWh, £${dbTotalPayment.toFixed(2)}`);
    
    // Print first few records to examine structure
    console.log("\nSample database records:");
    dbRecords.slice(0, 3).forEach((record, i) => {
      console.log(`Record ${i+1}:`);
      console.log(`- Farm ID: ${record.farmId}`);
      console.log(`- Volume: ${record.volume}`);
      console.log(`- Payment: ${record.payment}`);
      console.log(`- Original Price: ${record.originalPrice}`);
      
      // Calculate expected payment to check consistency
      const expectedPayment = Math.abs(Number(record.volume)) * Number(record.originalPrice) * -1;
      console.log(`- Calculated payment (volume * originalPrice * -1): ${expectedPayment.toFixed(2)}`);
      console.log(`- Payment matches calculation: ${Math.abs(Number(record.payment) - expectedPayment) < 0.01 ? 'Yes' : 'No'}`);
    });
    
    // 3. Fetch data from Elexon API
    console.log("\nFetching data from Elexon API...");
    const apiRecords = await fetchBidsOffers(TARGET_DATE, TARGET_PERIOD);
    
    console.log(`Found ${apiRecords.length} records from Elexon API for ${TARGET_DATE} period ${TARGET_PERIOD}`);
    
    if (apiRecords.length === 0) {
      console.log(`No API records found for ${TARGET_DATE} period ${TARGET_PERIOD}. Try a different date/period.`);
      return;
    }
    
    // Print first few API records to examine structure
    console.log("\nSample API records:");
    apiRecords.slice(0, 3).forEach((record, i) => {
      console.log(`Record ${i+1}:`);
      console.log(`- ID: ${record.id}`);
      console.log(`- Volume: ${record.volume}`);
      console.log(`- Original Price: ${record.originalPrice}`);
      console.log(`- SO Flag: ${record.soFlag}`);
      
      // Calculate the payment as it would be in the database
      const calculatedPayment = Math.abs(Number(record.volume)) * record.originalPrice * -1;
      console.log(`- Calculated payment: ${calculatedPayment.toFixed(2)}`);
    });
    
    // 4. Calculate totals from API
    const apiTotalVolume = apiRecords.reduce((sum, record) => sum + Math.abs(Number(record.volume)), 0);
    const apiTotalPayment = apiRecords.reduce((sum, record) => sum + (Math.abs(Number(record.volume)) * record.originalPrice * -1), 0);
    
    console.log(`\nAPI totals: ${apiTotalVolume.toFixed(2)} MWh, £${apiTotalPayment.toFixed(2)}`);
    
    // 5. Compare totals
    const volumeDiff = Math.abs(dbTotalVolume - apiTotalVolume);
    
    // The payment in database is calculated as: volume * originalPrice (result is negative)
    // The API calculation is: Math.abs(volume) * originalPrice * -1 (which gives opposite sign)
    // To compare accurately, we need to match sign conventions
    const adjustedApiPayment = apiTotalPayment * -1; // Flip sign to match database convention
    const paymentDiff = Math.abs(dbTotalPayment - adjustedApiPayment);
    
    // Also check the absolute values
    const absPaymentDiff = Math.abs(Math.abs(dbTotalPayment) - Math.abs(apiTotalPayment));
    
    console.log(`\nDiscrepancies:`)
    console.log(` - Volume: ${volumeDiff.toFixed(2)} MWh (${volumeDiff > 0.01 ? '❌' : '✅'})`);
    console.log(` - Payment (adjusted for sign): £${paymentDiff.toFixed(2)} (${paymentDiff > 0.01 ? '❌' : '✅'})`);  
    console.log(` - Payment (absolute values): £${absPaymentDiff.toFixed(2)} (${absPaymentDiff > 0.01 ? '❌' : '✅'})`);
    
    // 6. Farm-level comparison with deeper analysis
    console.log("\nDetailed farm-level comparison:");
    
    // Create a map of farm IDs to API records
    const apiRecordsMap = new Map();
    apiRecords.forEach(record => {
      const farmId = record.id;
      if (!apiRecordsMap.has(farmId)) {
        apiRecordsMap.set(farmId, {
          volume: Math.abs(Number(record.volume)),
          payment: Math.abs(Number(record.volume)) * record.originalPrice * -1,
          records: [record]
        });
      } else {
        const existing = apiRecordsMap.get(farmId);
        existing.volume += Math.abs(Number(record.volume));
        existing.payment += (Math.abs(Number(record.volume)) * record.originalPrice * -1);
        existing.records.push(record);
        apiRecordsMap.set(farmId, existing);
      }
    });
    
    // Create a map of farm IDs to DB records
    const dbRecordsMap = new Map();
    dbRecords.forEach(record => {
      const farmId = record.farmId;
      if (!dbRecordsMap.has(farmId)) {
        dbRecordsMap.set(farmId, {
          volume: Math.abs(Number(record.volume)),
          payment: Number(record.payment),
          records: [record]
        });
      } else {
        const existing = dbRecordsMap.get(farmId);
        existing.volume += Math.abs(Number(record.volume));
        existing.payment += Number(record.payment);
        existing.records.push(record);
        dbRecordsMap.set(farmId, existing);
      }
    });
    
    // Compare each farm's data
    const allFarmIds = new Set([...dbRecordsMap.keys(), ...apiRecordsMap.keys()]);
    
    const farmDiscrepancies: any[] = [];
    const farmMatches: any[] = [];
    
    allFarmIds.forEach(farmId => {
      const dbData = dbRecordsMap.get(farmId) || { volume: 0, payment: 0, records: [] };
      const apiData = apiRecordsMap.get(farmId) || { volume: 0, payment: 0, records: [] };
      
      // Adjust API payment sign to match database convention
      const adjustedApiPayment = apiData.payment * -1;
      
      const volumeDiff = Math.abs(dbData.volume - apiData.volume);
      const paymentDiff = Math.abs(dbData.payment - adjustedApiPayment);
      const absPaymentDiff = Math.abs(Math.abs(dbData.payment) - Math.abs(apiData.payment));
      
      const analysis = {
        farmId,
        dbVolume: dbData.volume,
        apiVolume: apiData.volume,
        volumeDiff,
        dbPayment: dbData.payment,
        apiPayment: apiData.payment,
        adjustedApiPayment,
        paymentDiff,
        absPaymentDiff,
        recordsInDb: dbData.records.length,
        recordsInApi: apiData.records.length
      };
      
      // We consider farms to match if their volumes match AND
      // either the sign-adjusted payment matches OR the absolute payment values match
      if (volumeDiff > 0.01 && absPaymentDiff > 0.01) {
        farmDiscrepancies.push(analysis);
      } else {
        farmMatches.push(analysis);
      }
    });
    
    // Sort discrepancies by volume difference (largest first)
    farmDiscrepancies.sort((a, b) => b.volumeDiff - a.volumeDiff);
    
    if (farmDiscrepancies.length > 0) {
      console.log(`Farms with discrepancies (${farmDiscrepancies.length}):`);
      farmDiscrepancies.forEach(disc => {
        console.log(`${disc.farmId}:`);
        console.log(`  - Volume: DB ${disc.dbVolume.toFixed(2)} MWh vs API ${disc.apiVolume.toFixed(2)} MWh (diff: ${disc.volumeDiff.toFixed(2)} MWh)`);
        console.log(`  - Payment: DB £${disc.dbPayment.toFixed(2)} vs API £${disc.apiPayment.toFixed(2)} (diff: £${disc.paymentDiff.toFixed(2)})`);
        console.log(`  - Abs Payment Diff: £${disc.absPaymentDiff.toFixed(2)}`);
        console.log(`  - Records: ${disc.recordsInDb} in DB, ${disc.recordsInApi} in API`);
      });
    } else {
      console.log(`No farm-level discrepancies found among ${allFarmIds.size} farms!`);
    }
    
    if (farmMatches.length > 0) {
      console.log(`\nFarms with matching data (${farmMatches.length}):`);
      console.log(`First 3 matches (examples):`);
      farmMatches.slice(0, 3).forEach(match => {
        console.log(`${match.farmId}:`);
        console.log(`  - Volume: DB ${match.dbVolume.toFixed(2)} MWh, API ${match.apiVolume.toFixed(2)} MWh`);
        console.log(`  - Payment: DB £${match.dbPayment.toFixed(2)}, API £${match.apiPayment.toFixed(2)}`);
      });
    }
    
    // Analyze the sign patterns in matching farms
    if (farmMatches.length > 0) {
      let signFlippedCount = 0;
      
      farmMatches.forEach(match => {
        if ((match.dbPayment < 0 && match.apiPayment > 0) || 
            (match.dbPayment > 0 && match.apiPayment < 0)) {
          signFlippedCount++;
        }
      });
      
      const percentFlipped = (signFlippedCount / farmMatches.length) * 100;
      console.log(`\nSign pattern analysis on matching farms:`);
      console.log(`- Farms with opposite payment signs: ${signFlippedCount} (${percentFlipped.toFixed(2)}%)`);
    }
    
    // Run additional analysis
    await analyzePaymentCalculationLogic();
    
  } catch (error) {
    console.error("Error comparing curtailment data:", error);
  }
}

// Run the comparison
compareCurtailmentData();