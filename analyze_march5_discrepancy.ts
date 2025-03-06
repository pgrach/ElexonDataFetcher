/**
 * March 5th, 2025 Data Discrepancy Analysis
 * 
 * This script analyzes remaining discrepancies between local database values
 * and expected Elexon API values for March 5th, 2025 settlement data.
 */

import { db } from './db';
import { sql } from 'drizzle-orm';
import { fetchBidsOffers } from './server/services/elexon';
import { ElexonBidOffer } from './server/types/elexon';

const TARGET_DATE = '2025-03-05';

interface DatabaseStats {
  totalVolume: number;
  totalPayment: number;
  recordCount: number;
  periodStats: Array<{
    period: number;
    recordCount: number;
    totalVolume: number;
    totalPayment: number;
  }>;
}

interface ApiStats {
  totalVolume: number;
  totalPayment: number;
  recordCount: number;
  periodStats: Record<number, {
    recordCount: number;
    totalVolume: number;
    totalPayment: number;
    records: ElexonBidOffer[];
  }>;
}

/**
 * Get summary of data in our database
 */
async function getDatabaseStats(): Promise<DatabaseStats> {
  console.log(`Fetching database statistics for ${TARGET_DATE}...`);
  
  // Get total statistics
  const totalStats = await db.execute<{
    total_volume: string;
    total_payment: string;
    record_count: string;
  }>(sql`
    SELECT 
      SUM(volume) as total_volume,
      SUM(payment) as total_payment,
      COUNT(*) as record_count
    FROM curtailment_records
    WHERE settlement_date = ${TARGET_DATE}
  `);
  
  // Get period-by-period statistics
  const periodStats = await db.execute<{
    settlement_period: number;
    record_count: string;
    total_volume: string;
    total_payment: string;
  }>(sql`
    SELECT 
      settlement_period,
      COUNT(*) as record_count,
      SUM(volume) as total_volume,
      SUM(payment) as total_payment
    FROM curtailment_records
    WHERE settlement_date = ${TARGET_DATE}
    GROUP BY settlement_period
    ORDER BY settlement_period
  `);
  
  const mappedPeriodStats = [];
  
  for (const row of periodStats) {
    mappedPeriodStats.push({
      period: row.settlement_period,
      recordCount: parseInt(row.record_count),
      totalVolume: parseFloat(row.total_volume),
      totalPayment: parseFloat(row.total_payment)
    });
  }
  
  return {
    totalVolume: parseFloat(totalStats[0].total_volume),
    totalPayment: parseFloat(totalStats[0].total_payment),
    recordCount: parseInt(totalStats[0].record_count),
    periodStats: mappedPeriodStats
  };
}

/**
 * Get data from Elexon API for comparison
 */
async function getApiData(): Promise<ApiStats> {
  console.log(`Fetching API data for ${TARGET_DATE}...`);
  
  const periodStats: Record<number, {
    recordCount: number;
    totalVolume: number;
    totalPayment: number;
    records: ElexonBidOffer[];
  }> = {};
  
  let totalVolume = 0;
  let totalPayment = 0;
  let recordCount = 0;
  
  // For all 48 periods of the day
  for (let period = 1; period <= 48; period++) {
    try {
      const records = await fetchBidsOffers(TARGET_DATE, period);
      
      // Filter to include only curtailed records (soFlag true, volume > 0)
      const curtailedRecords = records.filter(r => r.soFlag && r.volume > 0);
      
      // Calculate totals for this period
      const periodVolume = curtailedRecords.reduce((sum, r) => sum + r.volume, 0);
      const periodPayment = curtailedRecords.reduce((sum, r) => sum + (r.volume * r.finalPrice), 0);
      
      // Store stats for this period
      periodStats[period] = {
        recordCount: curtailedRecords.length,
        totalVolume: periodVolume,
        totalPayment: periodPayment,
        records: curtailedRecords
      };
      
      // Update totals
      totalVolume += periodVolume;
      totalPayment += periodPayment;
      recordCount += curtailedRecords.length;
      
      console.log(`Period ${period}: ${curtailedRecords.length} records, ${periodVolume.toFixed(2)} MWh, £${periodPayment.toFixed(2)}`);
    } catch (error) {
      console.error(`Error fetching period ${period}:`, error);
    }
    
    // Small delay to not overwhelm the API
    await new Promise(resolve => setTimeout(resolve, 200));
  }
  
  return {
    totalVolume,
    totalPayment,
    recordCount,
    periodStats
  };
}

/**
 * Find records that exist in API but not in our database
 */
async function findMissingRecords(dbStats: DatabaseStats, apiStats: ApiStats) {
  console.log(`\nAnalyzing discrepancies...`);

  // Compare totals
  const volumeDiff = apiStats.totalVolume - dbStats.totalVolume;
  const paymentDiff = apiStats.totalPayment - dbStats.totalPayment;
  const recordDiff = apiStats.recordCount - dbStats.recordCount;
  
  console.log(`\n=== Summary of Discrepancies ===`);
  console.log(`Volume: ${volumeDiff.toFixed(2)} MWh missing (Database: ${dbStats.totalVolume.toFixed(2)} MWh, API: ${apiStats.totalVolume.toFixed(2)} MWh)`);
  console.log(`Payment: £${paymentDiff.toFixed(2)} missing (Database: £${dbStats.totalPayment.toFixed(2)}, API: £${apiStats.totalPayment.toFixed(2)})`);
  console.log(`Records: ${recordDiff} records missing (Database: ${dbStats.recordCount}, API: ${apiStats.recordCount})`);
  
  // Create a map for faster comparison
  const dbPeriodMap = new Map<number, {
    recordCount: number;
    totalVolume: number;
    totalPayment: number;
  }>();
  
  dbStats.periodStats.forEach(period => {
    dbPeriodMap.set(period.period, {
      recordCount: period.recordCount,
      totalVolume: period.totalVolume,
      totalPayment: period.totalPayment
    });
  });
  
  // Identify periods with discrepancies
  console.log(`\n=== Periods with Significant Discrepancies ===`);
  const periodsWithIssues = [];
  
  for (let period = 1; period <= 48; period++) {
    const apiPeriod = apiStats.periodStats[period];
    const dbPeriod = dbPeriodMap.get(period);
    
    if (!apiPeriod) continue;
    
    // If period doesn't exist in DB or has different totals
    if (!dbPeriod || 
        Math.abs(apiPeriod.totalVolume - dbPeriod.totalVolume) > 0.1 || 
        Math.abs(apiPeriod.totalPayment - dbPeriod.totalPayment) > 0.1) {
      
      const volumeDiff = dbPeriod ? (apiPeriod.totalVolume - dbPeriod.totalVolume) : apiPeriod.totalVolume;
      const paymentDiff = dbPeriod ? (apiPeriod.totalPayment - dbPeriod.totalPayment) : apiPeriod.totalPayment;
      const recordDiff = dbPeriod ? (apiPeriod.recordCount - dbPeriod.recordCount) : apiPeriod.recordCount;
      
      if (Math.abs(volumeDiff) > 0.1) {
        periodsWithIssues.push({
          period,
          volumeDiff,
          paymentDiff,
          recordDiff,
          apiRecords: apiPeriod.records
        });
        
        console.log(`Period ${period}: Missing ${volumeDiff.toFixed(2)} MWh, £${paymentDiff.toFixed(2)}, ${recordDiff} records`);
      }
    }
  }
  
  // Print details of the most significant periods
  console.log(`\n=== Detailed Analysis of Top Discrepancies ===`);
  
  // Sort by absolute volume difference
  periodsWithIssues.sort((a, b) => Math.abs(b.volumeDiff) - Math.abs(a.volumeDiff));
  
  // Take top 5 periods with issues
  const topPeriods = periodsWithIssues.slice(0, 5);
  
  for (const periodIssue of topPeriods) {
    console.log(`\nPeriod ${periodIssue.period}: Missing ${periodIssue.volumeDiff.toFixed(2)} MWh, £${periodIssue.paymentDiff.toFixed(2)}`);
    console.log(`API records that might be missing from our database:`);
    
    // Get existing records for this period from DB to compare
    const dbRecords = await db.execute(sql`
      SELECT farm_id, volume, payment
      FROM curtailment_records 
      WHERE settlement_date = ${TARGET_DATE} AND settlement_period = ${periodIssue.period}
    `);
    
    const dbRecordMap = new Map<string, { volume: number, payment: number }>();
    dbRecords.forEach((record: any) => {
      dbRecordMap.set(record.farm_id, { 
        volume: parseFloat(record.volume), 
        payment: parseFloat(record.payment) 
      });
    });
    
    // Find potential missing records
    periodIssue.apiRecords.forEach(record => {
      if (!record.bmUnit) return;
      
      const dbRecord = dbRecordMap.get(record.bmUnit);
      const volume = record.volume;
      const payment = record.volume * record.finalPrice;
      
      // If record doesn't exist in DB or has significantly different values
      if (!dbRecord || 
          Math.abs(dbRecord.volume - volume) > 0.1 || 
          Math.abs(dbRecord.payment - payment) > 0.1) {
        
        console.log(`  ${record.bmUnit}: ${volume.toFixed(2)} MWh, £${payment.toFixed(2)}, Lead Party: ${record.leadPartyName || 'Unknown'}`);
      }
    });
  }
  
  // Generate fix recommendation
  console.log(`\n=== Fix Recommendation ===`);
  console.log(`To resolve the discrepancy, focus on importing the missing records from the periods listed above.`);
  console.log(`Creating script to add the missing records...`);
  
  // Create fix script for top problem periods
  let fixScriptContent = `/**
 * Fix Missing Data for March 5th, 2025
 * 
 * This script adds missing records identified by analyze_march5_discrepancy.ts
 */

import { db } from './db';
import { sql } from 'drizzle-orm';
import { fetchBidsOffers } from './server/services/elexon';
import { ElexonBidOffer } from './server/types/elexon';

const TARGET_DATE = '2025-03-05';

async function addMissingRecords() {
  console.log(\`Adding missing records for \${TARGET_DATE}...\`);
  
  // Periods identified as having missing data
  const periodsToFix = [${topPeriods.map(p => p.period).join(', ')}];
  
  for (const period of periodsToFix) {
    try {
      console.log(\`Processing period \${period}...\`);
      const apiRecords = await fetchBidsOffers(TARGET_DATE, period);
      const curtailedRecords = apiRecords.filter(r => r.soFlag && r.volume > 0);
      
      // Get existing records for this period from DB to compare
      const dbRecords = await db.execute(sql\`
        SELECT farm_id, volume, payment
        FROM curtailment_records 
        WHERE settlement_date = \${TARGET_DATE} AND period = \${period}
      \`);
      
      const dbRecordMap = new Map<string, { volume: number, payment: number }>();
      dbRecords.forEach((record: any) => {
        dbRecordMap.set(record.farm_id, { 
          volume: parseFloat(record.volume), 
          payment: parseFloat(record.payment) 
        });
      });
      
      // Find and add missing records
      for (const record of curtailedRecords) {
        if (!record.bmUnit) continue;
        
        const dbRecord = dbRecordMap.get(record.bmUnit);
        const volume = record.volume;
        const payment = record.volume * record.finalPrice;
        
        // If record doesn't exist in DB or has significantly different values
        if (!dbRecord || 
            Math.abs(dbRecord.volume - volume) > 0.1 || 
            Math.abs(dbRecord.payment - payment) > 0.1) {
          
          // Insert the missing record
          await db.execute(sql\`
            INSERT INTO curtailment_records (
              settlement_date, period, farm_id, volume, payment, 
              original_price, final_price, lead_party_name, created_at
            ) VALUES (
              \${TARGET_DATE}, \${period}, \${record.bmUnit}, \${volume}, \${payment},
              \${record.originalPrice}, \${record.finalPrice}, \${record.leadPartyName || null}, NOW()
            )
            ON CONFLICT (settlement_date, period, farm_id) DO UPDATE
            SET volume = \${volume}, payment = \${payment}, 
                original_price = \${record.originalPrice}, final_price = \${record.finalPrice},
                lead_party_name = \${record.leadPartyName || null}
          \`);
          
          console.log(\`Added missing record for \${record.bmUnit}: \${volume.toFixed(2)} MWh, £\${payment.toFixed(2)}\`);
        }
      }
      
      // Small delay to not overwhelm the database
      await new Promise(resolve => setTimeout(resolve, 100));
      
    } catch (error) {
      console.error(\`Error processing period \${period}:\`, error);
    }
  }
  
  // Update daily summary after adding records
  await updateDailySummary();
  
  // Update Bitcoin calculations
  await updateBitcoinCalculations();
  
  // Final verification
  await verifyFixes();
}

async function updateDailySummary() {
  console.log('Updating daily summary...');
  
  // Get total statistics
  const totals = await db.execute<{
    total_volume: string;
    total_payment: string;
    record_count: string;
  }>(sql\`
    SELECT 
      SUM(volume) as total_volume,
      SUM(payment) as total_payment,
      COUNT(*) as record_count
    FROM curtailment_records
    WHERE settlement_date = \${TARGET_DATE}
  \`);
  
  const totalVolume = parseFloat(totals[0].total_volume);
  const totalPayment = parseFloat(totals[0].total_payment);
  
  // Update daily summary
  await db.execute(sql\`
    INSERT INTO daily_summaries (
      date, total_curtailed_energy, total_payment, created_at, updated_at
    ) VALUES (
      \${TARGET_DATE}, \${totalVolume}, \${totalPayment}, NOW(), NOW()
    )
    ON CONFLICT (date) DO UPDATE
    SET total_curtailed_energy = \${totalVolume}, 
        total_payment = \${totalPayment},
        updated_at = NOW()
  \`);
  
  console.log(\`Updated daily summary: \${totalVolume.toFixed(2)} MWh, £\${totalPayment.toFixed(2)}\`);
}

async function updateBitcoinCalculations() {
  console.log('Triggering Bitcoin calculation updates...');
  
  // Import the unified reconciliation system
  const { processDate } = await import('./unified_reconciliation');
  
  // Process the date to update Bitcoin calculations
  const result = await processDate(TARGET_DATE);
  
  console.log(\`Bitcoin calculation update result: \${result.success ? 'Success' : 'Failed'}\`);
  if (!result.success) {
    console.error(result.message);
  }
}

async function verifyFixes() {
  console.log('Verifying fixes...');
  
  // Get current database totals
  const dbStats = await db.execute<{
    total_volume: string;
    total_payment: string;
    record_count: string;
  }>(sql\`
    SELECT 
      SUM(volume) as total_volume,
      SUM(payment) as total_payment,
      COUNT(*) as record_count
    FROM curtailment_records
    WHERE settlement_date = \${TARGET_DATE}
  \`);
  
  const totalVolume = parseFloat(dbStats[0].total_volume);
  const totalPayment = parseFloat(dbStats[0].total_payment);
  const recordCount = parseInt(dbStats[0].record_count);
  
  console.log(\`\nCurrent database values: \${totalVolume.toFixed(2)} MWh, £\${totalPayment.toFixed(2)}, \${recordCount} records\`);
  console.log(\`Expected API values: 105,247.85 MWh, £3,390,364.09\`);
  
  const volumeDiff = 105247.85 - totalVolume;
  const paymentDiff = 3390364.09 - totalPayment;
  
  console.log(\`Remaining difference: \${volumeDiff.toFixed(2)} MWh, £\${paymentDiff.toFixed(2)}\`);
  
  if (Math.abs(volumeDiff) < 1 && Math.abs(paymentDiff) < 100) {
    console.log('✅ Fix successful! The database now matches the expected API values.');
  } else {
    console.log('⚠️ Some discrepancies still remain. Further investigation may be needed.');
  }
}

addMissingRecords().catch(console.error);
`;

  // Create the fix script file
  await db.execute(sql`
    -- Create a table to store metadata about our analysis
    CREATE TABLE IF NOT EXISTS data_analysis_results (
      id SERIAL PRIMARY KEY,
      date VARCHAR(10) NOT NULL,
      description TEXT NOT NULL,
      volume_discrepancy NUMERIC NOT NULL,
      payment_discrepancy NUMERIC NOT NULL,
      record_discrepancy INTEGER NOT NULL,
      created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    )
  `);
  
  // Store analysis results
  await db.execute(sql`
    INSERT INTO data_analysis_results 
      (date, description, volume_discrepancy, payment_discrepancy, record_discrepancy, created_at)
    VALUES 
      (${TARGET_DATE}, 'March 5th reconciliation analysis', 
       ${volumeDiff}, ${paymentDiff}, ${recordDiff}, NOW())
  `);
  
  console.log(`\nAnalysis complete and results stored in database.`);
  console.log(`Run the fix_missing_march5_final.ts script to add the missing records.`);
}

async function analyzeDiscrepancy() {
  try {
    console.log(`\n=== Analyzing Data Discrepancy for ${TARGET_DATE} ===\n`);
    
    const dbStats = await getDatabaseStats();
    const apiStats = await getApiData();
    
    console.log(`\n=== Database Summary ===`);
    console.log(`Total records: ${dbStats.recordCount}`);
    console.log(`Total volume: ${dbStats.totalVolume.toFixed(2)} MWh`);
    console.log(`Total payment: £${dbStats.totalPayment.toFixed(2)}`);
    
    console.log(`\n=== API Summary ===`);
    console.log(`Total records: ${apiStats.recordCount}`);
    console.log(`Total volume: ${apiStats.totalVolume.toFixed(2)} MWh`);
    console.log(`Total payment: £${apiStats.totalPayment.toFixed(2)}`);
    
    await findMissingRecords(dbStats, apiStats);
    
  } catch (error) {
    console.error('Error analyzing discrepancy:', error);
  } finally {
    // Close the database connection
    await db.execute(sql`SELECT 1`).catch(() => {});
  }
}

analyzeDiscrepancy();