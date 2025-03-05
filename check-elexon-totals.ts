import { fetchBidsOffers } from "./server/services/elexon";
import { db } from "./db/index";

async function checkElexonTotals(date: string) {
  console.log(`Checking Elexon totals for ${date}...`);
  
  // Sample specific periods (including period 16 which was problematic)
  const periodsToCheck = [16, 43, 47, 48];
  
  let totalVolume = 0;
  let totalPayment = 0;
  let successfulPeriods = 0;
  let recordsCount = 0;
  
  for (const period of periodsToCheck) {
    try {
      console.log(`Fetching period ${period}...`);
      const records = await fetchBidsOffers(date, period);
      
      if (records.length > 0) {
        const periodVolume = records.reduce((sum, r) => sum + Math.abs(r.volume), 0);
        const periodPayment = records.reduce((sum, r) => sum + (Math.abs(r.volume) * r.originalPrice * -1), 0);
        
        totalVolume += periodVolume;
        totalPayment += periodPayment;
        recordsCount += records.length;
        successfulPeriods++;
        
        console.log(`Period ${period}: ${records.length} records, ${periodVolume.toFixed(2)} MWh, £${periodPayment.toFixed(2)}`);
      } else {
        console.log(`Period ${period}: No valid records found`);
      }
    } catch (error) {
      console.error(`Error processing period ${period}:`, error);
    }
    
    // Add a small delay between requests to avoid rate limiting
    await new Promise(resolve => setTimeout(resolve, 500));
  }
  
  console.log(`\n--- Summary for checked periods ---`);
  console.log(`Total periods with data: ${successfulPeriods} out of ${periodsToCheck.length}`);
  console.log(`Total records: ${recordsCount}`);
  console.log(`Total volume: ${totalVolume.toFixed(2)} MWh`);
  console.log(`Total payment: £${totalPayment.toFixed(2)}`);
  
  // Compare with database totals for specific periods
  const periodsToCheckStr = periodsToCheck.join(',');
  const specificPeriodsResult = await pool.query(`
    SELECT 
      COUNT(*) as record_count,
      SUM(ABS(volume::numeric)) as total_volume,
      SUM(payment::numeric) as total_payment,
      COUNT(DISTINCT settlement_period) as total_periods
    FROM curtailment_records 
    WHERE settlement_date = $1
    AND settlement_period IN (${periodsToCheckStr})
  `, [date]);
  
  console.log(`\n--- Database Summary for ${date} (periods ${periodsToCheckStr}) ---`);
  console.log(`Total periods: ${specificPeriodsResult.rows[0].total_periods}`);
  console.log(`Total records: ${specificPeriodsResult.rows[0].record_count}`);
  console.log(`Total volume: ${parseFloat(specificPeriodsResult.rows[0].total_volume || '0').toFixed(2)} MWh`);
  console.log(`Total payment: £${parseFloat(specificPeriodsResult.rows[0].total_payment || '0').toFixed(2)}`);
  
  // Compare with all database totals
  const fullResult = await pool.query(`
    SELECT 
      COUNT(*) as record_count,
      SUM(ABS(volume::numeric)) as total_volume,
      SUM(payment::numeric) as total_payment,
      COUNT(DISTINCT settlement_period) as total_periods
    FROM curtailment_records 
    WHERE settlement_date = $1
  `, [date]);
  
  console.log(`\n--- Database Summary for ${date} (all periods) ---`);
  console.log(`Total periods: ${fullResult.rows[0].total_periods}`);
  console.log(`Total records: ${fullResult.rows[0].record_count}`);
  console.log(`Total volume: ${parseFloat(fullResult.rows[0].total_volume || '0').toFixed(2)} MWh`);
  console.log(`Total payment: £${parseFloat(fullResult.rows[0].total_payment || '0').toFixed(2)}`);
  
  // Clean up the pool connection
  await pool.end();
}

const date = process.argv[2] || "2025-03-04";
checkElexonTotals(date);