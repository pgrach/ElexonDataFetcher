import { db } from "./db";
import { curtailmentRecords } from "./db/schema";
import { eq, sql } from "drizzle-orm";

const TARGET_DATE = '2025-03-05';
const SAMPLE_PERIODS = [31, 32, 33, 34, 44, 45, 46, 47, 48]; // A selection of periods to check

async function getDatabaseStats(date: string) {
  try {
    console.log(`\n=== Database Stats for ${date} ===`);
    
    // Get records for specific periods
    const periodStats = await db
      .select({
        period: curtailmentRecords.settlementPeriod,
        count: sql<number>`count(*)::int`,
        totalVolume: sql<string>`sum(abs(volume::numeric))::text`,
        totalPayment: sql<string>`sum(payment::numeric)::text`,
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date))
      .groupBy(curtailmentRecords.settlementPeriod)
      .orderBy(curtailmentRecords.settlementPeriod);
    
    // Create a map of periods to their stats
    const periodMap = new Map();
    for (const period of periodStats) {
      periodMap.set(period.period, {
        recordCount: period.count,
        totalVolume: parseFloat(period.totalVolume),
        totalPayment: parseFloat(period.totalPayment),
      });
    }
    
    // List stats for the sample periods
    console.log(`\nRecord Counts for Sample Periods:`);
    console.log('Period | In Database | Record Count | Volume (MWh) | Payment (£)');
    console.log('-------|-------------|--------------|--------------|------------');
    
    for (const period of SAMPLE_PERIODS) {
      const stats = periodMap.get(period);
      const inDb = stats ? 'Yes' : 'No';
      const count = stats ? stats.recordCount.toString() : '-';
      const volume = stats ? stats.totalVolume.toFixed(2) : '-';
      const payment = stats ? stats.totalPayment.toFixed(2) : '-';
      
      console.log(`${period.toString().padStart(6, ' ')} | ${inDb.padStart(11, ' ')} | ${count.padStart(12, ' ')} | ${volume.padStart(12, ' ')} | ${payment.padStart(12, ' ')}`);
    }
    
    // Calculate overall totals
    const totalStats = await db
      .select({
        recordCount: sql<number>`count(*)::int`,
        periodCount: sql<number>`count(distinct settlement_period)::int`,
        totalVolume: sql<string>`sum(abs(volume::numeric))::text`,
        totalPayment: sql<string>`sum(payment::numeric)::text`,
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date));
    
    console.log(`\nDatabase Total: ${totalStats[0].recordCount} records across ${totalStats[0].periodCount} periods`);
    console.log(`Total Volume: ${parseFloat(totalStats[0].totalVolume).toFixed(2)} MWh`);
    console.log(`Total Payment: £${parseFloat(totalStats[0].totalPayment).toFixed(2)}`);
    
    return {
      date,
      periodMap,
      periodCount: totalStats[0].periodCount,
      recordCount: totalStats[0].recordCount,
      totalVolume: parseFloat(totalStats[0].totalVolume),
      totalPayment: parseFloat(totalStats[0].totalPayment),
    };
  } catch (error) {
    console.error(`Error getting database stats:`, error);
    return null;
  }
}

async function getAPIData(date: string, periodsToCheck: number[]) {
  try {
    console.log(`\n=== API Data for ${date} (Checking ${periodsToCheck.length} periods) ===`);
    
    const { fetchBidsOffers } = await import('./server/services/elexon');
    
    const apiResults = [];
    
    for (const period of periodsToCheck) {
      const records = await fetchBidsOffers(date, period);
      
      if (records.length > 0) {
        const totalVolume = records.reduce((sum, r) => sum + Math.abs(r.volume), 0);
        const totalPayment = records.reduce((sum, r) => sum + (Math.abs(r.volume) * r.originalPrice), 0);
        
        apiResults.push({
          period,
          recordCount: records.length,
          totalVolume: totalVolume,
          totalPayment: totalPayment,
        });
        
        console.log(`Period ${period}: ${records.length} records, ${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)}`);
      } else {
        console.log(`Period ${period}: No records found in API`);
        apiResults.push({
          period,
          recordCount: 0,
          totalVolume: 0,
          totalPayment: 0,
        });
      }
    }
    
    return apiResults;
  } catch (error) {
    console.error(`Error getting API data:`, error);
    return null;
  }
}

async function auditCurtailmentData() {
  try {
    // Get database stats
    const dbStats = await getDatabaseStats(TARGET_DATE);
    
    // Get API data for sample periods
    const apiData = await getAPIData(TARGET_DATE, SAMPLE_PERIODS);
    
    // Compare database and API data
    if (dbStats && apiData) {
      console.log(`\n=== Comparison ===`);
      console.log('Period | In DB | API Records | Volume (API) | Payment (API) | Missing Records');
      console.log('-------|-------|------------|--------------|---------------|----------------');
      
      let totalMissingRecords = 0;
      let totalMissingVolume = 0;
      let totalMissingPayment = 0;
      
      for (const apiPeriod of apiData) {
        const periodStats = dbStats.periodMap.get(apiPeriod.period);
        const inDb = periodStats ? 'Yes' : 'No';
        const apiRecords = apiPeriod.recordCount;
        const dbRecords = periodStats ? periodStats.recordCount : 0;
        const missingRecords = apiRecords - dbRecords;
        const apiVolume = apiPeriod.totalVolume.toFixed(2);
        const apiPayment = apiPeriod.totalPayment.toFixed(2);
        
        console.log(`${apiPeriod.period.toString().padStart(6, ' ')} | ${inDb.padStart(5, ' ')} | ${apiRecords.toString().padStart(10, ' ')} | ${apiVolume.padStart(12, ' ')} | ${apiPayment.padStart(13, ' ')} | ${missingRecords > 0 ? missingRecords.toString().padStart(14, ' ') : ' '.repeat(14)}`);
        
        if (missingRecords > 0) {
          totalMissingRecords += missingRecords;
          totalMissingVolume += apiPeriod.totalVolume;
          totalMissingPayment += apiPeriod.totalPayment;
        }
      }
      
      // Summarize findings
      console.log(`\n=== Findings ===`);
      console.log(`Database has ${dbStats.recordCount} records across ${dbStats.periodCount} periods`);
      console.log(`Found ${totalMissingRecords} missing records in the ${SAMPLE_PERIODS.length} sample periods checked`);
      console.log(`Missing volume: ${totalMissingVolume.toFixed(2)} MWh`);
      console.log(`Missing payment: £${totalMissingPayment.toFixed(2)}`);
      
      // Estimate total impact
      if (totalMissingRecords > 0) {
        const missingPeriods = SAMPLE_PERIODS.filter(p => !dbStats.periodMap.has(p));
        const totalRemainingPeriods = 48 - dbStats.periodCount;
        
        console.log(`\n=== Estimated Total Impact ===`);
        console.log(`Missing ${missingPeriods.length} out of ${SAMPLE_PERIODS.length} sampled periods`);
        console.log(`Total missing periods: ${totalRemainingPeriods} out of 48`);
        
        if (missingPeriods.length > 0) {
          const avgRecordsPerMissingPeriod = totalMissingRecords / missingPeriods.length;
          const avgVolumePerMissingPeriod = totalMissingVolume / missingPeriods.length;
          const avgPaymentPerMissingPeriod = totalMissingPayment / missingPeriods.length;
          
          const estimatedTotalMissingRecords = avgRecordsPerMissingPeriod * totalRemainingPeriods;
          const estimatedTotalMissingVolume = avgVolumePerMissingPeriod * totalRemainingPeriods;
          const estimatedTotalMissingPayment = avgPaymentPerMissingPeriod * totalRemainingPeriods;
          
          console.log(`Estimated missing records: ~${Math.round(estimatedTotalMissingRecords)}`);
          console.log(`Estimated missing volume: ~${Math.round(estimatedTotalMissingVolume)} MWh`);
          console.log(`Estimated missing payment: ~£${Math.round(estimatedTotalMissingPayment)}`);
          
          console.log(`\nCurrent database total: ${dbStats.totalVolume.toFixed(2)} MWh, £${dbStats.totalPayment.toFixed(2)}`);
          console.log(`Estimated full day total: ~${Math.round(dbStats.totalVolume + estimatedTotalMissingVolume)} MWh, ~£${Math.round(dbStats.totalPayment + estimatedTotalMissingPayment)}`);
        }
      }
    }
  } catch (error) {
    console.error(`Error during audit:`, error);
  }
}

// Run the audit
auditCurtailmentData();