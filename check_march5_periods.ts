import { db } from "./db";
import { curtailmentRecords } from "./db/schema";
import { eq, sql } from "drizzle-orm";

const TARGET_DATE = '2025-03-05';
const EXPECTED_PERIODS = 48; // A full day should have 48 half-hour periods

async function getDatabaseStats(date: string) {
  try {
    console.log(`\n=== Database Stats for ${date} ===`);
    
    // Get periods and their record counts
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
    
    // List all periods
    console.log(`\nDetailed Period Stats:`);
    console.log('Period | Records | Volume (MWh) | Payment (£)');
    console.log('-------|---------|--------------|------------');
    
    for (const period of periodStats) {
      console.log(`${period.period.toString().padStart(6, ' ')} | ${period.count.toString().padStart(7, ' ')} | ${parseFloat(period.totalVolume).toFixed(2).padStart(12, ' ')} | ${parseFloat(period.totalPayment).toFixed(2).padStart(12, ' ')}`);
    }
    
    // Calculate totals
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
    
    // Check for missing periods
    const existingPeriods = new Set(periodStats.map(p => p.period));
    const missingPeriods = [];
    
    for (let i = 1; i <= EXPECTED_PERIODS; i++) {
      if (!existingPeriods.has(i)) {
        missingPeriods.push(i);
      }
    }
    
    if (missingPeriods.length > 0) {
      console.log(`\nMissing periods: ${missingPeriods.join(', ')}`);
    } else {
      console.log(`\nAll ${EXPECTED_PERIODS} periods are present in the database.`);
    }
    
    // Check for unexpected periods
    const unexpectedPeriods = [];
    for (const period of existingPeriods) {
      if (period < 1 || period > EXPECTED_PERIODS) {
        unexpectedPeriods.push(period);
      }
    }
    
    if (unexpectedPeriods.length > 0) {
      console.log(`\nUnexpected periods found: ${unexpectedPeriods.join(', ')}`);
    }
    
    return {
      date,
      periodCount: totalStats[0].periodCount,
      recordCount: totalStats[0].recordCount,
      totalVolume: parseFloat(totalStats[0].totalVolume),
      totalPayment: parseFloat(totalStats[0].totalPayment),
      missingPeriods,
      existingPeriods: Array.from(existingPeriods).sort((a, b) => a - b)
    };
  } catch (error) {
    console.error(`Error getting database stats:`, error);
    return null;
  }
}

async function getAPIData(date: string, periodsToCheck: number[]) {
  try {
    // Only for verification purposes - we'll check a few key periods
    console.log(`\n=== API Data for ${date} (sample of ${periodsToCheck.length} periods) ===`);
    
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
    
    // Select some periods to verify from the API 
    // We'll check: lowest period, highest period, middle period, and one missing period if any
    const periodsToCheck = [];
    
    if (dbStats) {
      // Add lowest existing period
      if (dbStats.existingPeriods.length > 0) {
        periodsToCheck.push(dbStats.existingPeriods[0]);
      }
      
      // Add highest existing period
      if (dbStats.existingPeriods.length > 0) {
        periodsToCheck.push(dbStats.existingPeriods[dbStats.existingPeriods.length - 1]);
      }
      
      // Add middle period
      if (dbStats.existingPeriods.length > 0) {
        const middleIndex = Math.floor(dbStats.existingPeriods.length / 2);
        periodsToCheck.push(dbStats.existingPeriods[middleIndex]);
      }
      
      // Add a missing period (if any)
      if (dbStats.missingPeriods.length > 0) {
        periodsToCheck.push(dbStats.missingPeriods[0]);
      }
      
      // Add periods 32, 33, and 34 to specifically check the gaps we saw
      if (!periodsToCheck.includes(32)) periodsToCheck.push(32);
      if (!periodsToCheck.includes(33)) periodsToCheck.push(33);
      if (!periodsToCheck.includes(34)) periodsToCheck.push(34);
    }
    
    // Get API data for selected periods
    const apiData = await getAPIData(TARGET_DATE, periodsToCheck);
    
    // Compare database and API data
    if (dbStats && apiData) {
      console.log(`\n=== Comparison ===`);
      
      for (const apiPeriod of apiData) {
        const dbPeriod = dbStats.existingPeriods.includes(apiPeriod.period);
        
        if (dbPeriod) {
          console.log(`Period ${apiPeriod.period}: Present in database`);
        } else {
          if (apiPeriod.recordCount > 0) {
            console.log(`Period ${apiPeriod.period}: MISSING from database but has ${apiPeriod.recordCount} records in API`);
          } else {
            console.log(`Period ${apiPeriod.period}: Not in database, no records in API`);
          }
        }
      }
      
      // Summarize findings
      console.log(`\n=== Findings ===`);
      console.log(`Database has ${dbStats.recordCount} records across ${dbStats.periodCount} periods`);
      console.log(`Expected ${EXPECTED_PERIODS} periods, missing ${dbStats.missingPeriods.length} periods`);
      
      if (dbStats.missingPeriods.length > 0) {
        const missingPeriodsWithData = apiData
          .filter(a => dbStats.missingPeriods.includes(a.period) && a.recordCount > 0)
          .map(a => a.period);
        
        if (missingPeriodsWithData.length > 0) {
          console.log(`CRITICAL: Periods ${missingPeriodsWithData.join(', ')} have data in API but are missing from database`);
        } else {
          console.log(`The checked missing periods don't appear to have data in the API`);
        }
      }
    }
  } catch (error) {
    console.error(`Error during audit:`, error);
  }
}

// Run the audit
auditCurtailmentData();