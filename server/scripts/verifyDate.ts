import { db } from "@db";
import { curtailmentRecords } from "@db/schema";
import { eq, sql } from "drizzle-orm";
import { fetchBidsOffers } from "../services/elexon";

async function verifyDate(date: string) {
  try {
    console.log(`Starting detailed verification of ${date} data...`);
    
    // Get current DB records totals
    const dbTotals = await db
      .select({
        recordCount: sql`COUNT(*)`,
        totalVolume: sql`SUM(${curtailmentRecords.volume}::numeric)`,
        totalPayment: sql`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date));
    
    console.log('\nDB Totals:');
    console.log('Total Records:', dbTotals[0]?.recordCount || 0);
    console.log('Total Volume:', dbTotals[0]?.totalVolume || 0, 'MWh');
    console.log('Total Payment:', dbTotals[0]?.totalPayment || 0, 'GBP');
    
    // Get period-by-period breakdown from DB
    const dbPeriods = await db
      .select({
        period: curtailmentRecords.settlementPeriod,
        recordCount: sql`COUNT(*)`,
        periodVolume: sql`SUM(${curtailmentRecords.volume}::numeric)`,
        periodPayment: sql`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date))
      .groupBy(curtailmentRecords.settlementPeriod)
      .orderBy(curtailmentRecords.settlementPeriod);

    // Sample a few periods to verify against API
    const periodsToCheck = dbPeriods.length > 0 
      ? dbPeriods.map(p => p.period).slice(0, 3) // First 3 periods with data
      : [1, 24, 48]; // Default periods if no data

    console.log('\nChecking selected periods against API...');
    
    for (const period of periodsToCheck) {
      console.log(`\nAnalyzing Period ${period}:`);
      
      // Get current DB records for this period
      const dbRecords = await db
        .select({
          farmId: curtailmentRecords.farmId,
          volume: curtailmentRecords.volume,
          payment: curtailmentRecords.payment,
          originalPrice: curtailmentRecords.originalPrice
        })
        .from(curtailmentRecords)
        .where(sql`${curtailmentRecords.settlementDate} = ${date} AND ${curtailmentRecords.settlementPeriod} = ${period}`);
      
      console.log(`DB Records for Period ${period}:`, dbRecords.length);
      
      // Fetch fresh API data
      console.log(`\nFetching fresh API data for period ${period}...`);
      const apiRecords = await fetchBidsOffers(date, period);
      
      // Compare totals
      const dbPeriodVolume = dbRecords.reduce((sum, r) => sum + Number(r.volume), 0);
      const apiPeriodVolume = apiRecords.reduce((sum, r) => sum + Math.abs(r.volume), 0);
      
      console.log('\nComparison:');
      console.log(`DB Volume: ${dbPeriodVolume}`);
      console.log(`API Volume: ${apiPeriodVolume}`);
      
      // Log details of any mismatches
      if (Math.abs(dbPeriodVolume - apiPeriodVolume) > 0.01) {
        console.log('\nDetailed record comparison:');
        const dbFarms = new Set(dbRecords.map(r => r.farmId));
        const apiFarms = new Set(apiRecords.map(r => r.id));
        
        console.log('Farms in DB but not in API:', 
          [...dbFarms].filter(f => !apiFarms.has(f)));
        console.log('Farms in API but not in DB:', 
          [...apiFarms].filter(f => !dbFarms.has(f)));
      }

      await new Promise(resolve => setTimeout(resolve, 2000)); // Rate limiting
    }
  } catch (error) {
    console.error('Verification failed:', error);
    process.exit(1);
  }
}

// Verify the date
const dateToVerify = process.argv[2] || '2025-01-01';
verifyDate(dateToVerify);
