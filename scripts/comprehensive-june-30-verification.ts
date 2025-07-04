import { db } from '../db/index.js';
import { curtailmentRecords } from '../db/schema.js';
import { eq, and } from 'drizzle-orm';
import { fetchBidsOffers } from '../server/services/elexon.js';

interface PeriodComparison {
  period: number;
  apiCount: number;
  dbCount: number;
  missing: number;
  apiVolume: string;
  dbVolume: string;
  apiPayment: string;
  dbPayment: string;
}

interface VerificationResult {
  date: string;
  totalApiRecords: number;
  totalDbRecords: number;
  totalMissing: number;
  periodsWithData: number[];
  periodsWithMissing: number[];
  periodComparisons: PeriodComparison[];
  apiTotals: {
    volume: number;
    payment: number;
  };
  dbTotals: {
    volume: number;
    payment: number;
  };
}

async function comprehensiveVerification(date: string): Promise<VerificationResult> {
  console.log(`\n=== COMPREHENSIVE VERIFICATION FOR ${date} ===`);
  
  const periodComparisons: PeriodComparison[] = [];
  let totalApiRecords = 0;
  let totalDbRecords = 0;
  let totalMissing = 0;
  const periodsWithData: number[] = [];
  const periodsWithMissing: number[] = [];
  
  let apiTotalVolume = 0;
  let apiTotalPayment = 0;
  let dbTotalVolume = 0;
  let dbTotalPayment = 0;

  // Check each of the 48 settlement periods
  for (let period = 1; period <= 48; period++) {
    try {
      // Fetch API data for this period
      const apiData = await fetchBidsOffers(date, period);
      
      // Fetch DB data for this period
      const dbData = await db.select()
        .from(curtailmentRecords)
        .where(
          and(
            eq(curtailmentRecords.settlementDate, date),
            eq(curtailmentRecords.settlementPeriod, period)
          )
        );

      const apiCount = apiData.length;
      const dbCount = dbData.length;
      const missing = Math.max(0, apiCount - dbCount);
      
      // Calculate volumes and payments
      const apiVolume = apiData.reduce((sum, record) => sum + Math.abs(parseFloat(record.volume)), 0);
      const apiPayment = apiData.reduce((sum, record) => sum + (Math.abs(parseFloat(record.volume)) * Math.abs(parseFloat(record.bidPrice))), 0);
      
      const dbVolume = dbData.reduce((sum, record) => sum + Math.abs(parseFloat(record.volume)), 0);
      const dbPayment = dbData.reduce((sum, record) => sum + parseFloat(record.payment), 0);

      periodComparisons.push({
        period,
        apiCount,
        dbCount,
        missing,
        apiVolume: apiVolume.toFixed(2),
        dbVolume: dbVolume.toFixed(2),
        apiPayment: apiPayment.toFixed(2),
        dbPayment: dbPayment.toFixed(2)
      });

      totalApiRecords += apiCount;
      totalDbRecords += dbCount;
      totalMissing += missing;
      
      apiTotalVolume += apiVolume;
      apiTotalPayment += apiPayment;
      dbTotalVolume += dbVolume;
      dbTotalPayment += dbPayment;

      if (apiCount > 0) {
        periodsWithData.push(period);
      }
      if (missing > 0) {
        periodsWithMissing.push(period);
      }

      // Progress indicator
      if (period % 12 === 0) {
        console.log(`Verified periods 1-${period}...`);
      }
    } catch (error) {
      console.error(`Error verifying period ${period}:`, error);
    }
  }

  return {
    date,
    totalApiRecords,
    totalDbRecords,
    totalMissing,
    periodsWithData,
    periodsWithMissing,
    periodComparisons,
    apiTotals: {
      volume: apiTotalVolume,
      payment: apiTotalPayment
    },
    dbTotals: {
      volume: dbTotalVolume,
      payment: dbTotalPayment
    }
  };
}

async function main() {
  try {
    const result = await comprehensiveVerification('2025-06-30');
    
    console.log('\n=== VERIFICATION RESULTS ===');
    console.log(`Date: ${result.date}`);
    console.log(`API Records: ${result.totalApiRecords}`);
    console.log(`DB Records: ${result.totalDbRecords}`);
    console.log(`Missing Records: ${result.totalMissing}`);
    console.log(`Periods with Data: ${result.periodsWithData.length} periods`);
    console.log(`Periods with Missing Data: ${result.periodsWithMissing.length} periods`);
    
    console.log('\n=== TOTALS COMPARISON ===');
    console.log(`API Volume: ${result.apiTotals.volume.toFixed(2)} MWh`);
    console.log(`DB Volume: ${result.dbTotals.volume.toFixed(2)} MWh`);
    console.log(`API Payment: £${result.apiTotals.payment.toFixed(2)}`);
    console.log(`DB Payment: £${result.dbTotals.payment.toFixed(2)}`);
    
    if (result.periodsWithMissing.length > 0) {
      console.log('\n=== PERIODS WITH MISSING DATA ===');
      result.periodsWithMissing.forEach(period => {
        const comparison = result.periodComparisons.find(p => p.period === period);
        if (comparison) {
          console.log(`Period ${period}: Missing ${comparison.missing} records (API: ${comparison.apiCount}, DB: ${comparison.dbCount})`);
        }
      });
    }
    
    console.log('\n=== DETAILED PERIOD BREAKDOWN ===');
    result.periodComparisons.forEach(comp => {
      if (comp.apiCount > 0) {
        console.log(`Period ${comp.period}: API(${comp.apiCount}) vs DB(${comp.dbCount}) - Vol: ${comp.apiVolume} vs ${comp.dbVolume} MWh`);
      }
    });
    
    // Summary assessment
    if (result.totalMissing === 0) {
      console.log('\n✅ DATA INTEGRITY: COMPLETE');
    } else {
      console.log('\n❌ DATA INTEGRITY: INCOMPLETE');
      console.log(`Action required: Ingest ${result.totalMissing} missing records from ${result.periodsWithMissing.length} periods`);
    }
    
  } catch (error) {
    console.error('Verification failed:', error);
  }
}

main();