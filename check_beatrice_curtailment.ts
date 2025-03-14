/**
 * Script to check Beatrice Offshore Windfarm Ltd curtailment for February 2025
 * 
 * This script performs a comprehensive analysis of Beatrice Offshore Windfarm
 * curtailment data, examining both database records and direct Elexon API calls
 * to verify data availability and completeness.
 */

import { db } from "./db";
import { curtailmentRecords } from "./db/schema";
import { eq, sql, and, asc } from "drizzle-orm";
import axios from "axios";
import fs from "fs/promises";
import path from "path";

// Constants
const ELEXON_BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1";
const BEATRICE_BMU_IDS = ['T_BEATO-1', 'T_BEATO-2', 'T_BEATO-3', 'T_BEATO-4'];
const TARGET_MONTH = '2025-02'; // February 2025
const REPORT_FILE = 'beatrice_curtailment_report.json';

interface ElexonBidOffer {
  settlementDate: string;
  settlementPeriod: number;
  id: string;
  bmUnit?: string;
  volume: number;
  soFlag: boolean;
  cadlFlag: boolean | null;
  originalPrice: number;
  finalPrice: number;
  leadPartyName?: string;
}

interface MonthlySummary {
  month: string;
  totalVolume: number;
  totalPayment: number;
  recordCount: number;
}

interface MonthlyApiCheck {
  month: string;
  apiAvailable: boolean;
  dataFound: boolean;
  errorMessage?: string;
}

interface BmuInfo {
  id: string;
  capacity: number;
}

interface CurtailmentReport {
  beatriceBmuInfo: BmuInfo[];
  historicalData: MonthlySummary[];
  latestMonth: {
    month: string;
    totalVolume: number;
    recordCount: number;
  };
  apiChecks: MonthlyApiCheck[];
  targetMonth: {
    month: string;
    dbRecords: number;
    apiAvailable: boolean;
    conclusion: string;
  };
  generatedAt: string;
}

/**
 * Delay utility function
 */
async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Get BMU information for Beatrice Offshore Windfarm
 */
async function getBeatriceBmuInfo(): Promise<BmuInfo[]> {
  console.log("Getting Beatrice BMU information...");
  
  const bmuInfo: BmuInfo[] = [
    { id: 'T_BEATO-1', capacity: 138.0 },
    { id: 'T_BEATO-2', capacity: 184.0 },
    { id: 'T_BEATO-3', capacity: 184.0 },
    { id: 'T_BEATO-4', capacity: 166.0 }
  ];
  
  return bmuInfo;
}

/**
 * Get historical monthly data for Beatrice from database
 */
async function getHistoricalMonthlyData(): Promise<MonthlySummary[]> {
  console.log("Fetching historical monthly data from database...");
  
  // Using a simpler query approach with proper result typing
  const query = `
    SELECT 
      date_trunc('month', settlement_date)::date as month, 
      SUM(volume::numeric) as total_volume,
      SUM(payment::numeric) as total_payment,
      COUNT(*) as record_count
    FROM 
      curtailment_records 
    WHERE 
      farm_id IN ('T_BEATO-1', 'T_BEATO-2', 'T_BEATO-3', 'T_BEATO-4')
    GROUP BY 
      date_trunc('month', settlement_date)::date
    ORDER BY 
      month DESC
    LIMIT 24
  `;
  
  // Execute with prepared statement to ensure proper parameter handling
  const results = await db.execute(sql.raw(query));
  
  console.log(`Found ${results.length} months of data`);
  
  // Format the results with type safety
  const monthlyData: MonthlySummary[] = [];
  
  for (const row of results) {
    if (row && typeof row === 'object') {
      const month = row.month?.toString() || '';
      const totalVolume = row.total_volume ? Math.abs(parseFloat(row.total_volume.toString())) : 0;
      const totalPayment = row.total_payment ? parseFloat(row.total_payment.toString()) : 0;
      const recordCount = row.record_count ? parseInt(row.record_count.toString()) : 0;
      
      monthlyData.push({
        month,
        totalVolume,
        totalPayment,
        recordCount
      });
    }
  }
  
  return monthlyData;
}

/**
 * Check if there are any database records for February 2025
 */
async function checkDatabaseForMonth(yearMonth: string): Promise<{
  hasData: boolean;
  count: number;
}> {
  console.log(`Checking database for ${yearMonth}...`);
  
  const startDate = `${yearMonth}-01`;
  const endDate = yearMonth === '2025-02' ? '2025-02-28' : `${yearMonth}-31`;
  
  const results = await db.execute(sql`
    SELECT COUNT(*) as count
    FROM curtailment_records 
    WHERE 
      farm_id IN ('T_BEATO-1', 'T_BEATO-2', 'T_BEATO-3', 'T_BEATO-4')
      AND settlement_date BETWEEN ${startDate} AND ${endDate}
  `);
  
  const count = parseInt(results[0].count.toString());
  return {
    hasData: count > 0,
    count
  };
}

/**
 * Check Elexon API for a specific date
 */
async function checkElexonApiForDate(date: string): Promise<{
  apiAvailable: boolean;
  dataFound: boolean;
  errorMessage?: string;
}> {
  console.log(`Checking Elexon API for ${date}...`);
  
  try {
    // Test just one period to verify API availability
    const [bidsResponse, offersResponse] = await Promise.all([
      axios.get(`${ELEXON_BASE_URL}/balancing/settlement/stack/all/bid/${date}/1`, {
        headers: { 'Accept': 'application/json' },
        timeout: 30000
      }),
      axios.get(`${ELEXON_BASE_URL}/balancing/settlement/stack/all/offer/${date}/1`, {
        headers: { 'Accept': 'application/json' },
        timeout: 30000
      })
    ]).catch(error => {
      if (axios.isAxiosError(error)) {
        throw new Error(`API error: ${error.response?.data?.message || error.message}`);
      }
      throw error;
    });
    
    // If we get here, API is available, now check if any data for Beatrice
    const allData = [...(bidsResponse.data?.data || []), ...(offersResponse.data?.data || [])];
    const beatriceData = allData.filter(record => 
      BEATRICE_BMU_IDS.includes(record.id) && 
      record.volume < 0 && 
      record.soFlag
    );
    
    return {
      apiAvailable: true,
      dataFound: beatriceData.length > 0
    };
  } catch (error) {
    return {
      apiAvailable: false,
      dataFound: false,
      errorMessage: error instanceof Error ? error.message : String(error)
    };
  }
}

/**
 * Run the analysis and generate a report
 */
async function runAnalysis(): Promise<void> {
  const report: CurtailmentReport = {
    beatriceBmuInfo: await getBeatriceBmuInfo(),
    historicalData: await getHistoricalMonthlyData(),
    latestMonth: {
      month: '',
      totalVolume: 0,
      recordCount: 0
    },
    apiChecks: [],
    targetMonth: {
      month: TARGET_MONTH,
      dbRecords: 0,
      apiAvailable: false,
      conclusion: ''
    },
    generatedAt: new Date().toISOString()
  };
  
  // Set latest month data
  if (report.historicalData.length > 0) {
    const latest = report.historicalData[0];
    report.latestMonth = {
      month: latest.month,
      totalVolume: latest.totalVolume,
      recordCount: latest.recordCount
    };
  }
  
  // Check database for target month
  const targetDbCheck = await checkDatabaseForMonth(TARGET_MONTH);
  report.targetMonth.dbRecords = targetDbCheck.count;
  
  // Check API for a selection of dates
  // 1. Check the target month (future)
  const targetApiCheck = await checkElexonApiForDate(`${TARGET_MONTH}-15`);
  report.apiChecks.push({
    month: TARGET_MONTH,
    apiAvailable: targetApiCheck.apiAvailable,
    dataFound: targetApiCheck.dataFound,
    errorMessage: targetApiCheck.errorMessage
  });
  report.targetMonth.apiAvailable = targetApiCheck.apiAvailable;
  
  // 2. Check the latest month we have data for
  if (report.latestMonth.month) {
    const latestApiCheck = await checkElexonApiForDate(
      `${report.latestMonth.month.substring(0, 7)}-15`
    );
    report.apiChecks.push({
      month: report.latestMonth.month.substring(0, 7),
      apiAvailable: latestApiCheck.apiAvailable,
      dataFound: latestApiCheck.dataFound,
      errorMessage: latestApiCheck.errorMessage
    });
  }
  
  // 3. Check a month we know should be available
  const knownGoodMonth = '2024-11';
  const knownGoodApiCheck = await checkElexonApiForDate(`${knownGoodMonth}-15`);
  report.apiChecks.push({
    month: knownGoodMonth,
    apiAvailable: knownGoodApiCheck.apiAvailable,
    dataFound: knownGoodApiCheck.dataFound,
    errorMessage: knownGoodApiCheck.errorMessage
  });
  
  // Formulate conclusion
  if (targetDbCheck.hasData) {
    report.targetMonth.conclusion = `Database already contains ${targetDbCheck.count} records for Beatrice Offshore Windfarm in ${TARGET_MONTH}.`;
  } else if (!targetApiCheck.apiAvailable) {
    report.targetMonth.conclusion = `The Elexon API is not yet providing data for ${TARGET_MONTH}. This is expected for future dates. The most recent data available is from ${report.latestMonth.month}.`;
  } else if (targetApiCheck.apiAvailable && !targetApiCheck.dataFound) {
    report.targetMonth.conclusion = `The Elexon API is available for ${TARGET_MONTH}, but no curtailment data was found for Beatrice Offshore Windfarm. This could mean there was no curtailment during this period.`;
  } else {
    report.targetMonth.conclusion = `The Elexon API has curtailment data for ${TARGET_MONTH} that should be ingested into our database.`;
  }
  
  // Save the report
  await fs.writeFile(REPORT_FILE, JSON.stringify(report, null, 2));
  console.log(`Report generated and saved to ${REPORT_FILE}`);
  
  // Print summary to console
  console.log("\n=== BEATRICE OFFSHORE WINDFARM CURTAILMENT ANALYSIS ===");
  console.log(`Analyzed ${report.historicalData.length} months of historical data`);
  console.log(`Most recent data: ${report.latestMonth.month} with ${report.latestMonth.totalVolume.toFixed(2)} MWh total curtailment`);
  console.log(`Target month (${TARGET_MONTH}) database records: ${report.targetMonth.dbRecords}`);
  console.log(`Target month API available: ${report.targetMonth.apiAvailable ? "Yes" : "No"}`);
  console.log("\nConclusion:");
  console.log(report.targetMonth.conclusion);
  console.log("\nHistorical Monthly Curtailment (Last 6 Months):");
  report.historicalData.slice(0, 6).forEach(month => {
    console.log(`${month.month}: ${month.totalVolume.toFixed(2)} MWh (${month.recordCount} records)`);
  });
}

// Run the analysis
runAnalysis().catch(error => {
  console.error("Error running analysis:", error);
});