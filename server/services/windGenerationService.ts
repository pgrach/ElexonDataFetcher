/**
 * Wind Generation Data Service
 * 
 * This service fetches, processes, and stores aggregated wind generation data
 * from Elexon's B1630 API endpoint (AGWS).
 */

import axios from 'axios';
import { db } from '../../db';
import { windGenerationData, InsertWindGenerationData } from '../../db/schema';
import { sql, eq, and, desc } from 'drizzle-orm';
import { logger } from '../utils/logger';
import { isValidDateString, formatDate, getDateRange } from '../utils/dates';
import { DatabaseError } from '../utils/errors';
import { addDays, subDays, parseISO } from 'date-fns';

// Constants
const ELEXON_API_BASE_URL = 'https://data.elexon.co.uk/bmrs/api/v1';
const MAX_DAYS_PER_REQUEST = 7; // API limitation
const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 5000;
const RATE_LIMIT_DELAY_MS = 60000; // 1 minute
const REQUEST_TIMEOUT_MS = 30000; // 30 seconds

// Types for API response
interface ElexonGenerationData {
  publishTime: string;
  businessType: string; 
  psrType: string;
  quantity: number;
  settlementDate: string;
  settlementPeriod: number;
}

interface ElexonAPIResponse {
  data: ElexonGenerationData[];
}

interface AggregatedWindData {
  settlementDate: string;
  settlementPeriod: number;
  windOnshore: number;
  windOffshore: number;
  totalWind: number;
}

/**
 * Sleep utility function
 */
async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Fetch wind generation data from Elexon API for a given date range
 * 
 * @param startDate - Start date in YYYY-MM-DD format
 * @param endDate - End date in YYYY-MM-DD format (max 7 days from startDate)
 * @param periodFrom - Optional start settlement period (1-48)
 * @param periodTo - Optional end settlement period (1-48)
 */
async function fetchWindGenerationData(
  startDate: string,
  endDate: string,
  periodFrom?: number,
  periodTo?: number,
  retryCount = 0
): Promise<ElexonGenerationData[]> {
  try {
    logger.info(`Fetching wind generation data from ${startDate} to ${endDate}`, {
      module: 'windGenerationService',
      startDate,
      endDate,
      periodFrom,
      periodTo
    });

    // Build URL with parameters
    let url = `${ELEXON_API_BASE_URL}/generation/actual/per-type/wind-and-solar?from=${startDate}&to=${endDate}`;
    
    if (periodFrom) {
      url += `&settlementPeriodFrom=${periodFrom}`;
    }
    
    if (periodTo) {
      url += `&settlementPeriodTo=${periodTo}`;
    }
    
    // Add JSON format parameter
    url += '&format=json';
    
    const response = await axios.get<ElexonAPIResponse>(url, {
      timeout: REQUEST_TIMEOUT_MS,
      headers: {
        Accept: 'application/json'
      }
    });
    
    if (!response.data || !Array.isArray(response.data.data)) {
      throw new Error('Invalid API response format - expected data array');
    }
    
    logger.info(`Retrieved ${response.data.data.length} records for ${startDate} to ${endDate}`, {
      module: 'windGenerationService'
    });
    
    return response.data.data;
  } catch (error) {
    // Handle rate limiting (429 status code)
    if (axios.isAxiosError(error) && error.response?.status === 429 && retryCount < MAX_RETRIES) {
      logger.warning(`Rate limited when fetching wind data, retrying after delay (${retryCount + 1}/${MAX_RETRIES})`, {
        module: 'windGenerationService'
      });
      await delay(RATE_LIMIT_DELAY_MS);
      return fetchWindGenerationData(startDate, endDate, periodFrom, periodTo, retryCount + 1);
    }
    
    // Handle server errors with retry
    if (axios.isAxiosError(error) && error.response?.status >= 500 && retryCount < MAX_RETRIES) {
      const delayMs = RETRY_DELAY_MS * Math.pow(2, retryCount);
      logger.warning(`Server error ${error.response.status}, retrying after ${delayMs}ms (${retryCount + 1}/${MAX_RETRIES})`, {
        module: 'windGenerationService'
      });
      await delay(delayMs);
      return fetchWindGenerationData(startDate, endDate, periodFrom, periodTo, retryCount + 1);
    }
    
    // Other errors
    let errorMessage = 'Unknown error';
    if (axios.isAxiosError(error)) {
      errorMessage = error.response?.data?.error || error.message;
    } else if (error instanceof Error) {
      errorMessage = error.message;
    }
    
    logger.error(`Failed to fetch wind generation data: ${errorMessage}`, {
      module: 'windGenerationService',
      startDate,
      endDate,
      retryCount
    });
    
    throw new Error(`Failed to fetch wind generation data: ${errorMessage}`);
  }
}

/**
 * Process and aggregate wind generation data by settlement date and period
 * 
 * @param data - Array of wind generation data from Elexon API
 */
function aggregateWindData(data: ElexonGenerationData[]): Map<string, AggregatedWindData> {
  const aggregatedData = new Map<string, AggregatedWindData>();
  
  for (const record of data) {
    // Only process wind generation records (ignore solar)
    if (record.businessType !== 'Wind generation' || 
        (record.psrType !== 'Wind Onshore' && record.psrType !== 'Wind Offshore')) {
      continue;
    }
    
    const key = `${record.settlementDate}-${record.settlementPeriod}`;
    
    if (!aggregatedData.has(key)) {
      aggregatedData.set(key, {
        settlementDate: record.settlementDate,
        settlementPeriod: record.settlementPeriod,
        windOnshore: 0,
        windOffshore: 0,
        totalWind: 0
      });
    }
    
    const entry = aggregatedData.get(key)!;
    
    if (record.psrType === 'Wind Onshore') {
      entry.windOnshore += record.quantity;
    } else if (record.psrType === 'Wind Offshore') {
      entry.windOffshore += record.quantity;
    }
    
    entry.totalWind = entry.windOnshore + entry.windOffshore;
    aggregatedData.set(key, entry);
  }
  
  return aggregatedData;
}

/**
 * Save aggregated wind generation data to the database
 * 
 * @param aggregatedData - Map of aggregated wind data by date and period
 */
async function saveAggregatedData(aggregatedData: Map<string, AggregatedWindData>): Promise<number> {
  try {
    if (aggregatedData.size === 0) {
      logger.info('No wind generation data to save', { module: 'windGenerationService' });
      return 0;
    }
    
    logger.info(`Saving ${aggregatedData.size} aggregated wind generation records`, {
      module: 'windGenerationService'
    });
    
    const recordsToInsert: InsertWindGenerationData[] = Array.from(aggregatedData.values()).map(entry => ({
      settlementDate: entry.settlementDate,
      settlementPeriod: entry.settlementPeriod,
      windOnshore: entry.windOnshore.toString(),
      windOffshore: entry.windOffshore.toString(),
      totalWind: entry.totalWind.toString(),
      lastUpdated: new Date(),
      dataSource: 'ELEXON'
    }));
    
    // Use upsert to handle existing records
    // This will insert new records or update existing ones if the unique constraint is violated
    const result = await db
      .insert(windGenerationData)
      .values(recordsToInsert)
      .onConflict({
        target: [windGenerationData.settlementDate, windGenerationData.settlementPeriod],
        set: {
          windOnshore: sql`EXCLUDED.wind_onshore`,
          windOffshore: sql`EXCLUDED.wind_offshore`,
          totalWind: sql`EXCLUDED.total_wind`,
          lastUpdated: sql`CURRENT_TIMESTAMP`
        }
      })
      .returning({ id: windGenerationData.id });
    
    logger.info(`Successfully saved ${result.length} wind generation records`, {
      module: 'windGenerationService'
    });
    
    return result.length;
  } catch (error) {
    logger.error('Error saving wind generation data', {
      module: 'windGenerationService',
      error: error instanceof Error ? error.message : String(error),
      recordCount: aggregatedData.size
    });
    
    throw new DatabaseError('Failed to save wind generation data', {
      context: { errorMessage: error instanceof Error ? error.message : String(error) }
    });
  }
}

/**
 * Process wind generation data for a date range by breaking it into 7-day chunks
 * 
 * @param startDate - Start date in YYYY-MM-DD format
 * @param endDate - End date in YYYY-MM-DD format
 */
export async function processDateRange(startDate: string, endDate: string): Promise<number> {
  // Validate dates
  if (!isValidDateString(startDate) || !isValidDateString(endDate)) {
    throw new Error('Invalid date format. Use YYYY-MM-DD format.');
  }
  
  const start = new Date(startDate);
  const end = new Date(endDate);
  
  if (start > end) {
    throw new Error('Start date must be before end date');
  }
  
  let recordsProcessed = 0;
  let currentStart = new Date(start);
  
  while (currentStart <= end) {
    // Calculate end date for this chunk (either 7 days from start or the overall end date)
    const chunkEnd = new Date(Math.min(
      addDays(currentStart, MAX_DAYS_PER_REQUEST - 1).getTime(),
      end.getTime()
    ));
    
    try {
      // Format dates for API call
      const formattedStart = formatDate(currentStart);
      const formattedEnd = formatDate(chunkEnd);
      
      // Fetch data for this date range
      const data = await fetchWindGenerationData(formattedStart, formattedEnd);
      
      // Aggregate data
      const aggregatedData = aggregateWindData(data);
      
      // Save to database
      const savedCount = await saveAggregatedData(aggregatedData);
      recordsProcessed += savedCount;
      
      logger.info(`Processed wind generation data for ${formattedStart} to ${formattedEnd}`, {
        module: 'windGenerationService',
        recordCount: aggregatedData.size,
        savedCount
      });
    } catch (error) {
      logger.error(`Error processing wind data for ${formatDate(currentStart)} to ${formatDate(chunkEnd)}`, {
        module: 'windGenerationService',
        error: error instanceof Error ? error.message : String(error)
      });
      // Continue with next chunk despite errors
    }
    
    // Move to next chunk
    currentStart = addDays(chunkEnd, 1);
  }
  
  return recordsProcessed;
}

/**
 * Process wind generation data for a single date
 * 
 * @param date - Date in YYYY-MM-DD format
 */
export async function processSingleDate(date: string): Promise<number> {
  return processDateRange(date, date);
}

/**
 * Process wind generation data for the last N days
 * 
 * @param days - Number of days to process (including today)
 */
export async function processRecentDays(days: number = 1): Promise<number> {
  const today = new Date();
  const startDate = subDays(today, days - 1);
  
  return processDateRange(formatDate(startDate), formatDate(today));
}

/**
 * Get wind generation data for a specific date
 * 
 * @param date - Date in YYYY-MM-DD format
 */
export async function getWindGenerationDataForDate(date: string): Promise<any[]> {
  try {
    if (!isValidDateString(date)) {
      throw new Error('Invalid date format. Use YYYY-MM-DD format.');
    }
    
    const result = await db
      .select({
        settlementDate: windGenerationData.settlementDate,
        settlementPeriod: windGenerationData.settlementPeriod,
        windOnshore: windGenerationData.windOnshore,
        windOffshore: windGenerationData.windOffshore,
        totalWind: windGenerationData.totalWind,
        lastUpdated: windGenerationData.lastUpdated
      })
      .from(windGenerationData)
      .where(eq(windGenerationData.settlementDate, date))
      .orderBy(windGenerationData.settlementPeriod);
    
    return result;
  } catch (error) {
    logger.error(`Failed to fetch wind generation data for ${date}`, {
      module: 'windGenerationService',
      error: error instanceof Error ? error.message : String(error)
    });
    
    throw new DatabaseError(`Failed to fetch wind generation data for ${date}`, {
      context: { date, errorMessage: error instanceof Error ? error.message : String(error) }
    });
  }
}

/**
 * Get the latest date with wind generation data
 */
export async function getLatestDataDate(): Promise<string | null> {
  try {
    const result = await db
      .select({ maxDate: windGenerationData.settlementDate })
      .from(windGenerationData)
      .orderBy(desc(windGenerationData.settlementDate))
      .limit(1);
    
    if (result.length === 0) {
      return null;
    }
    
    return formatDate(result[0].maxDate);
  } catch (error) {
    logger.error('Failed to fetch latest wind generation data date', {
      module: 'windGenerationService',
      error: error instanceof Error ? error.message : String(error)
    });
    
    return null;
  }
}

/**
 * Check if wind generation data exists for a specific date
 * 
 * @param date - Date in YYYY-MM-DD format
 */
export async function hasWindDataForDate(date: string): Promise<boolean> {
  try {
    if (!isValidDateString(date)) {
      return false;
    }
    
    const result = await db
      .select({ count: sql<number>`count(*)` })
      .from(windGenerationData)
      .where(eq(windGenerationData.settlementDate, date))
      .limit(1);
    
    return result[0].count > 0;
  } catch (error) {
    logger.error(`Error checking wind data for ${date}`, {
      module: 'windGenerationService',
      error: error instanceof Error ? error.message : String(error)
    });
    
    return false;
  }
}