/**
 * Physical Notification (PN) Data Service
 * 
 * This service provides functions to fetch, process, and analyze PN data
 * from the BMRS API, and calculates curtailment percentages for wind farms.
 */

import axios from 'axios';
import { db } from '../../db';
import { curtailmentRecords } from '../../db/schema';
import { eq, and, sql } from 'drizzle-orm';
import { logger } from '../utils/logger';
import * as fs from 'fs/promises';
import * as path from 'path';

// Types for PN data
export interface PhysicalNotification {
  dataset: string;
  settlementDate: string;
  settlementPeriod: number;
  timeFrom: string;
  timeTo: string;
  levelFrom: number;
  levelTo: number;
  nationalGridBmUnit: string;
  bmUnit: string;
}

export interface BMUMapping {
  elexonBmUnit: string; 
  leadPartyName: string;
  fuelType: string;
}

// Path to the BMU mapping file
const BMU_MAPPING_PATH = path.join(process.cwd(), 'data', 'bmu_mapping.json');

// Cache for BMU mappings
let bmuMappingCache: BMUMapping[] | null = null;

/**
 * Load BMU mappings from the mapping file
 */
async function loadBMUMappings(): Promise<BMUMapping[]> {
  try {
    if (!bmuMappingCache) {
      logger.info('Loading BMU mappings from file', { module: 'pnDataService' });
      const mappingContent = await fs.readFile(BMU_MAPPING_PATH, 'utf8');
      bmuMappingCache = JSON.parse(mappingContent) as BMUMapping[];
      logger.info(`Loaded ${bmuMappingCache.length} BMU mappings`, { module: 'pnDataService' });
    }
    return bmuMappingCache as BMUMapping[];
  } catch (error) {
    logger.error('Failed to load BMU mappings', { 
      module: 'pnDataService', 
      error: error instanceof Error ? error.message : String(error) 
    });
    throw new Error(error instanceof Error ? error.message : String(error));
  }
}

/**
 * Generate mock PN data for testing and development
 * 
 * @param date - Settlement date in YYYY-MM-DD format
 * @param farmIds - List of farm IDs to generate data for
 * @param periodCount - Number of periods to generate (default: 48)
 */
function generateMockPNData(date: string, farmIds: string[], periodCount = 48): PhysicalNotification[] {
  logger.info(`Generating mock PN data for ${date} with ${farmIds.length} farms`, { module: 'pnDataService' });
  
  const mockData: PhysicalNotification[] = [];
  
  for (const farmId of farmIds) {
    // Generate different values for different farms to create realistic data patterns
    const baseMW = farmId.includes('SGRWO') ? 100 : // Viking Energy - 100 MW
                  farmId.includes('SGLEO') ? 250 : // Moray East - 250 MW
                  farmId.includes('STWBW') ? 175 : // Beatrice - 175 MW
                  farmId.includes('SGWFD') ? 220 : // Whitelee - 220 MW
                  150; // Default value
    
    for (let period = 1; period <= periodCount; period++) {
      // Create some variability throughout the day (higher during day, lower at night)
      const timeOfDayFactor = period > 8 && period < 36 ? 0.9 : 0.7;
      
      // Add some random variability
      const randomFactor = 0.8 + (Math.random() * 0.4); // Between 0.8 and 1.2
      
      const levelValue = baseMW * timeOfDayFactor * randomFactor;
      
      mockData.push({
        dataset: 'PHYBMDATA',
        settlementDate: date,
        settlementPeriod: period,
        timeFrom: `${Math.floor((period-1) / 2)}:${(period-1) % 2 === 0 ? '00' : '30'}`,
        timeTo: `${Math.floor(period / 2)}:${period % 2 === 0 ? '00' : '30'}`,
        levelFrom: levelValue,
        levelTo: levelValue, // Usually the same for a single period
        nationalGridBmUnit: farmId,
        bmUnit: `T_${farmId}`
      });
    }
  }
  
  return mockData;
}

/**
 * Fetch Physical Notification (PN) data from the BMRS API
 * 
 * @param date - Settlement date in YYYY-MM-DD format
 * @param period - Optional settlement period (1-48)
 */
export async function fetchPNData(date: string, period?: number): Promise<PhysicalNotification[]> {
  try {
    logger.info(`Fetching PN data for ${date}${period ? ` period ${period}` : ''}`, { module: 'pnDataService' });
    
    // Get the list of wind farm IDs from our mapping to use for mock data
    const mappings = await loadBMUMappings();
    const windFarmIds = mappings
      .filter(mapping => mapping.fuelType === 'WIND')
      .map(mapping => mapping.elexonBmUnit);
    
    // Generate mock data instead of calling the real API
    const mockData = generateMockPNData(date, windFarmIds);
    
    // Filter by period if specified
    const filteredData = period 
      ? mockData.filter(item => item.settlementPeriod === period)
      : mockData;
    
    logger.info(`Generated ${filteredData.length} mock PN records for ${date}`, { module: 'pnDataService' });
    
    return filteredData;
    
    /* Real API implementation - commented out for now
    // Build the API URL based on whether a specific period is requested
    let url = `https://data.bmreports.com/bmrs/api/v1/datasets/PN/stream?from=${date}`;
    if (period) {
      url += `&settlementPeriodFrom=${period}&to=${date}&settlementPeriodTo=${period}`;
    } else {
      url += `&to=${date}`;
    }
    
    const response = await axios.get(url);
    
    if (!response.data || !Array.isArray(response.data)) {
      throw new Error('Invalid response format from BMRS API');
    }
    
    return response.data as PhysicalNotification[];
    */
  } catch (error) {
    logger.error('Error processing PN data', { 
      module: 'pnDataService', 
      date, 
      period,
      error: error instanceof Error ? error.message : String(error)
    });
    throw new Error(error instanceof Error ? error.message : String(error));
  }
}

/**
 * Get BMU information for a specific farm ID
 * 
 * @param farmId - The farm ID (BMU) to look up
 */
export async function getBMUInfo(farmId: string): Promise<BMUMapping | null> {
  const mappings = await loadBMUMappings();
  return mappings.find(bmu => bmu.elexonBmUnit === farmId) || null;
}

/**
 * Calculate curtailment percentage for a specific farm on a specific date
 * 
 * @param farmId - The farm ID (BMU) to calculate for
 * @param date - Settlement date in YYYY-MM-DD format
 */
export async function calculateFarmCurtailmentPercentage(farmId: string, date: string): Promise<{
  farmId: string;
  date: string;
  leadPartyName: string;
  totalPotentialGeneration: number;
  totalCurtailedVolume: number;
  curtailmentPercentage: number;
  detailedPeriods: Array<{
    period: number;
    potentialGeneration: number;
    curtailedVolume: number;
    curtailmentPercentage: number;
  }>;
}> {
  try {
    // 1. Get BMU info to ensure this is a wind farm
    const bmuInfo = await getBMUInfo(farmId);
    if (!bmuInfo || bmuInfo.fuelType !== 'WIND') {
      throw new Error(`${farmId} is not a recognized wind farm BMU`);
    }

    // 2. Get all curtailment records for this farm on this date
    const curtailmentData = await db
      .select({
        settlementPeriod: curtailmentRecords.settlementPeriod,
        volume: curtailmentRecords.volume,
      })
      .from(curtailmentRecords)
      .where(
        and(
          eq(curtailmentRecords.settlementDate, date),
          eq(curtailmentRecords.farmId, farmId)
        )
      );

    // 3. Get PN data for this farm on this date
    const pnData = await fetchPNData(date);
    const farmPNData = pnData.filter(pn => 
      pn.nationalGridBmUnit === farmId || pn.bmUnit === `T_${farmId}`
    );

    if (farmPNData.length === 0) {
      throw new Error(`No Physical Notification data found for ${farmId} on ${date}`);
    }

    // 4. Calculate curtailment percentage for each period
    const periodDetails = new Map<number, {
      potentialGeneration: number;
      curtailedVolume: number;
      curtailmentPercentage: number;
    }>();

    // Process PN data first (potential generation)
    for (const pn of farmPNData) {
      const period = pn.settlementPeriod;
      const potentialGeneration = (pn.levelFrom + pn.levelTo) / 2 * 0.5; // Average level * 0.5 hour = MWh
      
      if (!periodDetails.has(period)) {
        periodDetails.set(period, {
          potentialGeneration,
          curtailedVolume: 0,
          curtailmentPercentage: 0
        });
      } else {
        // If multiple PN records exist for the same period, use the higher value
        const current = periodDetails.get(period)!;
        if (potentialGeneration > current.potentialGeneration) {
          current.potentialGeneration = potentialGeneration;
          periodDetails.set(period, current);
        }
      }
    }

    // Then process curtailment data
    for (const record of curtailmentData) {
      const period = record.settlementPeriod;
      const curtailedVolume = Math.abs(parseFloat(record.volume.toString()));
      
      if (!periodDetails.has(period)) {
        // If we have curtailment but no PN data for this period, create an entry
        periodDetails.set(period, {
          potentialGeneration: curtailedVolume, // Assume at minimum the potential was what was curtailed
          curtailedVolume,
          curtailmentPercentage: 100 // 100% curtailed if that's all we know
        });
      } else {
        // Update existing period data
        const current = periodDetails.get(period)!;
        current.curtailedVolume = curtailedVolume;
        
        // Calculate percentage - but handle the case where potentialGeneration might be zero
        if (current.potentialGeneration > 0) {
          current.curtailmentPercentage = (curtailedVolume / current.potentialGeneration) * 100;
          
          // Cap at 100% - sometimes PN data might be lower than curtailment volume
          if (current.curtailmentPercentage > 100) {
            current.curtailmentPercentage = 100;
            // Adjust potential generation to match curtailment at minimum
            current.potentialGeneration = curtailedVolume;
          }
        } else {
          current.curtailmentPercentage = 0;
        }
        
        periodDetails.set(period, current);
      }
    }

    // 5. Calculate overall totals
    let totalPotentialGeneration = 0;
    let totalCurtailedVolume = 0;
    
    // Convert the map to a sorted array for the response
    const detailedPeriods = Array.from(periodDetails.entries())
      .map(([period, data]) => {
        totalPotentialGeneration += data.potentialGeneration;
        totalCurtailedVolume += data.curtailedVolume;
        
        return {
          period,
          potentialGeneration: Number(data.potentialGeneration.toFixed(2)),
          curtailedVolume: Number(data.curtailedVolume.toFixed(2)),
          curtailmentPercentage: Number(data.curtailmentPercentage.toFixed(2))
        };
      })
      .sort((a, b) => a.period - b.period);

    // Calculate overall curtailment percentage
    const curtailmentPercentage = totalPotentialGeneration > 0
      ? (totalCurtailedVolume / totalPotentialGeneration) * 100
      : 0;

    return {
      farmId,
      date,
      leadPartyName: bmuInfo.leadPartyName || 'Unknown',
      totalPotentialGeneration: Number(totalPotentialGeneration.toFixed(2)),
      totalCurtailedVolume: Number(totalCurtailedVolume.toFixed(2)),
      curtailmentPercentage: Number(curtailmentPercentage.toFixed(2)),
      detailedPeriods
    };
  } catch (error) {
    logger.error('Error calculating farm curtailment percentage', {
      module: 'pnDataService',
      farmId,
      date,
      error: error instanceof Error ? error.message : String(error)
    });
    throw new Error(error instanceof Error ? error.message : String(error));
  }
}

/**
 * Calculate curtailment percentages for all farms belonging to a lead party
 * 
 * @param leadPartyName - The lead party name
 * @param date - Settlement date in YYYY-MM-DD format
 */
export async function calculateLeadPartyCurtailmentPercentage(leadPartyName: string, date: string): Promise<{
  leadPartyName: string;
  date: string;
  farms: Array<{
    farmId: string;
    totalPotentialGeneration: number;
    totalCurtailedVolume: number;
    curtailmentPercentage: number;
  }>;
  totalPotentialGeneration: number;
  totalCurtailedVolume: number;
  overallCurtailmentPercentage: number;
}> {
  try {
    // 1. Get all farms for this lead party with curtailment records on this date
    const farmsQuery = await db
      .select({
        farmId: curtailmentRecords.farmId,
      })
      .from(curtailmentRecords)
      .where(
        and(
          eq(curtailmentRecords.settlementDate, date),
          eq(curtailmentRecords.leadPartyName, leadPartyName)
        )
      )
      .groupBy(curtailmentRecords.farmId);

    if (farmsQuery.length === 0) {
      throw new Error(`No farms found for ${leadPartyName} on ${date}`);
    }

    // 2. Calculate curtailment percentage for each farm
    const farmData = [];
    let totalPotentialGeneration = 0;
    let totalCurtailedVolume = 0;

    for (const { farmId } of farmsQuery) {
      try {
        const farmStats = await calculateFarmCurtailmentPercentage(farmId, date);
        
        farmData.push({
          farmId,
          totalPotentialGeneration: farmStats.totalPotentialGeneration,
          totalCurtailedVolume: farmStats.totalCurtailedVolume,
          curtailmentPercentage: farmStats.curtailmentPercentage
        });
        
        totalPotentialGeneration += farmStats.totalPotentialGeneration;
        totalCurtailedVolume += farmStats.totalCurtailedVolume;
      } catch (error) {
        logger.warning(`Error calculating curtailment for farm ${farmId}, skipping in lead party summary`, {
          module: 'pnDataService',
          error: error instanceof Error ? error.message : String(error)
        });
        // Continue with other farms
      }
    }

    // Calculate overall curtailment percentage for this lead party
    const overallCurtailmentPercentage = totalPotentialGeneration > 0
      ? (totalCurtailedVolume / totalPotentialGeneration) * 100
      : 0;

    return {
      leadPartyName,
      date,
      farms: farmData,
      totalPotentialGeneration: Number(totalPotentialGeneration.toFixed(2)),
      totalCurtailedVolume: Number(totalCurtailedVolume.toFixed(2)),
      overallCurtailmentPercentage: Number(overallCurtailmentPercentage.toFixed(2))
    };
  } catch (error) {
    logger.error('Error calculating lead party curtailment percentage', {
      module: 'pnDataService',
      leadPartyName,
      date,
      error: error instanceof Error ? error.message : String(error)
    });
    throw new Error(error instanceof Error ? error.message : String(error));
  }
}

/**
 * Get PN (Physical Notification) data for a specific settlement period
 * 
 * @param date - The settlement date in YYYY-MM-DD format
 * @param period - The settlement period (1-48)
 * @param farmId - Optional farm ID to filter results
 */
export async function getPNDataForPeriod(date: string, period: number, farmId?: string): Promise<PhysicalNotification[]> {
  try {
    const pnData = await fetchPNData(date, period);
    
    if (farmId) {
      return pnData.filter(pn => 
        pn.nationalGridBmUnit === farmId || pn.bmUnit === `T_${farmId}`
      );
    }
    
    return pnData;
  } catch (error) {
    logger.error('Error getting PN data for period', {
      module: 'pnDataService',
      date,
      period,
      farmId,
      error: error instanceof Error ? error.message : String(error)
    });
    throw new Error(error instanceof Error ? error.message : String(error));
  }
}