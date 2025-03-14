/**
 * Curtailment Analytics Service
 * 
 * Provides advanced analytics for wind farm curtailment data,
 * including curtailment percentages and efficiency metrics.
 */

import { db } from '@db/index';
import { curtailmentRecords } from '@db/schema';
import { eq, sql, and, between, gte, lte } from 'drizzle-orm';
import { format, parse, isValid, addDays } from 'date-fns';
import { readFile } from 'fs/promises';
import path from 'path';

// Path to BMU mapping file
const BMU_MAPPING_PATH = path.join(process.cwd(), 'server', 'data', 'bmuMapping.json');

// Cache for BMU mappings to avoid repeated file reads
let bmuMappingCache: any[] | null = null;

/**
 * Load BMU mappings from JSON file
 */
async function loadBmuMappings(): Promise<any[]> {
  if (bmuMappingCache !== null) {
    return bmuMappingCache;
  }

  try {
    const data = await readFile(BMU_MAPPING_PATH, 'utf8');
    bmuMappingCache = JSON.parse(data);
    return bmuMappingCache;
  } catch (error) {
    console.error('Error loading BMU mapping:', error);
    throw new Error('Failed to load BMU data');
  }
}

/**
 * Get generation capacity for a specific farm ID
 */
async function getFarmCapacity(farmId: string): Promise<number> {
  const bmuMappings = await loadBmuMappings();
  const bmu = bmuMappings.find(b => b.elexonBmUnit === farmId);
  
  if (!bmu || !bmu.generationCapacity) {
    return 0;
  }
  
  return parseFloat(bmu.generationCapacity);
}

/**
 * Get all farms for a lead party
 */
async function getFarmsForLeadParty(leadPartyName: string): Promise<{ farmId: string, capacity: number }[]> {
  const bmuMappings = await loadBmuMappings();
  return bmuMappings
    .filter(b => b.leadPartyName === leadPartyName && b.fuelType === 'WIND')
    .map(b => ({
      farmId: b.elexonBmUnit,
      capacity: parseFloat(b.generationCapacity || '0')
    }));
}

/**
 * Calculate total potential generation for a farm over a time period
 * 
 * Formula: capacity (MW) * hours = MWh
 * For a day: capacity * 24 hours
 * For a settlement period: capacity * 0.5 hours (each period is 30 minutes)
 */
function calculatePotentialGeneration(capacity: number, timeframe: 'day' | 'month' | 'year', periodCount?: number): number {
  if (capacity <= 0) return 0;
  
  let hours = 0;
  
  if (periodCount) {
    // Each settlement period is 30 minutes (0.5 hours)
    hours = periodCount * 0.5;
  } else {
    switch (timeframe) {
      case 'day':
        hours = 24;
        break;
      case 'month':
        // Average month length (365.25/12 days)
        hours = 24 * 30.4375;
        break;
      case 'year':
        // Account for leap years (365.25 days)
        hours = 24 * 365.25;
        break;
    }
  }
  
  return capacity * hours;
}

/**
 * Get curtailment percentage for a specific farm
 */
export async function getFarmCurtailmentPercentage(
  farmId: string, 
  timeframe: 'day' | 'month' | 'year', 
  value: string
): Promise<{
  farmId: string;
  capacity: number;
  totalPotentialGeneration: number;
  curtailedEnergy: number;
  curtailmentPercentage: number;
  periodCount: number;
  timeframe: string;
  value: string;
}> {
  // Get farm capacity
  const capacity = await getFarmCapacity(farmId);
  
  // Set date conditions based on timeframe
  let dateCondition;
  let startDate: string, endDate: string;
  
  if (timeframe === 'day') {
    dateCondition = eq(curtailmentRecords.settlementDate, value);
    startDate = value;
    endDate = value;
  } else if (timeframe === 'month') {
    // For a specific month (YYYY-MM)
    const [year, month] = value.split('-').map(n => parseInt(n, 10));
    const parsedStartDate = new Date(year, month - 1, 1);
    const parsedEndDate = new Date(year, month, 0); // Last day of month
    
    startDate = format(parsedStartDate, 'yyyy-MM-dd');
    endDate = format(parsedEndDate, 'yyyy-MM-dd');
    
    dateCondition = sql`${curtailmentRecords.settlementDate} BETWEEN ${startDate} AND ${endDate}`;
  } else if (timeframe === 'year') {
    // For a specific year
    startDate = `${value}-01-01`;
    endDate = `${value}-12-31`;
    
    dateCondition = sql`EXTRACT(YEAR FROM ${curtailmentRecords.settlementDate}) = ${parseInt(value, 10)}`;
  } else {
    throw new Error(`Invalid timeframe: ${timeframe}`);
  }
  
  // Get curtailment data for this farm
  const curtailmentData = await db
    .select({
      curtailedEnergy: sql<number>`SUM(ABS(volume))`,
      periodCount: sql<number>`COUNT(DISTINCT settlement_date || '-' || settlement_period)` // Unique periods
    })
    .from(curtailmentRecords)
    .where(
      and(
        dateCondition,
        eq(curtailmentRecords.farmId, farmId)
      )
    );
  
  const curtailedEnergy = Number(curtailmentData[0]?.curtailedEnergy || 0);
  const periodCount = Number(curtailmentData[0]?.periodCount || 0);
  
  // Calculate potential generation based on capacity and period count
  const totalPotentialGeneration = calculatePotentialGeneration(capacity, timeframe, periodCount);
  
  // Calculate curtailment percentage
  const curtailmentPercentage = totalPotentialGeneration > 0 
    ? (curtailedEnergy / totalPotentialGeneration) * 100 
    : 0;
  
  return {
    farmId,
    capacity,
    totalPotentialGeneration,
    curtailedEnergy,
    curtailmentPercentage,
    periodCount,
    timeframe,
    value
  };
}

/**
 * Get curtailment percentage for a lead party (all farms)
 */
export async function getLeadPartyCurtailmentPercentage(
  leadPartyName: string,
  timeframe: 'day' | 'month' | 'year',
  value: string
): Promise<{
  leadPartyName: string;
  farms: Array<{
    farmId: string;
    capacity: number;
    curtailedEnergy: number;
    curtailmentPercentage: number;
  }>;
  totalCapacity: number;
  totalPotentialGeneration: number;
  totalCurtailedEnergy: number;
  overallCurtailmentPercentage: number;
  periodCount: number;
  timeframe: string;
  value: string;
}> {
  // Get all farms for this lead party
  const farms = await getFarmsForLeadParty(leadPartyName);
  
  // Set date conditions based on timeframe
  let dateCondition;
  let startDate: string, endDate: string;
  
  if (timeframe === 'day') {
    dateCondition = eq(curtailmentRecords.settlementDate, value);
    startDate = value;
    endDate = value;
  } else if (timeframe === 'month') {
    // For a specific month (YYYY-MM)
    const [year, month] = value.split('-').map(n => parseInt(n, 10));
    const parsedStartDate = new Date(year, month - 1, 1);
    const parsedEndDate = new Date(year, month, 0); // Last day of month
    
    startDate = format(parsedStartDate, 'yyyy-MM-dd');
    endDate = format(parsedEndDate, 'yyyy-MM-dd');
    
    dateCondition = sql`${curtailmentRecords.settlementDate} BETWEEN ${startDate} AND ${endDate}`;
  } else if (timeframe === 'year') {
    // For a specific year
    startDate = `${value}-01-01`;
    endDate = `${value}-12-31`;
    
    dateCondition = sql`EXTRACT(YEAR FROM ${curtailmentRecords.settlementDate}) = ${parseInt(value, 10)}`;
  } else {
    throw new Error(`Invalid timeframe: ${timeframe}`);
  }
  
  // Get period count for the timeframe
  const periodData = await db
    .select({
      periodCount: sql<number>`COUNT(DISTINCT settlement_date || '-' || settlement_period)` // Unique periods
    })
    .from(curtailmentRecords)
    .where(
      and(
        dateCondition,
        eq(curtailmentRecords.leadPartyName, leadPartyName)
      )
    );
  
  const periodCount = Number(periodData[0]?.periodCount || 0);
  
  // Get curtailment data for each farm
  const farmResults = await Promise.all(farms.map(async (farm) => {
    const curtailmentData = await db
      .select({
        curtailedEnergy: sql<number>`SUM(ABS(volume))`
      })
      .from(curtailmentRecords)
      .where(
        and(
          dateCondition,
          eq(curtailmentRecords.farmId, farm.farmId)
        )
      );
    
    const curtailedEnergy = Number(curtailmentData[0]?.curtailedEnergy || 0);
    const potentialGeneration = calculatePotentialGeneration(farm.capacity, timeframe, periodCount);
    const curtailmentPercentage = potentialGeneration > 0 
      ? (curtailedEnergy / potentialGeneration) * 100 
      : 0;
    
    return {
      farmId: farm.farmId,
      capacity: farm.capacity,
      curtailedEnergy,
      curtailmentPercentage
    };
  }));
  
  // Calculate total capacity and overall statistics
  const totalCapacity = farms.reduce((sum, farm) => sum + farm.capacity, 0);
  const totalPotentialGeneration = calculatePotentialGeneration(totalCapacity, timeframe, periodCount);
  const totalCurtailedEnergy = farmResults.reduce((sum, farm) => sum + farm.curtailedEnergy, 0);
  const overallCurtailmentPercentage = totalPotentialGeneration > 0 
    ? (totalCurtailedEnergy / totalPotentialGeneration) * 100 
    : 0;
  
  return {
    leadPartyName,
    farms: farmResults,
    totalCapacity,
    totalPotentialGeneration,
    totalCurtailedEnergy,
    overallCurtailmentPercentage,
    periodCount,
    timeframe,
    value
  };
}

/**
 * Get farms with highest curtailment percentages
 */
export async function getTopCurtailedFarmsByPercentage(
  timeframe: 'day' | 'month' | 'year',
  value: string,
  limit: number = 10
): Promise<Array<{
  farmId: string;
  leadPartyName: string;
  capacity: number;
  curtailedEnergy: number;
  totalPotentialGeneration: number;
  curtailmentPercentage: number;
}>> {
  // Set date conditions based on timeframe
  let dateCondition;
  
  if (timeframe === 'day') {
    dateCondition = eq(curtailmentRecords.settlementDate, value);
  } else if (timeframe === 'month') {
    // For a specific month (YYYY-MM)
    const [year, month] = value.split('-').map(n => parseInt(n, 10));
    const parsedStartDate = new Date(year, month - 1, 1);
    const parsedEndDate = new Date(year, month, 0); // Last day of month
    
    const startDate = format(parsedStartDate, 'yyyy-MM-dd');
    const endDate = format(parsedEndDate, 'yyyy-MM-dd');
    
    dateCondition = sql`${curtailmentRecords.settlementDate} BETWEEN ${startDate} AND ${endDate}`;
  } else if (timeframe === 'year') {
    // For a specific year
    dateCondition = sql`EXTRACT(YEAR FROM ${curtailmentRecords.settlementDate}) = ${parseInt(value, 10)}`;
  } else {
    throw new Error(`Invalid timeframe: ${timeframe}`);
  }
  
  // Get period counts for all farms
  const periodData = await db
    .select({
      farmId: curtailmentRecords.farmId,
      leadPartyName: curtailmentRecords.leadPartyName,
      periodCount: sql<number>`COUNT(DISTINCT settlement_date || '-' || settlement_period)`, // Unique periods
      curtailedEnergy: sql<number>`SUM(ABS(volume))`
    })
    .from(curtailmentRecords)
    .where(dateCondition)
    .groupBy(curtailmentRecords.farmId, curtailmentRecords.leadPartyName);
  
  // Load BMU mappings for capacity data
  const bmuMappings = await loadBmuMappings();
  
  // Calculate curtailment percentages
  const results = await Promise.all(periodData.map(async (data) => {
    const bmu = bmuMappings.find(b => b.elexonBmUnit === data.farmId);
    const capacity = bmu ? parseFloat(bmu.generationCapacity || '0') : 0;
    
    // Calculate potential generation
    const totalPotentialGeneration = calculatePotentialGeneration(capacity, timeframe, Number(data.periodCount));
    
    // Calculate curtailment percentage
    const curtailmentPercentage = totalPotentialGeneration > 0 
      ? (Number(data.curtailedEnergy) / totalPotentialGeneration) * 100 
      : 0;
    
    return {
      farmId: data.farmId,
      leadPartyName: data.leadPartyName || 'Unknown',
      capacity,
      curtailedEnergy: Number(data.curtailedEnergy),
      totalPotentialGeneration,
      curtailmentPercentage
    };
  }));
  
  // Sort by curtailment percentage (highest first) and limit results
  return results
    .filter(r => r.capacity > 0) // Filter out farms with unknown/zero capacity
    .sort((a, b) => b.curtailmentPercentage - a.curtailmentPercentage)
    .slice(0, limit);
}