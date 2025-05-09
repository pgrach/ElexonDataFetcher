/**
 * Wind Farm Data Models
 * 
 * This file contains TypeScript interfaces and types for wind farm data.
 */

import fs from 'fs/promises';
import path from 'path';

// Path to BMU mapping data
const BMU_MAPPING_PATH = path.join(process.cwd(), "server", "data", "bmuMapping.json");

// Cache for loaded data
let bmuMappingCache: WindFarmBmu[] | null = null;

/**
 * Wind Farm BMU structure from Elexon
 */
export interface WindFarmBmu {
  /** Elexon BMU ID */
  elexonBmUnit: string;
  
  /** BMU short name */
  bmUnitName: string;
  
  /** BMU long name */
  bmUnitLongName: string;
  
  /** Lead party name */
  leadPartyName: string;
  
  /** Fuel type (e.g., "WIND") */
  fuelType: string;
  
  /** Registration date */
  registrationDate: string;
  
  /** Additional metadata */
  metadata?: Record<string, any>;
}

/**
 * Load all wind farm BMUs from the mapping file
 * 
 * @returns Promise resolving to array of WindFarmBmu objects
 */
export async function loadWindFarmBmus(): Promise<WindFarmBmu[]> {
  try {
    if (!bmuMappingCache) {
      console.log('Loading BMU mapping from:', BMU_MAPPING_PATH);
      const mappingContent = await fs.readFile(BMU_MAPPING_PATH, 'utf8');
      bmuMappingCache = JSON.parse(mappingContent);
      console.log(`Loaded ${bmuMappingCache.length} BMU mappings`);
    }
    
    return bmuMappingCache;
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    console.error('Error loading BMU mapping:', errorMessage);
    throw error;
  }
}

/**
 * Get wind farm BMUs only
 * 
 * @returns Promise resolving to array of wind farm BMUs
 */
export async function getWindFarmBmus(): Promise<WindFarmBmu[]> {
  const allBmus = await loadWindFarmBmus();
  return allBmus.filter(bmu => bmu.fuelType === "WIND");
}

/**
 * Get a set of wind farm BMU IDs
 * 
 * @returns Promise resolving to a Set of wind farm BMU IDs
 */
export async function getWindFarmBmuIds(): Promise<Set<string>> {
  const windFarms = await getWindFarmBmus();
  return new Set(windFarms.map(wf => wf.elexonBmUnit));
}

/**
 * Get a mapping of BMU IDs to lead party names
 * 
 * @returns Promise resolving to a Map of BMU IDs to lead party names
 */
export async function getLeadPartyMap(): Promise<Map<string, string>> {
  const windFarms = await getWindFarmBmus();
  return new Map(
    windFarms.map(wf => [wf.elexonBmUnit, wf.leadPartyName || 'Unknown'])
  );
}

/**
 * Clear the BMU mapping cache
 * Use this when you need to force a reload of the mapping data
 */
export function clearBmuCache(): void {
  bmuMappingCache = null;
}