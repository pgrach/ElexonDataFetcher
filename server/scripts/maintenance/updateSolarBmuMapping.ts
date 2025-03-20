/**
 * Solar/PV BMU Mapping Update Script
 * 
 * This script fetches data from the Elexon API to create a mapping of all Solar/PV
 * Balancing Mechanism Units (BMUs) in the system.
 * 
 * The resulting JSON file is saved to server/data/solarBmuMapping.json
 */

import axios from 'axios';
import fs from 'fs/promises';
import path from 'path';
import { z } from 'zod';

const ELEXON_API_URL = 'https://data.elexon.co.uk/bmrs/api/v1/reference/bmunits/all';
const SOLAR_BMU_MAPPING_PATH = path.join(process.cwd(), 'server', 'data', 'solarBmuMapping.json');

const MAX_RETRIES = 3;
const RETRY_DELAY = 1000; // 1 second
const REQUEST_TIMEOUT = 10000; // 10 seconds

// Define validation schema for API response data with nullable fields
const bmuResponseSchema = z.object({
  nationalGridBmUnit: z.string().nullable().optional(),
  elexonBmUnit: z.string().nullable().optional(),
  bmUnitName: z.string().nullable().optional(),
  generationCapacity: z.string().nullable().optional(),
  fuelType: z.string().nullable().optional(),
  leadPartyName: z.string().nullable().optional()
});

// We're using a more flexible approach for Solar BMU mapping
// instead of strict schema validation

async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function fetchBmuData(attempt = 1): Promise<any[]> {
  try {
    console.log(`Attempt ${attempt}/${MAX_RETRIES} to fetch BMU data...`);

    console.log('Making API request to:', ELEXON_API_URL);
    
    const response = await axios.get(ELEXON_API_URL, {
      timeout: REQUEST_TIMEOUT,
      headers: {
        'Accept': 'application/json'
      }
    });

    console.log('API response received, status:', response.status);
    if (!response.data) {
      throw new Error('No data returned from Elexon API');
    }
    
    // Check response structure
    console.log('Response data type:', typeof response.data);
    if (Array.isArray(response.data)) {
      console.log(`Response contains array of ${response.data.length} items`);
      // Log sample of first item
      if (response.data.length > 0) {
        console.log('Sample first item:', JSON.stringify(response.data[0]).substring(0, 500) + '...');
      }
    } else {
      console.log('Response structure:', Object.keys(response.data));
      throw new Error('Invalid response format from Elexon API - not an array');
    }

    // Validate API response data
    const validatedData = response.data.map((item, index) => {
      try {
        return bmuResponseSchema.parse(item);
      } catch (error) {
        console.warn(`Invalid BMU data at index ${index}:`, error);
        return null;
      }
    }).filter(item => item !== null);

    return validatedData;
  } catch (error) {
    if (attempt < MAX_RETRIES) {
      console.warn(`Attempt ${attempt} failed:`, error);
      const backoffDelay = RETRY_DELAY * Math.pow(2, attempt - 1);
      console.log(`Retrying in ${backoffDelay}ms...`);
      await delay(backoffDelay);
      return fetchBmuData(attempt + 1);
    }
    throw new Error(`Failed to fetch BMU data after ${MAX_RETRIES} attempts: ${error}`);
  }
}

async function updateSolarBmuMapping() {
  try {
    console.log('Starting Solar BMU mapping update process...');

    // Create data directory if it doesn't exist
    await fs.mkdir(path.dirname(SOLAR_BMU_MAPPING_PATH), { recursive: true });

    // Fetch data with retry logic
    const bmuData = await fetchBmuData();

    // Log all unique fuel types to better understand the data
    const fuelTypes = new Set<string>();
    bmuData.forEach(bmu => {
      if (bmu?.fuelType) {
        fuelTypes.add(bmu.fuelType.toUpperCase());
      }
    });
    console.log('Available fuel types in the API response:', Array.from(fuelTypes));

    // Since there's no explicit 'SOLAR' or 'PV' fuel type, try to identify solar units by their names
    // Common keywords that might indicate solar plants
    const solarKeywords = ['solar', 'pv', 'photovoltaic', 'sun', 'solar farm', 'solar park'];
    
    // Keywords to exclude (not solar units)
    const exclusionKeywords = ['battery', 'batteries', 'storage', 'bess', 'energy storage'];
    
    const potentialSolarBmus = bmuData.filter(bmu => {
      // Look for solar keywords in BMU name or ID
      const bmUnitName = (bmu?.bmUnitName || '').toLowerCase();
      const elexonBmUnit = (bmu?.elexonBmUnit || '').toLowerCase();
      const nationalGridBmUnit = (bmu?.nationalGridBmUnit || '').toLowerCase();
      
      // First check if any solar keywords match
      const hasSolarKeyword = solarKeywords.some(keyword => 
        bmUnitName.includes(keyword) || 
        elexonBmUnit.includes(keyword) || 
        nationalGridBmUnit.includes(keyword)
      );
      
      // Then check for exclusion keywords
      const hasExclusionKeyword = exclusionKeywords.some(keyword => 
        bmUnitName.includes(keyword) || 
        elexonBmUnit.includes(keyword) || 
        nationalGridBmUnit.includes(keyword)
      );
      
      // Include if it has a solar keyword and doesn't have an exclusion keyword
      return hasSolarKeyword && !hasExclusionKeyword;
    });
    
    console.log(`Found ${potentialSolarBmus.length} potential solar BMUs by name keywords before validation`);

    // Additionally, check OTHER fuel types that might be solar (but exclude storage systems)
    const otherFuelTypeBmus = bmuData.filter(bmu => {
      if (bmu?.fuelType !== 'OTHER') return false;
      
      // Exclude battery storage systems
      const bmUnitName = (bmu?.bmUnitName || '').toLowerCase();
      const elexonBmUnit = (bmu?.elexonBmUnit || '').toLowerCase();
      const nationalGridBmUnit = (bmu?.nationalGridBmUnit || '').toLowerCase();
      
      const hasExclusionKeyword = exclusionKeywords.some(keyword => 
        bmUnitName.includes(keyword) || 
        elexonBmUnit.includes(keyword) || 
        nationalGridBmUnit.includes(keyword)
      );
      
      return !hasExclusionKeyword;
    });
    
    console.log(`Found ${otherFuelTypeBmus.length} BMUs with 'OTHER' fuel type (excluding storage)`);
    
    // Log some samples of filtered 'OTHER' fuel type to see if we can identify patterns
    if (otherFuelTypeBmus.length > 0) {
      console.log('Sample OTHER fuel type BMUs (excluding storage):');
      otherFuelTypeBmus.slice(0, 5).forEach((bmu, index) => {
        console.log(`  ${index + 1}. ${bmu.bmUnitName || 'unnamed'} (${bmu.elexonBmUnit || 'no ID'})`);
      });
    }

    // Combine both methods to get potential solar BMUs
    const combinedPotentialSolar = [...potentialSolarBmus];
    
    // Add filtered OTHER fuel types that might be solar (aren't already included)
    otherFuelTypeBmus.forEach(bmu => {
      const isAlreadyIncluded = combinedPotentialSolar.some(
        solarBmu => solarBmu.elexonBmUnit === bmu.elexonBmUnit
      );
      
      if (!isAlreadyIncluded) {
        combinedPotentialSolar.push(bmu);
      }
    });
    
    console.log(`Combined total of ${combinedPotentialSolar.length} potential solar BMUs`);

    // Validate and filter solar units (using the combined approach)
    const solarBmus = (await Promise.all(
      combinedPotentialSolar
        .filter(bmu => {
          // Ensure the BMU has required fields
          return bmu.nationalGridBmUnit && bmu.elexonBmUnit && bmu.bmUnitName && bmu.generationCapacity;
        })
        .map(async bmu => {
          try {
            // Make sure all required fields exist
            if (!bmu.nationalGridBmUnit || !bmu.elexonBmUnit || !bmu.bmUnitName || !bmu.generationCapacity) {
              console.warn('Skipping BMU with missing required fields', bmu.elexonBmUnit || 'unknown');
              return null;
            }
            
            return {
              nationalGridBmUnit: bmu.nationalGridBmUnit,
              elexonBmUnit: bmu.elexonBmUnit,
              bmUnitName: bmu.bmUnitName,
              generationCapacity: bmu.generationCapacity,
              fuelType: 'SOLAR', // Standardize on 'SOLAR' regardless of original 'SOLAR' or 'PV'
              leadPartyName: bmu.leadPartyName || 'Unknown'
            };
          } catch (error) {
            console.warn('Solar BMU validation failed:', error);
            return null;
          }
        })
    )).filter(bmu => bmu !== null);

    // Sort by BMU ID for consistency
    const sortedSolarBmus = solarBmus.sort((a, b) => 
      a!.elexonBmUnit.localeCompare(b!.elexonBmUnit)
    );

    console.log(`Found ${sortedSolarBmus.length} valid solar BMUs`);

    // Write the filtered data to solarBmuMapping.json
    await fs.writeFile(
      SOLAR_BMU_MAPPING_PATH,
      JSON.stringify(sortedSolarBmus, null, 2),
      'utf8'
    );

    console.log(`Successfully updated Solar BMU mapping at ${SOLAR_BMU_MAPPING_PATH}`);
    console.log(`Total solar BMUs: ${sortedSolarBmus.length}`);

  } catch (error) {
    console.error('Error updating Solar BMU mapping:', error);
    process.exit(1);
  }
}

// Execute the update function
updateSolarBmuMapping();