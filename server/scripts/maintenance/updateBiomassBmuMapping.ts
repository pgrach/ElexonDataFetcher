/**
 * Biomass BMU Mapping Update Script
 * 
 * This script fetches data from the Elexon API to create a mapping of all Biomass
 * Balancing Mechanism Units (BMUs) in the system.
 * 
 * The resulting JSON file is saved to server/data/biomassBmuMapping.json
 */

import axios from 'axios';
import fs from 'fs/promises';
import path from 'path';
import { z } from 'zod';

const ELEXON_API_URL = 'https://data.elexon.co.uk/bmrs/api/v1/reference/bmunits/all';
const BIOMASS_BMU_MAPPING_PATH = path.join(process.cwd(), 'server', 'data', 'biomassBmuMapping.json');

const MAX_RETRIES = 3;
const RETRY_DELAY = 1000; // 1 second
const REQUEST_TIMEOUT = 10000; // 10 seconds

// Define validation schema for API response data
const bmuResponseSchema = z.object({
  nationalGridBmUnit: z.string().nullable().optional(),
  elexonBmUnit: z.string().nullable().optional(),
  bmUnitName: z.string().nullable().optional(),
  generationCapacity: z.string().nullable().optional(),
  fuelType: z.string().nullable().optional(),
  leadPartyName: z.string().nullable().optional()
});

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
    } else if (response.data && response.data.data && Array.isArray(response.data.data)) {
      console.log(`Response contains array of ${response.data.data.length} items`);
      // Log sample of first item
      if (response.data.data.length > 0) {
        console.log('Sample first item:', JSON.stringify(response.data.data[0]).substring(0, 500) + '...');
      }
      return response.data.data;
    } else {
      console.log('Response structure:', Object.keys(response.data));
      throw new Error('Invalid response format from Elexon API - expected data array');
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

async function updateBiomassBmuMapping() {
  try {
    console.log('Starting Biomass BMU mapping update process...');

    // Create data directory if it doesn't exist
    await fs.mkdir(path.dirname(BIOMASS_BMU_MAPPING_PATH), { recursive: true });

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

    // Filter Biomass units
    const biomassBmus = bmuData.filter(bmu => 
      bmu?.fuelType?.toUpperCase() === 'BIOMASS'
    );
    
    console.log(`Found ${biomassBmus.length} Biomass BMUs before validation`);

    // Validate and process Biomass units
    const validBiomassBmus = (await Promise.all(
      biomassBmus
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
            
            // Extract any region information from the BMU name if possible
            let region = '';
            const name = bmu.bmUnitName.toLowerCase();
            
            // Simple region extraction logic
            if (name.includes('scotland') || name.includes('scottish')) {
              region = 'Scotland';
            } else if (name.includes('wales') || name.includes('welsh')) {
              region = 'Wales';
            } else if (name.includes('england') || name.includes('english')) {
              region = 'England';
            } else if (name.includes('london')) {
              region = 'London';
            } else if (name.includes('north') && name.includes('west')) {
              region = 'North West';
            } else if (name.includes('north') && name.includes('east')) {
              region = 'North East';
            } else if (name.includes('south') && name.includes('west')) {
              region = 'South West';
            } else if (name.includes('south') && name.includes('east')) {
              region = 'South East';
            } else if (name.includes('north')) {
              region = 'North';
            } else if (name.includes('south')) {
              region = 'South';
            } else if (name.includes('east')) {
              region = 'East';
            } else if (name.includes('west')) {
              region = 'West';
            } else if (name.includes('midland')) {
              region = 'Midlands';
            }
            
            // Determine biomass type from name if possible
            let biomassType = 'General Biomass';
            if (name.includes('waste')) {
              biomassType = 'Waste Biomass';
            } else if (name.includes('wood')) {
              biomassType = 'Wood/Forestry Biomass';
            } else if (name.includes('straw')) {
              biomassType = 'Straw/Agricultural Biomass';
            } else if (name.includes('landfill')) {
              biomassType = 'Landfill Gas';
            } else if (name.includes('sewage')) {
              biomassType = 'Sewage Gas';
            } else if (name.includes('biogas')) {
              biomassType = 'Biogas';
            }
            
            return {
              nationalGridBmUnit: bmu.nationalGridBmUnit,
              elexonBmUnit: bmu.elexonBmUnit,
              bmUnitName: bmu.bmUnitName,
              generationCapacity: bmu.generationCapacity,
              fuelType: bmu.fuelType || 'BIOMASS',
              biomassType: biomassType,
              leadPartyName: bmu.leadPartyName || 'Unknown',
              region: region
            };
          } catch (error) {
            console.warn('Biomass BMU validation failed:', error);
            return null;
          }
        })
    )).filter(bmu => bmu !== null);

    // Sort by BMU ID for consistency
    const sortedBiomassBmus = validBiomassBmus.sort((a, b) => 
      a!.elexonBmUnit.localeCompare(b!.elexonBmUnit)
    );

    console.log(`Found ${sortedBiomassBmus.length} valid Biomass BMUs`);

    // Write the filtered data to biomassBmuMapping.json
    await fs.writeFile(
      BIOMASS_BMU_MAPPING_PATH,
      JSON.stringify(sortedBiomassBmus, null, 2),
      'utf8'
    );

    console.log(`Successfully updated Biomass BMU mapping at ${BIOMASS_BMU_MAPPING_PATH}`);
    console.log(`Total Biomass BMUs: ${sortedBiomassBmus.length}`);

  } catch (error) {
    console.error('Error updating Biomass BMU mapping:', error);
    process.exit(1);
  }
}

// Execute the update function
updateBiomassBmuMapping();