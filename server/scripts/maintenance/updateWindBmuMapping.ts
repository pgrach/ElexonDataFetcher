/**
 * Wind BMU Mapping Update Script
 * 
 * This script fetches data from the Elexon API to create a mapping of all Wind
 * Balancing Mechanism Units (BMUs) in the system.
 * 
 * The resulting JSON file is saved to server/data/windBmuMapping.json
 */

import axios from 'axios';
import fs from 'fs/promises';
import path from 'path';
import { z } from 'zod';

const ELEXON_API_URL = 'https://data.elexon.co.uk/bmrs/api/v1/reference/bmunits/all';
const WIND_BMU_MAPPING_PATH = path.join(process.cwd(), 'server', 'data', 'windBmuMapping.json');

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

async function updateWindBmuMapping() {
  try {
    console.log('Starting Wind BMU mapping update process...');

    // Create data directory if it doesn't exist
    await fs.mkdir(path.dirname(WIND_BMU_MAPPING_PATH), { recursive: true });

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

    // Filter Wind units
    const windBmus = bmuData.filter(bmu => 
      bmu?.fuelType?.toUpperCase() === 'WIND'
    );
    
    console.log(`Found ${windBmus.length} Wind BMUs before validation`);

    // Validate and process Wind units
    const validWindBmus = (await Promise.all(
      windBmus
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
              fuelType: 'WIND',
              leadPartyName: bmu.leadPartyName || 'Unknown'
            };
          } catch (error) {
            console.warn('Wind BMU validation failed:', error);
            return null;
          }
        })
    )).filter(bmu => bmu !== null);

    // Sort by BMU ID for consistency
    const sortedWindBmus = validWindBmus.sort((a, b) => 
      a!.elexonBmUnit.localeCompare(b!.elexonBmUnit)
    );

    console.log(`Found ${sortedWindBmus.length} valid Wind BMUs`);

    // Write the filtered data to windBmuMapping.json
    await fs.writeFile(
      WIND_BMU_MAPPING_PATH,
      JSON.stringify(sortedWindBmus, null, 2),
      'utf8'
    );

    console.log(`Successfully updated Wind BMU mapping at ${WIND_BMU_MAPPING_PATH}`);
    console.log(`Total Wind BMUs: ${sortedWindBmus.length}`);

  } catch (error) {
    console.error('Error updating Wind BMU mapping:', error);
    process.exit(1);
  }
}

// Execute the update function
updateWindBmuMapping();