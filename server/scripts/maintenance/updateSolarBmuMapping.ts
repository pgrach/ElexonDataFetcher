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

// Define validation schema for API response data
const bmuResponseSchema = z.object({
  nationalGridBmUnit: z.string(),
  elexonBmUnit: z.string(),
  bmUnitName: z.string(),
  generationCapacity: z.string(),
  fuelType: z.string().nullable(),
  leadPartyName: z.string().nullable()
}).partial();

// Define validation schema for Solar BMU data
const solarBmuSchema = z.object({
  nationalGridBmUnit: z.string(),
  elexonBmUnit: z.string(),
  bmUnitName: z.string(),
  generationCapacity: z.string().refine(val => !isNaN(parseFloat(val)), {
    message: "Generation capacity must be a valid number string"
  }),
  fuelType: z.literal('SOLAR'),
  leadPartyName: z.string().min(1, "Lead party name is required")
});

async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function fetchBmuData(attempt = 1): Promise<any[]> {
  try {
    console.log(`Attempt ${attempt}/${MAX_RETRIES} to fetch BMU data...`);

    const response = await axios.get(ELEXON_API_URL, {
      timeout: REQUEST_TIMEOUT,
      headers: {
        'Accept': 'application/json'
      }
    });

    if (!response.data || !Array.isArray(response.data)) {
      throw new Error('Invalid response format from Elexon API');
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

    // Check if any solar units are available
    const solarTypeCheck = bmuData.filter(bmu => bmu?.fuelType?.toUpperCase().includes('SOLAR') || 
                                            bmu?.fuelType?.toUpperCase().includes('PV'));
    
    console.log(`Found ${solarTypeCheck.length} potential solar BMUs before validation`);

    // Validate and filter solar units
    const solarBmus = (await Promise.all(
      bmuData
        .filter(bmu => {
          const fuelType = bmu?.fuelType?.toUpperCase() || '';
          return fuelType.includes('SOLAR') || fuelType.includes('PV');
        })
        .map(async bmu => {
          try {
            return solarBmuSchema.parse({
              nationalGridBmUnit: bmu.nationalGridBmUnit,
              elexonBmUnit: bmu.elexonBmUnit,
              bmUnitName: bmu.bmUnitName,
              generationCapacity: bmu.generationCapacity,
              fuelType: 'SOLAR', // Standardize on 'SOLAR' regardless of original 'SOLAR' or 'PV'
              leadPartyName: bmu.leadPartyName || 'Unknown'
            });
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