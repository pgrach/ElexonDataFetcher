import axios from 'axios';
import fs from 'fs/promises';
import path from 'path';
import { z } from 'zod';

const ELEXON_API_URL = 'https://data.elexon.co.uk/bmrs/api/v1/reference/bmunits/all';
// This is the central BMU mapping file used by all services
const BMU_MAPPING_PATH = path.join(process.cwd(), 'server', 'data', 'bmuMapping.json');

const MAX_RETRIES = 3;
const RETRY_DELAY = 1000; // 1 second
const REQUEST_TIMEOUT = 10000; // 10 seconds

// Define validation schema for BMU data
const bmuSchema = z.object({
  nationalGridBmUnit: z.string(),
  elexonBmUnit: z.string(),
  bmUnitName: z.string(),
  generationCapacity: z.string().refine(val => !isNaN(parseFloat(val)), {
    message: "Generation capacity must be a valid number string"
  }),
  fuelType: z.literal('WIND'),
  leadPartyName: z.string().min(1, "Lead party name is required")
});

const bmuResponseSchema = z.object({
  nationalGridBmUnit: z.string(),
  elexonBmUnit: z.string(),
  bmUnitName: z.string(),
  generationCapacity: z.string(),
  fuelType: z.string().nullable(),
  leadPartyName: z.string().nullable()
}).partial();

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

async function updateBmuMapping() {
  try {
    console.log('Starting BMU mapping update process...');

    // Create data directory if it doesn't exist
    await fs.mkdir(path.dirname(BMU_MAPPING_PATH), { recursive: true });

    // Fetch data with retry logic
    const bmuData = await fetchBmuData();

    // Validate and filter wind units
    const windBmus = (await Promise.all(
      bmuData
        .filter(bmu => bmu?.fuelType?.toUpperCase() === 'WIND')
        .map(async bmu => {
          try {
            return bmuSchema.parse({
              nationalGridBmUnit: bmu.nationalGridBmUnit,
              elexonBmUnit: bmu.elexonBmUnit,
              bmUnitName: bmu.bmUnitName,
              generationCapacity: bmu.generationCapacity,
              fuelType: 'WIND',
              leadPartyName: bmu.leadPartyName || 'Unknown'
            });
          } catch (error) {
            console.warn('BMU validation failed:', error);
            return null;
          }
        })
    )).filter(bmu => bmu !== null);

    // Sort by BMU ID for consistency
    const sortedWindBmus = windBmus.sort((a, b) => 
      a!.elexonBmUnit.localeCompare(b!.elexonBmUnit)
    );

    console.log(`Found ${sortedWindBmus.length} valid wind BMUs`);

    // Write the filtered data to bmuMapping.json
    await fs.writeFile(
      BMU_MAPPING_PATH,
      JSON.stringify(sortedWindBmus, null, 2),
      'utf8'
    );

    console.log(`Successfully updated BMU mapping at ${BMU_MAPPING_PATH}`);
    console.log(`Total wind BMUs: ${sortedWindBmus.length}`);

  } catch (error) {
    console.error('Error updating BMU mapping:', error);
    process.exit(1);
  }
}

// Execute the update function
updateBmuMapping();
