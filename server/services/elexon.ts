import { db } from "@db";
import axios from "axios";
import fs from "fs/promises";
import path from "path";
import { ElexonBidOffer, ElexonResponse } from "../types/elexon";

const ELEXON_BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1";
const BMU_MAPPING_PATH = path.join(process.cwd(), 'server', 'data', 'bmuMapping.json');
const BATCH_SIZE = 2; // Process 2 periods at a time
const MAX_RETRIES = 5;
const BASE_DELAY = 2000; // 2 second base delay
const MAX_REQUESTS_PER_MINUTE = 3500; // Keep well under the 5000 limit for safety
const REQUEST_WINDOW = 60000; // 1 minute in milliseconds
const RATE_LIMIT_DELAY = 10000; // 10 seconds delay for rate limit
const BATCH_DELAY = 5000; // 5 seconds between batches

let windFarmBmuMap: Map<string, any> | null = null;
let requestQueue: { timestamp: number }[] = [];

async function loadWindFarmIds(): Promise<Map<string, any>> {
  if (windFarmBmuMap !== null) {
    return windFarmBmuMap;
  }

  try {
    const mappingContent = await fs.readFile(BMU_MAPPING_PATH, 'utf8');
    const bmuMapping = JSON.parse(mappingContent);
    windFarmBmuMap = new Map(
      bmuMapping.map((bmu: any) => [
        bmu.elexonBmUnit,
        {
          name: bmu.bmUnitName,
          capacity: parseFloat(bmu.generationCapacity),
          fuelType: bmu.fuelType
        }
      ])
    );
    console.log(`Loaded ${windFarmBmuMap.size} wind farm IDs from mapping`);
    return windFarmBmuMap;
  } catch (error) {
    console.error('Error loading BMU mapping:', error);
    throw error;
  }
}

function checkRateLimit(): boolean {
  const now = Date.now();
  requestQueue = requestQueue.filter(req => now - req.timestamp < REQUEST_WINDOW);
  const canMakeRequest = requestQueue.length < MAX_REQUESTS_PER_MINUTE;
  if (!canMakeRequest) {
    console.log(`Rate limit reached: ${requestQueue.length} requests in last minute`);
  }
  return canMakeRequest;
}

async function waitForRateLimit(): Promise<void> {
  while (!checkRateLimit()) {
    console.log('Waiting for rate limit window...');
    await delay(2000); // Check every 2 seconds
  }
  requestQueue.push({ timestamp: Date.now() });
}

async function fetchWithRetry(url: string, attempt = 1): Promise<any> {
  try {
    await waitForRateLimit();
    console.log(`Fetching: ${url} (Attempt ${attempt}/${MAX_RETRIES})`);

    const response = await axios.get(url, {
      timeout: 30000, // 30 seconds timeout
      headers: {
        'Accept': 'application/json'
      }
    });

    // Detailed validation of response format
    if (!response.data) {
      console.warn(`Warning: Empty response from ${url}`);
      throw new Error('Empty response from API');
    }

    if (!response.data.data || !Array.isArray(response.data.data)) {
      console.warn(`Warning: Invalid response format from ${url}`);
      console.log('Response:', JSON.stringify(response.data).slice(0, 500) + '...');
      throw new Error('Invalid response format from API');
    }

    // Add delay after successful request to prevent rate limiting
    await delay(BASE_DELAY);

    return response.data;
  } catch (error: any) {
    console.error(`Error fetching ${url} (Attempt ${attempt}/${MAX_RETRIES}):`, 
      error.response?.status || error.message);

    if (error.response?.data) {
      console.error('API Error Response:', JSON.stringify(error.response.data, null, 2));
    }

    // Check for rate limiting
    if (error.response?.status === 429) {
      console.log('Rate limit hit, waiting longer before retry...');
      await delay(RATE_LIMIT_DELAY * Math.pow(2, attempt));
      return fetchWithRetry(url, attempt + 1);
    }

    if (attempt < MAX_RETRIES) {
      const backoffDelay = BASE_DELAY * Math.pow(2, attempt - 1);
      console.log(`Retry ${attempt}/${MAX_RETRIES} for ${url} after ${backoffDelay}ms`);
      await delay(backoffDelay);
      return fetchWithRetry(url, attempt + 1);
    }
    throw error;
  }
}

async function fetchBatchBidsOffers(date: string, periods: number[]): Promise<ElexonBidOffer[]> {
  try {
    const windFarmMap = await loadWindFarmIds();
    console.log(`\nProcessing ${periods.length} periods for ${date}`);

    const requests = periods.flatMap(period => [
      `${ELEXON_BASE_URL}/balancing/settlement/stack/all/bid/${date}/${period}`,
      `${ELEXON_BASE_URL}/balancing/settlement/stack/all/offer/${date}/${period}`
    ]);

    console.log(`Making ${requests.length} API requests for ${date}`);
    const responses = await Promise.all(
      requests.map(url => fetchWithRetry(url))
    );

    let allRecords: ElexonBidOffer[] = [];

    for (let i = 0; i < responses.length; i += 2) {
      const period = periods[Math.floor(i/2)];
      const bids = responses[i]?.data || [];
      const offers = responses[i + 1]?.data || [];

      console.log(`[${date} P${period}] Raw data - Bids: ${bids.length}, Offers: ${offers.length}`);

      // Process bids and offers with improved validation
      const validRecords = [...bids, ...offers].filter(record => {
        const windFarmInfo = windFarmMap.get(record.id);
        const isWindFarm = !!windFarmInfo;
        const isNegativeVolume = record.volume < 0;
        const isSOFlagged = record.soFlag === true;

        // Log invalid records for debugging
        if (isNegativeVolume && (!isWindFarm || !isSOFlagged)) {
          console.log(`Invalid record (${!isWindFarm ? 'Not wind farm' : 'Not SO flagged'}):`, {
            id: record.id,
            name: windFarmInfo?.name || 'Unknown',
            period: period,
            volume: record.volume,
            soFlag: record.soFlag
          });
        }

        return isWindFarm && isNegativeVolume && isSOFlagged;
      });

      if (validRecords.length > 0) {
        console.log(`[${date} P${period}] Found ${validRecords.length} valid curtailment records`);
        console.log('Sample valid record:', JSON.stringify(validRecords[0], null, 2));

        // Calculate period totals
        const periodTotal = validRecords.reduce((sum, r) => sum + Math.abs(r.volume), 0);
        const periodPayment = validRecords.reduce((sum, r) => sum + (Math.abs(r.volume) * r.originalPrice * -1), 0);
        console.log(`[${date} P${period}] Volume: ${periodTotal.toFixed(2)} MWh, Payment: £${periodPayment.toFixed(2)}`);
      }

      allRecords = [...allRecords, ...validRecords];
    }

    return allRecords;
  } catch (error) {
    console.error(`Detailed error for ${date}:`, error);
    if (axios.isAxiosError(error)) {
      console.error(`API error details:`, {
        status: error.response?.status,
        statusText: error.response?.statusText,
        data: error.response?.data
      });
    }
    throw error;
  }
}

export async function fetchBidsOffers(date: string, period: number): Promise<ElexonBidOffer[]> {
  return fetchBatchBidsOffers(date, [period]);
}

export async function fetchMultiplePeriods(date: string, startPeriod: number, endPeriod: number): Promise<ElexonBidOffer[]> {
  const periods = Array.from({ length: endPeriod - startPeriod + 1 }, (_, i) => startPeriod + i);
  const batches = [];

  // Split periods into smaller batches
  for (let i = 0; i < periods.length; i += BATCH_SIZE) {
    batches.push(periods.slice(i, i + BATCH_SIZE));
  }

  let allRecords: ElexonBidOffer[] = [];

  // Process batches sequentially to maintain rate limits
  for (const batch of batches) {
    console.log(`Processing batch for periods ${batch[0]}-${batch[batch.length-1]}`);
    const batchRecords = await fetchBatchBidsOffers(date, batch);
    allRecords = [...allRecords, ...batchRecords];

    // Add delay between batches
    if (batches.indexOf(batch) < batches.length - 1) {
      console.log(`Adding delay of ${BATCH_DELAY}ms between batches...`);
      await delay(BATCH_DELAY);
    }
  }

  return allRecords;
}

export async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}