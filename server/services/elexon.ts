import { db } from "@db";
import axios from "axios";
import fs from "fs/promises";
import path from "path";
import { ElexonBidOffer, ElexonResponse } from "../types/elexon";

const ELEXON_BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1";
const BMU_MAPPING_PATH = path.join(process.cwd(), 'server', 'data', 'bmuMapping.json');
const BATCH_SIZE = 12; // Process 12 settlement periods concurrently
const MAX_RETRIES = 3;
const BASE_DELAY = 100; // Base delay in ms
const MAX_REQUESTS_PER_MINUTE = 4500; // Keep slightly under the 5000 limit for safety
const REQUEST_WINDOW = 60000; // 1 minute in milliseconds

let windFarmIds: Set<string> | null = null;
let requestQueue: { timestamp: number }[] = [];

async function loadWindFarmIds(): Promise<Set<string>> {
  if (windFarmIds !== null) {
    return windFarmIds;
  }

  try {
    const mappingContent = await fs.readFile(BMU_MAPPING_PATH, 'utf8');
    const bmuMapping = JSON.parse(mappingContent);
    windFarmIds = new Set(bmuMapping.map((bmu: any) => bmu.elexonBmUnit));
    console.log(`Loaded ${windFarmIds.size} wind farm IDs from mapping`);
    return windFarmIds;
  } catch (error) {
    console.error('Error loading BMU mapping:', error);
    throw error;
  }
}

function checkRateLimit(): boolean {
  const now = Date.now();
  // Remove requests older than 1 minute
  requestQueue = requestQueue.filter(req => now - req.timestamp < REQUEST_WINDOW);
  return requestQueue.length < MAX_REQUESTS_PER_MINUTE;
}

async function waitForRateLimit(): Promise<void> {
  while (!checkRateLimit()) {
    await delay(100); // Wait 100ms before checking again
  }
  requestQueue.push({ timestamp: Date.now() });
}

async function fetchWithRetry(url: string, attempt = 1): Promise<any> {
  try {
    await waitForRateLimit();
    console.log(`Fetching: ${url} (Attempt ${attempt}/${MAX_RETRIES})`);
    const response = await axios.get(url);

    // Log response status and size
    const dataSize = JSON.stringify(response.data).length;
    console.log(`Response Status: ${response.status}, Data Size: ${dataSize} bytes`);

    if (response.data && (!response.data.data || !Array.isArray(response.data.data))) {
      console.warn(`Warning: Unexpected response format from ${url}`);
      console.log('Response:', JSON.stringify(response.data).slice(0, 200) + '...');
    }

    return response.data;
  } catch (error: any) {
    console.error(`Error fetching ${url} (Attempt ${attempt}/${MAX_RETRIES}):`, 
      error.response?.status || error.message);

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
    const validWindFarmIds = await loadWindFarmIds();
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

      // Log raw data counts
      console.log(`[${date} P${period}] Raw data - Bids: ${bids.length}, Offers: ${offers.length}`);

      // Process bids
      const validBids = bids.filter(record => 
        record.volume < 0 && 
        record.soFlag && 
        validWindFarmIds.has(record.id)
      );

      // Process offers
      const validOffers = offers.filter(record => 
        record.volume < 0 && 
        record.soFlag && 
        validWindFarmIds.has(record.id)
      );

      const periodRecords = [...validBids, ...validOffers];

      // Calculate period totals for logging
      const periodTotal = periodRecords.reduce((sum, r) => sum + Math.abs(r.volume), 0);
      const periodPayment = periodRecords.reduce((sum, r) => sum + (Math.abs(r.volume) * r.originalPrice * -1), 0);

      console.log(`[${date} P${period}] Valid Records: ${periodRecords.length} (from ${bids.length + offers.length} total)`);
      if (periodTotal > 0) {
        console.log(`[${date} P${period}] Volume: ${periodTotal.toFixed(2)} MWh, Payment: £${periodPayment.toFixed(2)}`);
      }

      allRecords = [...allRecords, ...periodRecords];
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

  // Split periods into batches
  for (let i = 0; i < periods.length; i += BATCH_SIZE) {
    batches.push(periods.slice(i, i + BATCH_SIZE));
  }

  let allRecords: ElexonBidOffer[] = [];

  // Process batches sequentially to maintain rate limits
  for (const batch of batches) {
    const batchRecords = await fetchBatchBidsOffers(date, batch);
    allRecords = [...allRecords, ...batchRecords];
  }

  return allRecords;
}

export async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}