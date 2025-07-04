import { db } from "@db";
import axios from "axios";
import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from 'url';
import { ElexonBidOffer, ElexonResponse } from "../types/elexon";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const ELEXON_BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1";
// Use absolute path for consistent reference across all services
const BMU_MAPPING_PATH = path.join(process.cwd(), "server", "data", "bmuMapping.json");
const MAX_REQUESTS_PER_MINUTE = 4500; // Keep buffer below 5000 limit
const REQUEST_WINDOW_MS = 60000; // 1 minute in milliseconds
const PARALLEL_REQUESTS = 10; // Allow 10 parallel requests

let windFarmIds: Set<string> | null = null;
let requestTimestamps: number[] = [];

async function loadWindFarmIds(): Promise<Set<string>> {
  if (windFarmIds !== null) {
    return windFarmIds;
  }

  try {
    console.log('Loading BMU mapping from:', BMU_MAPPING_PATH);
    const mappingContent = await fs.readFile(BMU_MAPPING_PATH, 'utf8');
    const bmuMapping = JSON.parse(mappingContent);
    windFarmIds = new Set(bmuMapping.map((bmu: any) => bmu.elexonBmUnit));
    console.log(`Loaded ${windFarmIds.size} wind farm BMU IDs`);
    return windFarmIds;
  } catch (error) {
    console.error('Error loading BMU mapping:', error);
    throw error;
  }
}

function trackRequest() {
  const now = Date.now();
  requestTimestamps = [...requestTimestamps, now].filter(timestamp => 
    now - timestamp < REQUEST_WINDOW_MS
  );
}

async function waitForRateLimit(): Promise<void> {
  const now = Date.now();
  requestTimestamps = requestTimestamps.filter(timestamp => 
    now - timestamp < REQUEST_WINDOW_MS
  );

  if (requestTimestamps.length >= MAX_REQUESTS_PER_MINUTE) {
    const oldestRequest = requestTimestamps[0];
    const waitTime = REQUEST_WINDOW_MS - (now - oldestRequest);
    console.log(`Rate limit reached, waiting ${Math.ceil(waitTime/1000)}s...`);
    await delay(waitTime + 100); // Add 100ms buffer
    return waitForRateLimit(); // Recheck after waiting
  }
}

async function makeRequest(url: string, date: string, period: number): Promise<any> {
  await waitForRateLimit();

  try {
    const response = await axios.get(url, {
      headers: { 'Accept': 'application/json' },
      timeout: 30000 // 30 second timeout
    });

    trackRequest();
    return response;
  } catch (error) {
    if (axios.isAxiosError(error) && error.response?.status === 429) {
      console.log(`[${date} P${period}] Rate limited, retrying after delay...`);
      await delay(60000); // Wait 1 minute on rate limit
      return makeRequest(url, date, period);
    }
    throw error;
  }
}

export async function fetchBidsOffers(date: string, period: number): Promise<ElexonBidOffer[]> {
  try {
    const validWindFarmIds = await loadWindFarmIds();

    // Make parallel requests for bids and offers
    const [bidsResponse, offersResponse] = await Promise.all([
      makeRequest(
        `${ELEXON_BASE_URL}/balancing/settlement/stack/all/bid/${date}/${period}`,
        date,
        period
      ),
      makeRequest(
        `${ELEXON_BASE_URL}/balancing/settlement/stack/all/offer/${date}/${period}`,
        date,
        period
      )
    ]).catch(error => {
      console.error(`[${date} P${period}] Error fetching data:`, error.message);
      return [{ data: { data: [] } }, { data: { data: [] } }];
    });

    if (!bidsResponse.data?.data || !offersResponse.data?.data) {
      console.error(`[${date} P${period}] Invalid API response format`);
      return [];
    }

    const validBids = bidsResponse.data.data.filter((record: any) => 
      record.volume < 0 && record.soFlag && validWindFarmIds.has(record.id)
    );

    const validOffers = offersResponse.data.data.filter((record: any) => 
      record.volume < 0 && record.soFlag && validWindFarmIds.has(record.id)
    );

    const allRecords = [...validBids, ...validOffers];

    if (allRecords.length > 0) {
      const periodTotal = allRecords.reduce((sum, r) => sum + Math.abs(r.volume), 0);
      const periodPayment = allRecords.reduce((sum, r) => sum + (Math.abs(r.volume) * r.originalPrice * -1), 0);
      console.log(`[${date} P${period}] Records: ${allRecords.length} (${periodTotal.toFixed(2)} MWh, £${periodPayment.toFixed(2)})`);
    }

    return allRecords;
  } catch (error) {
    if (axios.isAxiosError(error)) {
      console.error(`[${date} P${period}] Elexon API error:`, error.response?.data || error.message);
      throw new Error(`Elexon API error: ${error.response?.data?.error || error.message}`);
    }
    console.error(`[${date} P${period}] Unexpected error:`, error);
    throw error;
  }
}

export async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}