import { db } from "@db";
import axios from "axios";
import fs from "fs/promises";
import path from "path";
import { ElexonBidOffer, ElexonResponse } from "../types/elexon";

const ELEXON_BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1";
const BMU_MAPPING_PATH = path.join(process.cwd(), '..', 'data', 'bmuMapping.json');

// Rate limiting configuration
const MAX_REQUESTS_PER_MINUTE = 4500; // Keep buffer below 5000 limit
const RATE_LIMIT_WINDOW = 60000; // 1 minute in milliseconds
let requestCount = 0;
let windowStart = Date.now();

let windFarmIds: Set<string> | null = null;

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

async function checkRateLimit(): Promise<void> {
  const now = Date.now();
  if (now - windowStart >= RATE_LIMIT_WINDOW) {
    // Reset window if it's expired
    requestCount = 0;
    windowStart = now;
    return;
  }

  if (requestCount >= MAX_REQUESTS_PER_MINUTE) {
    const waitTime = RATE_LIMIT_WINDOW - (now - windowStart);
    console.log(`Rate limit reached, waiting ${waitTime}ms`);
    await delay(waitTime);
    requestCount = 0;
    windowStart = Date.now();
  }
}

async function makeElexonRequest<T>(url: string, attempt = 1): Promise<T> {
  const MAX_RETRIES = 3;
  const RETRY_DELAY = 5000;

  try {
    await checkRateLimit();
    requestCount++;

    const response = await axios.get<T>(url, {
      timeout: 30000,
      headers: { 'Accept': 'application/json' }
    });

    return response.data;
  } catch (error) {
    if (axios.isAxiosError(error) && error.response?.status === 429) {
      if (attempt < MAX_RETRIES) {
        console.log(`Rate limit hit, waiting ${RETRY_DELAY}ms before retry ${attempt + 1}`);
        await delay(RETRY_DELAY);
        return makeElexonRequest(url, attempt + 1);
      }
    }
    throw error;
  }
}

export async function fetchBidsOffers(date: string, period: number): Promise<ElexonBidOffer[]> {
  try {
    const validWindFarmIds = await loadWindFarmIds();

    // Make parallel requests with rate limiting
    const [bidsResponse, offersResponse] = await Promise.all([
      makeElexonRequest<ElexonResponse>(
        `${ELEXON_BASE_URL}/balancing/settlement/stack/all/bid/${date}/${period}`
      ),
      makeElexonRequest<ElexonResponse>(
        `${ELEXON_BASE_URL}/balancing/settlement/stack/all/offer/${date}/${period}`
      )
    ]);

    // Log sample responses for verification
    if (period === 1) {
      console.log('\nSample Bid Response:', JSON.stringify(bidsResponse.data?.[0], null, 2));
      console.log('\nSample Offer Response:', JSON.stringify(offersResponse.data?.[0], null, 2));
    }

    const bids = bidsResponse.data || [];
    const offers = offersResponse.data || [];

    console.log(`[${date} P${period}] Processing ${bids.length} bids and ${offers.length} offers`);

    // Process bids with relaxed filtering
    const validBids = bids.filter(record => 
      validWindFarmIds.has(record.id) && // Only wind farms
      record.volume < 0 // Only curtailment (negative volume)
    );

    // Process offers with relaxed filtering
    const validOffers = offers.filter(record => 
      validWindFarmIds.has(record.id) && // Only wind farms
      record.volume < 0 // Only curtailment (negative volume)
    );

    const allRecords = [...validBids, ...validOffers];

    // Calculate and log period totals for verification
    const periodTotal = allRecords.reduce((sum, r) => sum + Math.abs(r.volume), 0);
    const periodPayment = allRecords.reduce((sum, r) => sum + (Math.abs(r.volume) * r.originalPrice * -1), 0);

    if (periodTotal > 0) {
      console.log(`[${date} P${period}] Period totals: ${periodTotal.toFixed(2)} MWh, £${periodPayment.toFixed(2)}`);
    }

    return allRecords;
  } catch (error) {
    if (axios.isAxiosError(error)) {
      console.error(`Elexon API error for ${date} period ${period}:`, error.response?.data || error.message);
      throw new Error(`Elexon API error: ${error.response?.data?.error || error.message}`);
    }
    throw error;
  }
}

export async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}