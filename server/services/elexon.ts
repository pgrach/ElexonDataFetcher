import { db } from "@db";
import axios from "axios";
import fs from "fs/promises";
import path from "path";
import { ElexonBidOffer, ElexonResponse } from "../types/elexon";

const ELEXON_BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1";
const BMU_MAPPING_PATH = path.join(process.cwd(), 'server', 'data', 'bmuMapping.json');

let windFarmIds: Set<string> | null = null;

async function loadWindFarmIds(): Promise<Set<string>> {
  if (windFarmIds !== null) {
    return windFarmIds;
  }

  try {
    const mappingContent = await fs.readFile(BMU_MAPPING_PATH, 'utf8');
    const bmuMapping = JSON.parse(mappingContent);

    // Load all wind farm BMU IDs from mapping
    windFarmIds = new Set(bmuMapping.map((bmu: any) => bmu.elexonBmUnit));

    console.log(`Loaded ${windFarmIds.size} wind farm IDs from mapping`);
    return windFarmIds;
  } catch (error) {
    console.error('Error loading BMU mapping:', error);
    throw error;
  }
}

export async function fetchBidsOffers(date: string, period: number): Promise<ElexonBidOffer[]> {
  try {
    const validWindFarmIds = await loadWindFarmIds();

    // Add detailed logging for debugging specific dates
    console.log(`[${date} P${period}] Fetching bids and offers...`);

    // Fetch bids and offers in parallel
    const [bidsResponse, offersResponse] = await Promise.all([
      axios.get<ElexonResponse>(`${ELEXON_BASE_URL}/balancing/settlement/stack/all/bid/${date}/${period}`).catch(error => {
        console.error(`[${date} P${period}] Error fetching bids:`, error.response?.data || error.message);
        return { data: { data: [] } };
      }),
      axios.get<ElexonResponse>(`${ELEXON_BASE_URL}/balancing/settlement/stack/all/offer/${date}/${period}`).catch(error => {
        console.error(`[${date} P${period}] Error fetching offers:`, error.response?.data || error.message);
        return { data: { data: [] } };
      })
    ]);

    // Enhanced error checking for API responses
    if (!bidsResponse.data?.data || !offersResponse.data?.data) {
      console.error(`[${date} P${period}] Invalid API response format`);
      console.log('Bids Response:', JSON.stringify(bidsResponse.data, null, 2));
      console.log('Offers Response:', JSON.stringify(offersResponse.data, null, 2));
      return [];
    }

    const bids = bidsResponse.data.data;
    const offers = offersResponse.data.data;

    console.log(`[${date} P${period}] Processing ${bids.length} bids and ${offers.length} offers`);

    // Process bids with enhanced logging
    const validBids = bids.filter(record => {
      const isValid = record.volume < 0 && record.soFlag && validWindFarmIds.has(record.id);
      if (isValid) {
        console.log(`[${date} P${period}] Valid bid from ${record.id}: volume=${record.volume}, originalPrice=${record.originalPrice}`);
      }
      return isValid;
    });

    // Similarly process offers with enhanced logging
    const validOffers = offers.filter(record => {
      const isValid = record.volume < 0 && record.soFlag && validWindFarmIds.has(record.id);
      if (isValid) {
        console.log(`[${date} P${period}] Valid offer from ${record.id}: volume=${record.volume}, originalPrice=${record.originalPrice}`);
      }
      return isValid;
    });

    const allRecords = [...validBids, ...validOffers];

    // Enhanced logging for debugging
    if (allRecords.length === 0) {
      console.log(`[${date} P${period}] No valid curtailment records found. Total bids: ${bids.length}, Total offers: ${offers.length}`);
      if (bids.length > 0 || offers.length > 0) {
        console.log(`[${date} P${period}] Sample bid:`, bids[0]);
        console.log(`[${date} P${period}] Sample offer:`, offers[0]);
      }
    } else {
      const periodTotal = allRecords.reduce((sum, r) => sum + Math.abs(r.volume), 0);
      const periodPayment = allRecords.reduce((sum, r) => sum + (Math.abs(r.volume) * r.originalPrice * -1), 0);
      console.log(`[${date} P${period}] Found ${allRecords.length} valid curtailment records (${validBids.length} bids, ${validOffers.length} offers)`);
      console.log(`[${date} P${period}] Period totals: ${periodTotal.toFixed(2)} MWh, £${periodPayment.toFixed(2)}`);
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