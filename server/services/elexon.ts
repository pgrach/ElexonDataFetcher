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

    // Fetch bids and offers in parallel
    const [bidsResponse, offersResponse] = await Promise.all([
      axios.get<ElexonResponse>(`${ELEXON_BASE_URL}/balancing/settlement/stack/all/bid/${date}/${period}`),
      axios.get<ElexonResponse>(`${ELEXON_BASE_URL}/balancing/settlement/stack/all/offer/${date}/${period}`)
    ]);

    // Log sample responses for debugging
    if (period === 1) {
      const sampleBid = bidsResponse.data?.data?.[0];
      const sampleOffer = offersResponse.data?.data?.[0];
      console.log('\nSample Bid Response:', JSON.stringify(sampleBid, null, 2));
      console.log('\nSample Offer Response:', JSON.stringify(sampleOffer, null, 2));
    }

    const bids = bidsResponse.data?.data || [];
    const offers = offersResponse.data?.data || [];

    console.log(`[${date} P${period}] Processing ${bids.length} bids and ${offers.length} offers`);

    // Process bids with updated filtering logic
    const validBids = bids.filter(record => {
      if (!record || typeof record !== 'object') return false;

      const isWindFarm = record.id && validWindFarmIds.has(record.id);
      const isNegativeVolume = typeof record.volume === 'number' && record.volume < 0;
      const hasValidPrices = typeof record.originalPrice === 'number';
      const isSoFlagged = record.soFlag === true; // Explicitly check for SO flag

      if (isWindFarm && isNegativeVolume && hasValidPrices && isSoFlagged) {
        console.log(`[${date} P${period}] Valid bid from ${record.id}: volume=${record.volume}, originalPrice=${record.originalPrice}, SO-flagged=true`);
        return true;
      } else {
        if (isWindFarm) {
          console.log(`[${date} P${period}] Skipping bid from ${record.id}: volume=${record.volume}, originalPrice=${record.originalPrice}, SO-flagged=${record.soFlag}`);
        }
        return false;
      }
    });

    // Process offers with the same updated filtering logic
    const validOffers = offers.filter(record => {
      if (!record || typeof record !== 'object') return false;

      const isWindFarm = record.id && validWindFarmIds.has(record.id);
      const isNegativeVolume = typeof record.volume === 'number' && record.volume < 0;
      const hasValidPrices = typeof record.originalPrice === 'number';
      const isSoFlagged = record.soFlag === true; // Explicitly check for SO flag

      if (isWindFarm && isNegativeVolume && hasValidPrices && isSoFlagged) {
        console.log(`[${date} P${period}] Valid offer from ${record.id}: volume=${record.volume}, originalPrice=${record.originalPrice}, SO-flagged=true`);
        return true;
      } else {
        if (isWindFarm) {
          console.log(`[${date} P${period}] Skipping offer from ${record.id}: volume=${record.volume}, originalPrice=${record.originalPrice}, SO-flagged=${record.soFlag}`);
        }
        return false;
      }
    });

    const allRecords = [...validBids, ...validOffers].map(record => ({
      ...record,
      volume: Math.abs(record.volume), // Convert to positive for easier processing
      payment: Math.abs(record.volume) * record.originalPrice * -1 // Calculate payment as per specification
    }));

    console.log(`[${date} P${period}] Found ${allRecords.length} valid curtailment records (${validBids.length} bids, ${validOffers.length} offers)`);

    // Calculate and log period totals
    if (allRecords.length > 0) {
      const periodTotal = allRecords.reduce((sum, r) => sum + r.volume, 0);
      const periodPayment = allRecords.reduce((sum, r) => sum + r.payment, 0);
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