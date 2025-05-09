/**
 * Minerstat API Service
 * 
 * This service handles interactions with the Minerstat API to fetch Bitcoin price
 * and difficulty data, with caching for performance.
 */

import axios from 'axios';
import { priceCache, difficultyCache } from '../utils/cache';

/**
 * Fetch current Bitcoin price and difficulty from Minerstat API
 * Uses caching to reduce API calls
 * 
 * @returns Promise resolving to difficulty and price in GBP
 */
export async function fetchBitcoinStats(): Promise<{
  difficulty: number;
  priceGbp: number;
  source: 'cache' | 'api';
}> {
  try {
    // First check if we have cached values
    const cachedPrice = priceCache.get('current');
    const cachedDifficulty = difficultyCache.get('current');
    
    // If both values are in cache, return them
    if (cachedPrice !== undefined && cachedDifficulty !== undefined) {
      console.log('Using cached Minerstat data:', {
        difficulty: cachedDifficulty,
        priceGbp: cachedPrice,
        source: 'cache'
      });
      
      return {
        difficulty: cachedDifficulty,
        priceGbp: cachedPrice,
        source: 'cache'
      };
    }
    
    // Otherwise, fetch from API
    const response = await axios.get('https://api.minerstat.com/v2/coins?list=BTC');
    const btcData = response.data[0];

    if (!btcData || typeof btcData.difficulty !== 'number' || typeof btcData.price !== 'number') {
      throw new Error('Invalid response format from Minerstat API');
    }

    // Convert USD to GBP (using a fixed rate - in production this should be fetched from a forex API)
    const usdToGbpRate = 0.79; // Example fixed rate
    const priceInGbp = btcData.price * usdToGbpRate;

    console.log('Minerstat API response:', {
      difficulty: btcData.difficulty,
      priceUsd: btcData.price,
      priceGbp: priceInGbp
    });

    // Cache the values (1 hour TTL is set in the cache module)
    priceCache.set('current', priceInGbp);
    difficultyCache.set('current', btcData.difficulty);
    
    return {
      difficulty: btcData.difficulty,
      priceGbp: priceInGbp,
      source: 'api'
    };
  } catch (error) {
    console.error('Error fetching from Minerstat API:', error);
    
    // If we have cached values, return them as fallback
    const cachedPrice = priceCache.get('current');
    const cachedDifficulty = difficultyCache.get('current');
    
    if (cachedPrice !== undefined && cachedDifficulty !== undefined) {
      console.log('Using cached data as fallback after API error');
      return {
        difficulty: cachedDifficulty,
        priceGbp: cachedPrice,
        source: 'cache'
      };
    }
    
    // If no cached values either, throw error
    throw new Error('Failed to fetch Bitcoin stats and no cached data available');
  }
}

/**
 * Format Bitcoin price for display
 * 
 * @param price - The price to format
 * @returns Formatted price string
 */
export function formatBitcoinPrice(price: number): string {
  return new Intl.NumberFormat('en-GB', {
    style: 'currency',
    currency: 'GBP',
    maximumFractionDigits: 0
  }).format(price);
}