/**
 * Bitcoin Data Models
 * 
 * This file contains TypeScript interfaces and types for Bitcoin-related data.
 */

import fs from 'fs/promises';
import path from 'path';

// Path to historical difficulty data
const DIFFICULTIES_PATH = path.join(process.cwd(), "server", "data", "2024_difficulties.json");

// Cache for loaded data
let difficultiesCache: BitcoinDifficulty[] | null = null;

/**
 * Bitcoin mining difficulty period data
 */
export interface BitcoinDifficulty {
  /** Timestamp of difficulty adjustment */
  timestamp: number;
  
  /** Difficulty value */
  difficulty: number;
  
  /** Optional block height for reference */
  blockHeight?: number;
}

/**
 * Bitcoin miner specification
 */
export interface MinerModel {
  /** Model name (e.g., "S19J_PRO") */
  name: string;
  
  /** Hashrate in terahashes per second */
  hashrate: number;
  
  /** Power consumption in watts */
  powerConsumption: number;
  
  /** Efficiency in joules per terahash */
  efficiency: number;
}

/**
 * Bitcoin calculation result
 */
export interface BitcoinCalculationResult {
  /** Amount of Bitcoin mined */
  bitcoinMined: number;
  
  /** Value at current price in GBP */
  valueAtCurrentPrice: number;
  
  /** Network difficulty used for calculation */
  difficulty: number;
  
  /** Bitcoin price in GBP used for calculation */
  currentPrice: number;
}

/**
 * Load historical difficulty data
 * 
 * @returns Promise resolving to array of BitcoinDifficulty objects
 */
export async function loadHistoricalDifficulties(): Promise<BitcoinDifficulty[]> {
  try {
    if (!difficultiesCache) {
      console.log('Loading historical difficulties from:', DIFFICULTIES_PATH);
      const content = await fs.readFile(DIFFICULTIES_PATH, 'utf8');
      difficultiesCache = JSON.parse(content);
      console.log(`Loaded ${difficultiesCache.length} difficulty periods`);
    }
    
    return difficultiesCache;
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    console.error('Error loading historical difficulties:', errorMessage);
    throw error;
  }
}

/**
 * Get difficulty for a specific date
 * 
 * @param date Date to get difficulty for (YYYY-MM-DD format)
 * @returns Promise resolving to difficulty value
 */
export async function getDifficultyForDate(date: string): Promise<number> {
  const difficulties = await loadHistoricalDifficulties();
  const timestamp = new Date(date).getTime();
  
  // Find the difficulty period active at the given date
  for (let i = difficulties.length - 1; i >= 0; i--) {
    if (difficulties[i].timestamp <= timestamp) {
      return difficulties[i].difficulty;
    }
  }
  
  // If no matching period found, return the earliest known difficulty
  return difficulties[0]?.difficulty || 0;
}

/**
 * Clear the difficulties cache
 * Use this when you need to force a reload of the difficulty data
 */
export function clearDifficultiesCache(): void {
  difficultiesCache = null;
}