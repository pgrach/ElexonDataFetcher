/**
 * Bitcoin calculation utilities for Bitcoin Mining Analytics platform
 * 
 * This module centralizes Bitcoin mining calculations to ensure consistent
 * results across different parts of the application.
 */

import { CalculationError } from './errors';
import { logger } from './logger';
import { minerModels, DEFAULT_DIFFICULTY, type MinerStats } from '../types/bitcoin';

// Constants for Bitcoin calculation
const SECONDS_PER_DAY = 86400;
const JOULES_IN_KWH = 3600000;
const TERAHASHES_TO_HASHES = 1e12;
const SATOSHIS_PER_BITCOIN = 1e8;
const HALVING_DATE = new Date('2024-04-20'); // Bitcoin halving occurred on April 20, 2024
const PRE_HALVING_REWARD = 6.25; // Block reward before April 20, 2024
const POST_HALVING_REWARD = 3.125; // Block reward after April 20, 2024

/**
 * Get the appropriate block reward based on the date
 * After the April 20, 2024 halving, the reward is 3.125 BTC per block
 */
function getBlockReward(date: Date = new Date()): number {
  return date >= HALVING_DATE ? POST_HALVING_REWARD : PRE_HALVING_REWARD;
}

/**
 * Calculate the amount of Bitcoin that could be mined with the given parameters
 * 
 * @param mwh - Energy in MWh
 * @param minerModel - The miner model name (e.g., 'S19J_PRO')
 * @param difficulty - Network difficulty (default to latest value if not provided)
 * @param date - Date for the calculation (affects block reward due to halving)
 * @returns The amount of Bitcoin that could be mined
 */
export function calculateBitcoin(
  mwh: number,
  minerModel: string,
  difficulty: number = DEFAULT_DIFFICULTY,
  date: Date = new Date()
): number {
  try {
    // Validate inputs
    if (mwh < 0) {
      throw new CalculationError(`Invalid energy value: ${mwh}. Must be a positive number.`);
    }
    
    if (!minerModels[minerModel]) {
      throw new CalculationError(`Invalid miner model: ${minerModel}. Valid models are: ${Object.keys(minerModels).join(', ')}`);
    }
    
    if (difficulty <= 0) {
      throw new CalculationError(`Invalid difficulty: ${difficulty}. Must be a positive number.`);
    }
    
    // Get miner stats
    const minerStats = minerModels[minerModel];
    
    // Convert MWh to kWh
    const kWh = mwh * 1000;
    
    // Calculate maximum hashes achievable with this energy
    const totalHashes = calculateTotalHashes(kWh, minerStats);
    
    // Calculate expected bitcoins with the specified date (for correct block reward)
    const bitcoinMined = calculateExpectedBitcoin(totalHashes, difficulty, date);
    
    return bitcoinMined;
  } catch (error) {
    if (error instanceof CalculationError) {
      throw error;
    }
    
    // Log and wrap unexpected errors
    logger.error('Bitcoin calculation error', {
      module: 'bitcoin',
      context: { mwh, minerModel, difficulty, date: date.toISOString() },
      error: error as Error
    });
    
    throw new CalculationError(`Bitcoin calculation failed: ${(error as Error).message}`, {
      context: { mwh, minerModel, difficulty, date: date.toISOString() },
      originalError: error as Error
    });
  }
}

/**
 * Calculate the total number of hashes that could be computed with the given energy
 */
function calculateTotalHashes(kWh: number, minerStats: MinerStats): number {
  // Energy in joules
  const joules = kWh * JOULES_IN_KWH;
  
  // Time in seconds the miner could run with this energy
  const secondsOfMining = joules / minerStats.power;
  
  // Total hashes = hashrate (H/s) * time (s)
  return minerStats.hashrate * TERAHASHES_TO_HASHES * secondsOfMining;
}

/**
 * Calculate the expected Bitcoin rewards based on hashing power
 * @param totalHashes - Total hashes calculated
 * @param difficulty - Network difficulty 
 * @param date - Date for calculating reward (to account for halving)
 */
function calculateExpectedBitcoin(
  totalHashes: number, 
  difficulty: number, 
  date: Date = new Date()
): number {
  // Expected number of hashes per block
  const hashesPerBlock = difficulty * Math.pow(2, 32);
  
  // Expected number of blocks
  const expectedBlocks = totalHashes / hashesPerBlock;
  
  // Get correct block reward based on date
  const blockReward = getBlockReward(date);
  
  // Expected Bitcoin (blocks * reward per block)
  return expectedBlocks * blockReward;
}

/**
 * Calculate the mining efficiency in Bitcoin per MWh for a given miner model
 */
export function calculateMiningEfficiency(
  minerModel: string,
  difficulty: number = DEFAULT_DIFFICULTY,
  date: Date = new Date()
): number {
  // Just calculate for 1 MWh to get BTC/MWh rate
  return calculateBitcoin(1, minerModel, difficulty, date);
}

/**
 * Convert MWh to estimated hashrate in TH/s
 */
export function energyToHashrate(mwh: number, minerModel: string): number {
  if (!minerModels[minerModel]) {
    throw new CalculationError(`Invalid miner model: ${minerModel}`);
  }
  
  // Convert MWh to W
  const watts = mwh * 1000 * 1000;
  
  // Calculate how many miners could run
  const minerCount = watts / minerModels[minerModel].power;
  
  // Calculate hashrate
  return minerCount * minerModels[minerModel].hashrate;
}

/**
 * Calculate mining revenue in fiat currency
 */
export function calculateMiningRevenue(
  bitcoin: number,
  bitcoinPrice: number
): number {
  return bitcoin * bitcoinPrice;
}

/**
 * Calculate expected mining revenue for a period
 */
export function calculateExpectedRevenue(
  mwh: number,
  minerModel: string,
  bitcoinPrice: number,
  difficulty: number = DEFAULT_DIFFICULTY,
  date: Date = new Date()
): { bitcoin: number; fiatValue: number } {
  const bitcoin = calculateBitcoin(mwh, minerModel, difficulty, date);
  const fiatValue = calculateMiningRevenue(bitcoin, bitcoinPrice);
  
  return { bitcoin, fiatValue };
}