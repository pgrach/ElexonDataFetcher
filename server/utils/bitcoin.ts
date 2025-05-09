/**
 * Bitcoin Mining Calculation Utilities
 * 
 * This module provides functions for calculating Bitcoin mining potential
 * based on energy input, difficulty, and miner specifications.
 */

import { BitcoinCalculation } from '../types/bitcoin';

// Miner configurations
interface MinerConfig {
  hashrate: number; // TH/s
  power: number;    // watts
}

// Map of miner models to their specifications
const minerConfigs: Record<string, MinerConfig> = {
  'S19J_PRO': {
    hashrate: 104, // TH/s
    power: 3068    // watts
  },
  'S19_XP': {
    hashrate: 140, // TH/s
    power: 3010    // watts  
  },
  'M50S': {
    hashrate: 130, // TH/s
    power: 3300    // watts
  }
};

// Default miner to use if no model is specified
const DEFAULT_MINER = 'S19J_PRO';

/**
 * Calculate Bitcoin mining potential from energy input
 * 
 * @param energyMWh Energy input in MWh
 * @param difficulty Current Bitcoin network difficulty
 * @param minerModel Miner model to use for calculations (optional, default: S19J_PRO)
 * @returns Bitcoin calculation result
 */
export function calculateBitcoin(
  energyMWh: number,
  difficulty: number,
  minerModel: string = DEFAULT_MINER
): BitcoinCalculation {
  // Get miner configuration
  const config = minerConfigs[minerModel] || minerConfigs[DEFAULT_MINER];
  
  // Convert MWh to Wh for calculation
  const energyWh = energyMWh * 1000000;
  
  // Calculate how many miners could run
  const miningHours = energyWh / config.power;
  
  // Calculate total hash power (TH)
  const totalHashPower = miningHours * config.hashrate;
  
  // Bitcoin mining formula constants
  const BLOCK_REWARD = 3.125; // Current block reward in BTC
  const BLOCKS_PER_DAY = 144; // Average blocks per day
  const DIFFICULTY_FACTOR = 2**32; // Difficulty factor
  
  // Calculate share of total network hashpower
  const networkDifficulty = difficulty / DIFFICULTY_FACTOR;
  const dailyHashesRequired = networkDifficulty * BLOCKS_PER_DAY;
  const hashShare = totalHashPower / dailyHashesRequired;
  
  // Calculate Bitcoin mined
  const bitcoinMined = hashShare * BLOCK_REWARD * BLOCKS_PER_DAY;
  
  // Generate period identifier for the calculation
  const now = new Date();
  const period = now.toISOString().slice(0, 19).replace('T', ' ');
  
  return {
    bitcoinMined,
    difficulty,
    valueAtPrice: 0, // This will be set by the calling code with current price
    period
  };
}