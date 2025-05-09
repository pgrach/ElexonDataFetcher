/**
 * Bitcoin Types
 * 
 * Type definitions for Bitcoin-related data structures.
 */

/**
 * Bitcoin calculation result from a mining calculation
 */
export interface BitcoinCalculation {
  bitcoinMined: number;
  difficulty: number;
  valueAtPrice: number;
  period: string;
}

/**
 * Bitcoin difficulty data structure
 */
export interface BitcoinDifficulty {
  timestamp: string;
  difficulty: number;
}

/**
 * Bitcoin price data structure
 */
export interface BitcoinPrice {
  timestamp: string;
  priceUsd: number;
  priceGbp: number;
}

/**
 * Bitcoin mining summary for a date range
 */
export interface BitcoinMiningSummary {
  dateRange: {
    start: string;
    end: string;
  };
  totalBitcoinMined: number;
  averageDifficulty: number;
  valueAtCurrentPrice: number;
  currentPrice: number;
  dailyDetails?: Array<{
    date: string;
    bitcoinMined: number;
    valueAtCurrentPrice: number;
  }>;
}