import { z } from 'zod';

export const BitcoinCalculationSchema = z.object({
  bitcoinMined: z.number(),
  difficulty: z.union([
    z.number(),
    z.object({ difficulty: z.number() }),
    z.null()
  ]).transform(val => {
    if (typeof val === 'number') return val;
    if (val && typeof val === 'object' && 'difficulty' in val) return val.difficulty;
    return DEFAULT_DIFFICULTY;
  })
});

export type BitcoinCalculation = z.infer<typeof BitcoinCalculationSchema>;

// Miner efficiency is in J/TH, hashrate in TH/s
export interface MinerStats {
  hashrate: number;   // TH/s
  power: number;      // Watts
}

export interface BMUCalculation {
  farmId: string;
  bitcoinMined: number;
  curtailedMwh: number;
}

export interface DynamoDBHistoricalData {
  difficulty: number;
  date: string;
}

// Type for DynamoDB response
export type DynamoDBDifficultyResponse = { difficulty: number } | null;

/**
 * Validates and extracts difficulty value from various input types
 * 
 * @param data - The difficulty data which could be a number, an object with difficulty property, or unknown
 * @returns A valid number representing Bitcoin network difficulty
 */
export function validateDifficulty(data: unknown): number {
  // If it's already a number, return it directly
  if (typeof data === 'number') {
    return data;
  }
  
  // If it's an object with a difficulty property
  if (data !== null && 
      typeof data === 'object' && 
      'difficulty' in data) {
    
    // If the difficulty property is a number
    const difficultyValue = (data as { difficulty: unknown }).difficulty;
    if (typeof difficultyValue === 'number') {
      return difficultyValue;
    }
    
    // If it's a string that can be parsed to a number
    if (typeof difficultyValue === 'string') {
      const parsed = parseFloat(difficultyValue);
      if (!isNaN(parsed)) {
        return parsed;
      }
    }
  }
  
  // Return default if we couldn't extract a valid number
  return DEFAULT_DIFFICULTY;
}

export const minerModels: Record<string, MinerStats> = {
  S19J_PRO: {
    hashrate: 100,
    power: 3050
  },
  S9: {
    hashrate: 13.5,
    power: 1323
  },
  M20S: {
    hashrate: 68,
    power: 3360
  }
};

// Default values for fallback
export const DEFAULT_DIFFICULTY = 108105433845147; // Current network difficulty as fallback