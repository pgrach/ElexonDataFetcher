import { z } from 'zod';

export const BitcoinCalculationSchema = z.object({
  bitcoinMined: z.number(),
  difficulty: z.union([
    z.number(),
    z.object({ difficulty: z.number() }),
    z.null()
  ])
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
export type DynamoDBDifficultyResponse = number | DynamoDBHistoricalData | null;

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