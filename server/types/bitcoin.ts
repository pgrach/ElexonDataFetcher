import { z } from 'zod';

export const BitcoinCalculationSchema = z.object({
  bitcoinMined: z.number(),
  valueAtCurrentPrice: z.number(),
  difficulty: z.number(),
  price: z.number()
});

export type BitcoinCalculation = z.infer<typeof BitcoinCalculationSchema>;

// Miner efficiency is in J/TH, hashrate in TH/s
export interface MinerStats {
  hashrate: number;   // TH/s
  power: number;      // Watts
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