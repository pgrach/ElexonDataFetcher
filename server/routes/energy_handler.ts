// Energy parameter handling for curtailment routes

import { db } from "@db";
import { historicalBitcoinCalculations, curtailmentRecords } from "@db/schema";
import { and, eq, sql } from "drizzle-orm";

/**
 * Handle energy parameter without farm ID
 * 
 * This function processes requests when only an energy parameter is provided,
 * calculating proportional Bitcoin based on the energy ratio compared to the total
 * for that date.
 * 
 * @param formattedDate The date to process
 * @param minerModel The miner model to use for calculations 
 * @param energyParam The energy parameter (in MWh)
 * @param currentPrice Current Bitcoin price in GBP
 * @returns Response object with calculated Bitcoin or null if calculation not possible
 */
export async function handleEnergyParameter(
  formattedDate: string, 
  minerModel: string, 
  energyParam: string, 
  currentPrice: number | null
) {
  if (!energyParam) return null;
  
  console.log(`Energy parameter handler: ${energyParam} MWh for ${formattedDate}`);
      
  // First get the total energy for this date to calculate proportion
  const curtailmentTotal = await db
    .select({
      totalEnergy: sql<string>`SUM(ABS(volume::numeric))`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, formattedDate));
    
  const dateTotal = Number(curtailmentTotal[0]?.totalEnergy) || 0;
  
  // Now get the total Bitcoin for this date
  const bitcoinTotal = await db
    .select({
      totalBitcoin: sql<string>`SUM(bitcoin_mined::numeric)`,
      difficulty: sql<string>`MIN(difficulty)`
    })
    .from(historicalBitcoinCalculations)
    .where(
      and(
        eq(historicalBitcoinCalculations.settlementDate, formattedDate),
        eq(historicalBitcoinCalculations.minerModel, minerModel)
      )
    );
    
  const dateBitcoin = Number(bitcoinTotal[0]?.totalBitcoin) || 0;
  
  // Only perform proportional calculation if we have both values
  if (dateTotal > 0 && dateBitcoin > 0) {
    const energyValue = Number(energyParam);
    const proportion = energyValue / dateTotal;
    const proportionalBitcoin = dateBitcoin * proportion;
    const difficulty = Number(bitcoinTotal[0]?.difficulty) || 0;
    
    console.log(`Proportional calculation: ${energyValue} MWh / ${dateTotal} MWh = ${proportion}`);
    console.log(`Proportional Bitcoin: ${dateBitcoin} BTC × ${proportion} = ${proportionalBitcoin} BTC`);
    
    return {
      bitcoinMined: proportionalBitcoin,
      valueAtCurrentPrice: proportionalBitcoin * (currentPrice || 0),
      difficulty: difficulty,
      currentPrice
    };
  }
  
  return null;
}