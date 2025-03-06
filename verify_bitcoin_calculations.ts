import { db } from "./db";
import { sql } from "drizzle-orm";

async function verifyBitcoinCalculations() {
  const TARGET_DATE = '2025-03-05';
  
  try {
    console.log(`\n=== Verifying Bitcoin Calculations for ${TARGET_DATE} ===\n`);
    
    // Check for Bitcoin calculations
    const calculations = await db
      .select({
        count: sql<string>`COUNT(*)::text`,
        totalBitcoin: sql<string>`SUM(bitcoin_mined)::text`,
        minerModels: sql<string>`COUNT(DISTINCT miner_model)::text`
      })
      .from(sql`historical_bitcoin_calculations`)
      .where(sql`settlement_date = ${TARGET_DATE}`);
    
    const calculationCount = Number(calculations[0].count);
    const totalBitcoin = Number(calculations[0].totalBitcoin);
    const minerModels = Number(calculations[0].minerModels);
    
    console.log(`Total Bitcoin calculations: ${calculationCount}`);
    console.log(`Total Bitcoin mined: ${totalBitcoin.toFixed(8)} BTC`);
    console.log(`Distinct miner models: ${minerModels}`);
    
    // Check for coverage across periods and farms
    const coverageStats = await db
      .select({
        distinctPeriods: sql<string>`COUNT(DISTINCT settlement_period)::text`,
        distinctFarms: sql<string>`COUNT(DISTINCT farm_id)::text`
      })
      .from(sql`historical_bitcoin_calculations`)
      .where(sql`settlement_date = ${TARGET_DATE}`);
    
    const distinctPeriods = Number(coverageStats[0].distinctPeriods);
    const distinctFarms = Number(coverageStats[0].distinctFarms);
    
    console.log(`\n=== Coverage Statistics ===`);
    console.log(`Periods covered: ${distinctPeriods} / 48 ${distinctPeriods === 48 ? '✅' : '❌'}`);
    console.log(`Farms covered: ${distinctFarms}`);
    
    // Check for farm consistency between curtailment records and bitcoin calculations
    const curtailmentFarms = await db
      .select({
        farmCount: sql<string>`COUNT(DISTINCT farm_id)::text`
      })
      .from(sql`curtailment_records`)
      .where(sql`settlement_date = ${TARGET_DATE}`);
      
    const bitcoinFarms = await db
      .select({
        farmCount: sql<string>`COUNT(DISTINCT farm_id)::text`
      })
      .from(sql`historical_bitcoin_calculations`)
      .where(sql`settlement_date = ${TARGET_DATE}`);
      
    const curtailmentFarmCount = Number(curtailmentFarms[0].farmCount);
    const bitcoinFarmCount = Number(bitcoinFarms[0].farmCount);
    
    console.log(`\n=== Farm Consistency Check ===`);
    console.log(`Curtailment record farms: ${curtailmentFarmCount}`);
    console.log(`Bitcoin calculation farms: ${bitcoinFarmCount}`);
    console.log(`Farm consistency: ${curtailmentFarmCount === bitcoinFarmCount ? '✅' : '❌'}`);
    
    // Check for calculations by miner model
    const minerStats = await db
      .select({
        minerModel: sql<string>`miner_model`,
        count: sql<string>`COUNT(*)::text`,
        totalBitcoin: sql<string>`SUM(bitcoin_mined)::text`,
        totalMWh: sql<string>`SUM(curtailed_mwh)::text`
      })
      .from(sql`historical_bitcoin_calculations`)
      .where(sql`settlement_date = ${TARGET_DATE}`)
      .groupBy(sql`miner_model`);
    
    console.log(`\n=== Calculations by Miner Model ===`);
    console.log(`Model\tCount\tTotal BTC\tTotal MWh`);
    
    minerStats.forEach(stat => {
      const count = Number(stat.count);
      const bitcoinMined = Number(stat.totalBitcoin);
      const curtailedMWh = Number(stat.totalMWh);
      
      console.log(`${stat.minerModel}\t${count}\t${bitcoinMined.toFixed(8)}\t${curtailedMWh.toFixed(2)}`);
    });
    
    // Generate recommendation
    console.log(`\n=== Recommendation ===`);
    
    if (distinctPeriods < 48 || curtailmentFarmCount !== bitcoinFarmCount) {
      console.log(`⚠️ Bitcoin calculations for ${TARGET_DATE} need to be reconciled.`);
      console.log(`Run the following command to update calculations:`);
      console.log(`npx tsx unified_reconciliation.ts date ${TARGET_DATE}`);
    } else {
      console.log(`✅ Bitcoin calculations for ${TARGET_DATE} appear to be complete.`);
      console.log(`No further action needed.`);
    }
    
  } catch (error) {
    console.error('Error verifying Bitcoin calculations:', error);
  }
}

verifyBitcoinCalculations();