/**
 * Script to check Beatrice Offshore Windfarm Ltd curtailment for February 2025
 */
import { db } from "./db";
import { curtailmentRecords } from "./db/schema";
import { sql } from "drizzle-orm";

async function checkBeatriceCurtailment() {
  const BEATRICE_BMU_IDS = ['T_BEATO-1', 'T_BEATO-2', 'T_BEATO-3', 'T_BEATO-4'];
  const YEAR_MONTH = '2025-02'; // February 2025
  
  console.log(`Checking curtailment data for Beatrice Offshore Windfarm Ltd for ${YEAR_MONTH}`);
  
  try {
    // Calculate total curtailment by farm id
    const curtailmentByFarm = await db
      .select({
        farmId: curtailmentRecords.farmId,
        totalVolume: sql<string>`SUM(ABS(${curtailmentRecords.volume})::numeric)`,
        totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(
        sql`${curtailmentRecords.farmId} IN (${sql.join(BEATRICE_BMU_IDS, ", ")}) 
            AND ${curtailmentRecords.settlementDate} >= ${YEAR_MONTH + '-01'}
            AND ${curtailmentRecords.settlementDate} <= ${YEAR_MONTH + '-29'}`
      )
      .groupBy(curtailmentRecords.farmId);
    
    // Calculate overall total
    const overallTotal = await db
      .select({
        totalVolume: sql<string>`SUM(ABS(${curtailmentRecords.volume})::numeric)`,
        totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(
        sql`${curtailmentRecords.farmId} IN (${sql.join(BEATRICE_BMU_IDS, ", ")}) 
            AND ${curtailmentRecords.settlementDate} >= ${YEAR_MONTH + '-01'}
            AND ${curtailmentRecords.settlementDate} <= ${YEAR_MONTH + '-29'}`
      );
    
    // Get daily breakdown
    const dailyBreakdown = await db
      .select({
        settlementDate: curtailmentRecords.settlementDate,
        totalVolume: sql<string>`SUM(ABS(${curtailmentRecords.volume})::numeric)`,
        totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(
        sql`${curtailmentRecords.farmId} IN (${sql.join(BEATRICE_BMU_IDS, ", ")}) 
            AND ${curtailmentRecords.settlementDate} >= ${YEAR_MONTH + '-01'}
            AND ${curtailmentRecords.settlementDate} <= ${YEAR_MONTH + '-29'}`
      )
      .groupBy(curtailmentRecords.settlementDate)
      .orderBy(curtailmentRecords.settlementDate);
    
    // Display results
    console.log('\nCurtailment by Farm:');
    console.log('-------------------');
    curtailmentByFarm.forEach(farm => {
      console.log(`${farm.farmId}: ${parseFloat(farm.totalVolume).toFixed(2)} MWh, £${parseFloat(farm.totalPayment).toFixed(2)}`);
    });
    
    console.log('\nDaily Breakdown:');
    console.log('---------------');
    dailyBreakdown.forEach(day => {
      console.log(`${day.settlementDate}: ${parseFloat(day.totalVolume).toFixed(2)} MWh, £${parseFloat(day.totalPayment).toFixed(2)}`);
    });
    
    console.log('\nOverall Total for Beatrice Offshore Windfarm Ltd:');
    console.log('---------------------------------------------');
    console.log(`Total Volume: ${parseFloat(overallTotal[0].totalVolume).toFixed(2)} MWh`);
    console.log(`Total Payment: £${parseFloat(overallTotal[0].totalPayment).toFixed(2)}`);
    
  } catch (error) {
    console.error('Error querying curtailment data:', error);
  }
}

// Run the check
checkBeatriceCurtailment();