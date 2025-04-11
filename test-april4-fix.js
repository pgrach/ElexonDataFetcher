/**
 * Test April 4 Farm-Specific Bitcoin Calculation Fix
 * 
 * This script tests our fix for the inconsistency between aggregate and individual farm
 * Bitcoin calculation values for April 4, 2025. It ensures that the system now uses
 * historical data from the database for all views when available, rather than doing
 * on-the-fly calculations with different difficulty values.
 */

import fetch from 'node-fetch';

async function testApril4Fix() {
  console.log('=== Testing April 4, 2025 Bitcoin Calculation Fix ===');
  console.log('Comparing aggregate vs proportional calculations');
  
  // First get the aggregate data for April 4, 2025
  const aggregateResponse = await fetch('http://localhost:5000/api/summary/daily/2025-04-04');
  if (!aggregateResponse.ok) {
    console.error('Failed to fetch aggregate data for 2025-04-04', await aggregateResponse.text());
    process.exit(1);
  }
  
  const aggregateData = await aggregateResponse.json();
  const totalEnergy = aggregateData.totalCurtailedEnergy;
  
  console.log('\nAggregate data for 2025-04-04:');
  console.log('Total Curtailed Energy:', totalEnergy, 'MWh');
  
  // Get Bitcoin mining potential for the aggregate data
  const bitcoinAggregateResponse = await fetch('http://localhost:5000/api/curtailment/mining-potential?date=2025-04-04&minerModel=S19J_PRO');
  if (!bitcoinAggregateResponse.ok) {
    console.error('Failed to fetch Bitcoin data', await bitcoinAggregateResponse.text());
    process.exit(1);
  }
  
  const bitcoinAggregateData = await bitcoinAggregateResponse.json();
  const totalBitcoin = bitcoinAggregateData.bitcoinMined;
  
  console.log('Total Bitcoin Mined:', totalBitcoin, 'BTC');
  console.log('Difficulty Used:', bitcoinAggregateData.difficulty);
  
  // Calculate the BTC/MWh ratio for the aggregate view
  const aggregateBtcPerMwh = totalBitcoin / totalEnergy;
  console.log('Aggregate BTC/MWh Ratio:', aggregateBtcPerMwh);
  console.log('Aggregate £/MWh at current price:', (aggregateBtcPerMwh * bitcoinAggregateData.currentPrice).toFixed(2));
  
  // Now we'll calculate what a farm with ~10% of the energy should receive
  // This is how the individual farm view would use the energy parameter
  const farmPercentage = 0.1; // 10%
  const farmEnergy = totalEnergy * farmPercentage;
  
  console.log(`\nSimulated farm with ${farmPercentage * 100}% of total energy:`);
  console.log('Farm Energy:', farmEnergy.toFixed(2), 'MWh');
  
  // Expected Bitcoin (if calculations are consistent)
  const expectedBitcoin = totalBitcoin * farmPercentage;
  console.log('Expected Bitcoin (proportional):', expectedBitcoin, 'BTC');
  
  // Get Bitcoin mining potential for this specific farm's energy
  const bitcoinFarmResponse = await fetch(
    `http://localhost:5000/api/curtailment/mining-potential?date=2025-04-04&minerModel=S19J_PRO&energy=${farmEnergy}`
  );
  if (!bitcoinFarmResponse.ok) {
    console.error(`Failed to fetch Bitcoin data for energy=${farmEnergy}`, await bitcoinFarmResponse.text());
    process.exit(1);
  }
  
  const bitcoinFarmData = await bitcoinFarmResponse.json();
  console.log('Actual Bitcoin Calculated:', bitcoinFarmData.bitcoinMined, 'BTC');
  console.log('Difficulty Used:', bitcoinFarmData.difficulty);
  
  // Calculate the BTC/MWh ratio for the farm calculation
  const farmBtcPerMwh = bitcoinFarmData.bitcoinMined / farmEnergy;
  console.log('Farm BTC/MWh Ratio:', farmBtcPerMwh);
  console.log('Farm £/MWh at current price:', (farmBtcPerMwh * bitcoinFarmData.currentPrice).toFixed(2));
  
  // Compare the values
  console.log('\n=== Comparison Results ===');
  
  // Check if the farm's bitcoin is proportional to its energy (matches the aggregate ratio)
  const btcDifference = Math.abs(bitcoinFarmData.bitcoinMined - expectedBitcoin);
  const btcPercentDiff = (btcDifference / expectedBitcoin) * 100;
  
  const ratiosDifferent = Math.abs(aggregateBtcPerMwh - farmBtcPerMwh) > 0.0000001;
  const difficultiesDifferent = bitcoinAggregateData.difficulty !== bitcoinFarmData.difficulty;
  
  if (ratiosDifferent) {
    console.log('❌ INCONSISTENCY DETECTED: BTC/MWh ratios differ!');
    console.log(`Aggregate: ${aggregateBtcPerMwh} vs Farm: ${farmBtcPerMwh}`);
    console.log(`Difference: ${Math.abs(aggregateBtcPerMwh - farmBtcPerMwh)}`);
    console.log(`Percentage Difference: ${btcPercentDiff.toFixed(4)}%`);
  } else {
    console.log('✅ SUCCESS: BTC/MWh ratios are consistent');
  }
  
  if (difficultiesDifferent) {
    console.log('❌ INCONSISTENCY DETECTED: Difficulty values differ!');
    console.log(`Aggregate: ${bitcoinAggregateData.difficulty} vs Farm: ${bitcoinFarmData.difficulty}`);
  } else {
    console.log('✅ SUCCESS: Difficulty values are consistent');
  }
}

testApril4Fix().catch(err => {
  console.error('Test failed with error:', err);
  process.exit(1);
});