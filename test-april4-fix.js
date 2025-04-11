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
  
  // Test with a real farm - Viking Wind Farm (T_VKNGW-1)
  console.log('\nTesting with real farm: T_VKNGW-1');
  
  // Get Bitcoin mining potential for a specific farm
  const bitcoinFarmResponse = await fetch(
    `http://localhost:5000/api/curtailment/mining-potential?date=2025-04-04&minerModel=S19J_PRO&farmId=T_VKNGW-1`
  );
  if (!bitcoinFarmResponse.ok) {
    console.error(`Failed to fetch Bitcoin data for farm T_VKNGW-1`, await bitcoinFarmResponse.text());
    process.exit(1);
  }
  
  // Also test a fallback for a farm with energy parameter but no historical data
  console.log('\nTesting with simulated farm (10% energy):');
  const farmPercentage = 0.1; // 10%
  const farmEnergy = totalEnergy * farmPercentage;
  console.log('Farm Energy:', farmEnergy.toFixed(2), 'MWh');
  
  // Expected Bitcoin (if calculations are consistent using the BTC/MWh ratio)
  const expectedBitcoin = totalBitcoin * farmPercentage;
  console.log('Expected Bitcoin (proportional):', expectedBitcoin.toFixed(8), 'BTC');
  
  // Test the fallback calculation by using a non-existent farm ID
  const bitcoinFallbackResponse = await fetch(
    `http://localhost:5000/api/curtailment/mining-potential?date=2025-04-04&minerModel=S19J_PRO&farmId=SIMULATED_FARM&energy=${farmEnergy}`
  );
  if (!bitcoinFallbackResponse.ok) {
    console.error(`Failed to fetch Bitcoin data for fallback calculation`, await bitcoinFallbackResponse.text());
    process.exit(1);
  }
  
  // Process actual farm data
  const bitcoinFarmData = await bitcoinFarmResponse.json();
  console.log('Actual Farm Bitcoin:', bitcoinFarmData.bitcoinMined, 'BTC');
  console.log('Farm Difficulty Used:', bitcoinFarmData.difficulty);

  // Process fallback calculation data
  const bitcoinFallbackData = await bitcoinFallbackResponse.json();
  console.log('\nFallback calculation results:');
  console.log('Calculated Bitcoin:', bitcoinFallbackData.bitcoinMined, 'BTC');
  console.log('Fallback Difficulty Used:', bitcoinFallbackData.difficulty);
  
  // Evaluate farm vs. fallback calculation
  console.log('\nEvaluating farm vs. fallback calculation:');
  // We don't know the exact farm energy, but we can check if the difficulty values are consistent
  const farmVsFallbackDifficultyDifferent = bitcoinFarmData.difficulty !== bitcoinFallbackData.difficulty;
  
  // Evaluate fallback calculation vs. expected proportional result
  const btcDifference = Math.abs(bitcoinFallbackData.bitcoinMined - expectedBitcoin);
  const btcPercentDiff = (btcDifference / expectedBitcoin) * 100;
  
  // Calculate the BTC/MWh ratio for the fallback calculation
  const fallbackBtcPerMwh = bitcoinFallbackData.bitcoinMined / farmEnergy;
  console.log('Fallback BTC/MWh Ratio:', fallbackBtcPerMwh);
  console.log('Fallback £/MWh at current price:', (fallbackBtcPerMwh * bitcoinFallbackData.currentPrice).toFixed(2));
  
  // Compare the values
  console.log('\n=== Comparison Results ===');
  
  const ratiosDifferent = Math.abs(aggregateBtcPerMwh - fallbackBtcPerMwh) > 0.0000001;
  const difficultiesDifferent = bitcoinAggregateData.difficulty !== bitcoinFallbackData.difficulty;
  
  // Check if farm and fallback use the same difficulty
  if (farmVsFallbackDifficultyDifferent) {
    console.log('❌ INCONSISTENCY DETECTED: Farm vs Fallback difficulty values differ!');
    console.log(`Farm: ${bitcoinFarmData.difficulty} vs Fallback: ${bitcoinFallbackData.difficulty}`);
  } else {
    console.log('✅ SUCCESS: Farm and fallback difficulty values are consistent');
  }
  
  // Check if the fallback calculation gives the expected proportional value
  if (ratiosDifferent) {
    console.log('❌ INCONSISTENCY DETECTED: BTC/MWh ratios differ in fallback calculation!');
    console.log(`Aggregate: ${aggregateBtcPerMwh} vs Fallback: ${fallbackBtcPerMwh}`);
    console.log(`Difference: ${Math.abs(aggregateBtcPerMwh - fallbackBtcPerMwh)}`);
    console.log(`Percentage Difference: ${btcPercentDiff.toFixed(4)}%`);
  } else {
    console.log('✅ SUCCESS: BTC/MWh ratios are consistent in fallback calculation');
  }
  
  // Check if the aggregate and fallback calculations use the same difficulty
  if (difficultiesDifferent) {
    console.log('❌ INCONSISTENCY DETECTED: Aggregate vs Fallback difficulty values differ!');
    console.log(`Aggregate: ${bitcoinAggregateData.difficulty} vs Fallback: ${bitcoinFallbackData.difficulty}`);
  } else {
    console.log('✅ SUCCESS: Aggregate and fallback difficulty values are consistent');
  }
}

testApril4Fix().catch(err => {
  console.error('Test failed with error:', err);
  process.exit(1);
});