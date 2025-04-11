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
  console.log('Comparing aggregate data vs. specific energy calculations');
  
  // First get the aggregate data for April 4, 2025
  const aggregateResponse = await fetch('http://localhost:5000/api/summary/daily/2025-04-04');
  if (!aggregateResponse.ok) {
    console.error('Failed to fetch aggregate data for 2025-04-04', await aggregateResponse.text());
    process.exit(1);
  }
  
  const aggregateData = await aggregateResponse.json();
  console.log('\nAggregate data for 2025-04-04:');
  console.log('Total Curtailed Energy:', aggregateData.totalCurtailedEnergy, 'MWh');
  
  // Get Bitcoin mining potential for the aggregate data
  const bitcoinAggregateResponse = await fetch('http://localhost:5000/api/curtailment/mining-potential?date=2025-04-04&minerModel=S19J_PRO');
  if (!bitcoinAggregateResponse.ok) {
    console.error('Failed to fetch Bitcoin data', await bitcoinAggregateResponse.text());
    process.exit(1);
  }
  
  const bitcoinAggregateData = await bitcoinAggregateResponse.json();
  console.log('Total Bitcoin Mined:', bitcoinAggregateData.bitcoinMined, 'BTC');
  console.log('Difficulty Used:', bitcoinAggregateData.difficulty);
  
  // Calculate the BTC/MWh ratio for the aggregate view
  const aggregateBtcPerMwh = bitcoinAggregateData.bitcoinMined / aggregateData.totalCurtailedEnergy;
  console.log('Aggregate BTC/MWh Ratio:', aggregateBtcPerMwh);
  console.log('Aggregate £/MWh at current price:', (aggregateBtcPerMwh * bitcoinAggregateData.currentPrice).toFixed(2));
  
  // Now test with a specific energy value using the energy parameter
  // Let's use 1000 MWh as our test energy value
  const testEnergyValue = 1000; // MWh
  
  // Get Bitcoin mining potential for this specific energy value
  const bitcoinEnergyResponse = await fetch(
    `http://localhost:5000/api/curtailment/mining-potential?date=2025-04-04&minerModel=S19J_PRO&energy=${testEnergyValue}`
  );
  if (!bitcoinEnergyResponse.ok) {
    console.error(`Failed to fetch Bitcoin data for energy=${testEnergyValue}`, await bitcoinEnergyResponse.text());
    process.exit(1);
  }
  
  const bitcoinEnergyData = await bitcoinEnergyResponse.json();
  console.log(`\nSpecific energy (${testEnergyValue} MWh) calculation for 2025-04-04:`);
  console.log('Bitcoin Mined:', bitcoinEnergyData.bitcoinMined, 'BTC');
  console.log('Difficulty Used:', bitcoinEnergyData.difficulty);
  
  // Calculate the BTC/MWh ratio for the specific energy calculation
  const energyBtcPerMwh = bitcoinEnergyData.bitcoinMined / testEnergyValue;
  console.log('Energy BTC/MWh Ratio:', energyBtcPerMwh);
  console.log('Energy £/MWh at current price:', (energyBtcPerMwh * bitcoinEnergyData.currentPrice).toFixed(2));
  
  // Compare the ratios
  console.log('\n=== Comparison Results ===');
  const ratiosDifferent = Math.abs(aggregateBtcPerMwh - energyBtcPerMwh) > 0.0000001;
  const difficultiesDifferent = bitcoinAggregateData.difficulty !== bitcoinEnergyData.difficulty;
  
  if (ratiosDifferent) {
    console.log('❌ INCONSISTENCY DETECTED: BTC/MWh ratios differ!');
    console.log(`Aggregate: ${aggregateBtcPerMwh} vs Energy: ${energyBtcPerMwh}`);
    console.log(`Difference: ${Math.abs(aggregateBtcPerMwh - energyBtcPerMwh)}`);
  } else {
    console.log('✅ SUCCESS: BTC/MWh ratios are consistent');
  }
  
  if (difficultiesDifferent) {
    console.log('❌ INCONSISTENCY DETECTED: Difficulty values differ!');
    console.log(`Aggregate: ${bitcoinAggregateData.difficulty} vs Energy: ${bitcoinEnergyData.difficulty}`);
  } else {
    console.log('✅ SUCCESS: Difficulty values are consistent');
  }
}

testApril4Fix().catch(err => {
  console.error('Test failed with error:', err);
  process.exit(1);
});