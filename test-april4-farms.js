/**
 * Test for Farm-Specific Historical Bitcoin Data retrieval
 * 
 * This script tests the consistency of Bitcoin calculations for multiple farms,
 * ensuring that the system prioritizes historical data from the database when available.
 */

// Using ES module imports instead of CommonJS
import fetch from 'node-fetch';

async function testFarms() {
  console.log('=== Farm-Specific Historical Bitcoin Data Test ===');
  console.log('Testing data retrieval for multiple farms on 2025-04-04\n');

  // Test a list of farms that should have data
  const farms = [
    'T_VKNGW-1',
    'T_VKNGW-2',
    'T_VKNGW-3',
    'T_VKNGW-4',
    'T_SGRWO-1',
    'TOTALLY_NON_EXISTENT_FARM'  // This one doesn't exist and should fallback to calculation
  ];

  // Get aggregate data first for reference
  console.log('Getting aggregate data for 2025-04-04...');
  const aggregateResponse = await fetch(
    'http://localhost:5000/api/curtailment/mining-potential?date=2025-04-04&minerModel=S19J_PRO'
  );
  if (!aggregateResponse.ok) {
    console.error('Failed to fetch aggregate data', await aggregateResponse.text());
    process.exit(1);
  }
  const aggregateData = await aggregateResponse.json();
  console.log(`Aggregate Bitcoin: ${aggregateData.bitcoinMined} BTC, Difficulty: ${aggregateData.difficulty}\n`);

  // Test each farm
  console.log('Testing farms:');
  for (const farm of farms) {
    try {
      const response = await fetch(
        `http://localhost:5000/api/curtailment/mining-potential?date=2025-04-04&minerModel=S19J_PRO&farmId=${farm}`
      );
      
      if (!response.ok) {
        console.error(`Failed to fetch data for farm ${farm}`, await response.text());
        continue;
      }
      
      const data = await response.json();
      console.log(`Farm: ${farm.padEnd(20)} | Bitcoin: ${data.bitcoinMined} BTC | Difficulty: ${data.difficulty}`);
      
      // Check difficulty consistency
      if (data.difficulty !== aggregateData.difficulty) {
        console.warn(`  ⚠️ Inconsistent difficulty for ${farm}`);
      }
    } catch (error) {
      console.error(`Error processing farm ${farm}:`, error);
    }
  }
  
  // Also test with just an energy parameter (no farm ID)
  console.log('\nTesting with energy parameter only (10% of total):');
  const energyValue = 1132.58; // Approximately 10% of total
  const expectedProportionalBtc = aggregateData.bitcoinMined * 0.1;
  
  try {
    const energyResponse = await fetch(
      `http://localhost:5000/api/curtailment/mining-potential?date=2025-04-04&minerModel=S19J_PRO&energy=${energyValue}`
    );
    
    if (!energyResponse.ok) {
      console.error(`Failed to fetch data with energy parameter`, await energyResponse.text());
      process.exit(1);
    }
    
    const energyData = await energyResponse.json();
    console.log(`Energy only: ${energyValue} MWh | Bitcoin: ${energyData.bitcoinMined} BTC | Difficulty: ${energyData.difficulty}`);
    console.log(`Expected proportional BTC: ${expectedProportionalBtc}`);
    
    // Check calculation accuracy
    const difference = Math.abs(energyData.bitcoinMined - expectedProportionalBtc);
    const percentDifference = (difference / expectedProportionalBtc) * 100;
    
    if (percentDifference > 0.01) { // More than 0.01% difference
      console.warn(`  ⚠️ Proportional calculation off by ${percentDifference.toFixed(6)}%`);
    } else {
      console.log(`  ✅ Proportional calculation accurate (within 0.01%)`);
    }
  
    // Test energy parameter with a simulated farm
    console.log('\nTesting with energy parameter and simulated farm:');
    const simulatedResponse = await fetch(
      `http://localhost:5000/api/curtailment/mining-potential?date=2025-04-04&minerModel=S19J_PRO&farmId=SIMULATED_TEST&energy=${energyValue}`
    );
    
    if (!simulatedResponse.ok) {
      console.error(`Failed to fetch data with simulated farm and energy`, await simulatedResponse.text());
      process.exit(1);
    }
    
    const simulatedData = await simulatedResponse.json();
    console.log(`Energy + farm: ${energyValue} MWh | Bitcoin: ${simulatedData.bitcoinMined} BTC | Difficulty: ${simulatedData.difficulty}`);
    console.log(`Expected proportional BTC: ${expectedProportionalBtc}`);
    
    // Check calculation accuracy
    const diffSim = Math.abs(simulatedData.bitcoinMined - expectedProportionalBtc);
    const percentDiffSim = (diffSim / expectedProportionalBtc) * 100;
    
    if (percentDiffSim > 0.01) { // More than 0.01% difference
      console.warn(`  ⚠️ Simulated farm proportional calculation off by ${percentDiffSim.toFixed(6)}%`);
    } else {
      console.log(`  ✅ Simulated farm proportional calculation accurate (within 0.01%)`);
    }
  } catch (error) {
    console.error('Error testing with energy parameter:', error);
  }
}

testFarms().catch(err => {
  console.error('Test failed with error:', err);
  process.exit(1);
});