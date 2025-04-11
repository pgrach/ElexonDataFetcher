/**
 * Test Farm-Specific Historical Data Retrieval
 * 
 * This script tests our implementation that prioritizes farm-specific historical data
 * from the database instead of using on-the-fly calculations.
 */

import axios from 'axios';

async function testFarmSpecificData() {
  console.log('Testing farm-specific historical data retrieval...');
  
  try {
    // Test case 1: Farm with known historical data (should return exact value)
    const testDate = '2025-04-04';
    const farmId = 'T_VKNGW-1'; // Known farm with historical data
    const minerModel = 'S19J_PRO';
    
    // Make requests to test various scenarios
    console.log(`\nScenario 1: Known farm ${farmId} on ${testDate}`);
    
    // Get farm-specific data
    const farmResponse = await axios.get(`http://localhost:5000/api/curtailment/mining-potential?date=${testDate}&minerModel=${minerModel}&farmId=${farmId}`);
    console.log(`Farm-specific Bitcoin: ${farmResponse.data.bitcoinMined} BTC`);
    
    // Get full day data for comparison
    const fullDay = await axios.get(`http://localhost:5000/api/curtailment/mining-potential?date=${testDate}&minerModel=${minerModel}`);
    console.log(`Full day Bitcoin: ${fullDay.data.bitcoinMined} BTC`);
    
    // Calculate percentage
    const percentage = (farmResponse.data.bitcoinMined / fullDay.data.bitcoinMined) * 100;
    console.log(`Farm represents: ${percentage.toFixed(2)}% of total Bitcoin mined on ${testDate}`);
    
    // Test case 2: Non-existent farm ID (should return zero)
    const nonExistentFarm = 'FAKE_FARM_ID_12345';
    console.log(`\nScenario 2: Non-existent farm ${nonExistentFarm}`);
    
    const fakeResponse = await axios.get(`http://localhost:5000/api/curtailment/mining-potential?date=${testDate}&minerModel=${minerModel}&farmId=${nonExistentFarm}`);
    console.log(`Fake farm Bitcoin: ${fakeResponse.data.bitcoinMined} BTC`);
    
    // Verify it's zero
    console.log(`Verification: ${fakeResponse.data.bitcoinMined === 0 ? 'PASSED ✓' : 'FAILED ✗'} (Expected: 0 BTC)`);
    
    // Test case 3: Simulated farm with energy parameter
    const simulatedFarm = 'SIMULATED_TEST_FARM';
    const energyParam = 100; // MWh
    console.log(`\nScenario 3: Simulated farm with energy parameter (${energyParam} MWh)`);
    
    const simulatedResponse = await axios.get(`http://localhost:5000/api/curtailment/mining-potential?date=${testDate}&minerModel=${minerModel}&farmId=${simulatedFarm}&energy=${energyParam}`);
    console.log(`Simulated farm Bitcoin: ${simulatedResponse.data.bitcoinMined} BTC`);
    
    // Calculate expected proportion
    const totalEnergy = 11325.76; // From server logs
    const expectedProportion = energyParam / totalEnergy;
    const expectedBitcoin = fullDay.data.bitcoinMined * expectedProportion;
    
    console.log(`Expected proportional Bitcoin: ${expectedBitcoin.toFixed(8)} BTC`);
    const errorMargin = Math.abs(simulatedResponse.data.bitcoinMined - expectedBitcoin) / expectedBitcoin;
    console.log(`Error margin: ${(errorMargin * 100).toFixed(4)}%`);
    console.log(`Verification: ${errorMargin < 0.01 ? 'PASSED ✓' : 'FAILED ✗'} (within 1% error margin)`);
    
    console.log('\nFarm-specific test complete.');
  } catch (error) {
    console.error('Error during testing:', error.message);
    if (error.response) {
      console.error('Response data:', error.response.data);
      console.error('Response status:', error.response.status);
    }
  }
}

// Run the test
testFarmSpecificData();