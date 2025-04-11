/**
 * Test Energy Parameter Proportional Calculation
 * 
 * This script tests our fix for energy parameter without farm ID,
 * ensuring that energy-only calculations correctly apply proportions
 * based on historical data for the date.
 */

import axios from 'axios';

async function testEnergyParameter() {
  console.log('Testing energy parameter without farm ID...');
  
  try {
    // Test case 1: April 4, 2025 with 100 MWh (about 10% of daily total)
    const testDate = '2025-04-04';
    const energy = 100; // MWh
    const minerModel = 'S19J_PRO';
    
    // Make requests to test various scenarios
    console.log(`\nScenario 1: Testing date ${testDate} with energy=${energy} MWh`);
    
    // Get full Bitcoin for the day first (baseline)
    const fullDay = await axios.get(`http://localhost:5000/api/curtailment/mining-potential?date=${testDate}&minerModel=${minerModel}`);
    console.log(`Full day Bitcoin: ${fullDay.data.bitcoinMined} BTC`);
    
    // Now test with energy parameter
    const withEnergy = await axios.get(`http://localhost:5000/api/curtailment/mining-potential?date=${testDate}&minerModel=${minerModel}&energy=${energy}`);
    console.log(`With energy parameter ${energy} MWh: ${withEnergy.data.bitcoinMined} BTC`);
    
    // We need to get the actual total energy from the log message
    const realTotalEnergy = 11325.76; // From server logs
    const expectedProportion = energy / realTotalEnergy;
    const expectedBitcoin = fullDay.data.bitcoinMined * expectedProportion;
    
    console.log(`\nAnalysis:`);
    console.log(`Total energy for ${testDate}: ${realTotalEnergy} MWh`);
    console.log(`Proportion: ${energy} / ${realTotalEnergy} = ${expectedProportion.toFixed(6)} (${(expectedProportion * 100).toFixed(4)}%)`);
    console.log(`Expected Bitcoin: ${fullDay.data.bitcoinMined} × ${expectedProportion.toFixed(6)} = ${expectedBitcoin.toFixed(8)} BTC`);
    console.log(`Actual Bitcoin: ${withEnergy.data.bitcoinMined} BTC`);
    
    // Check if the result matches our expectation within a small margin of error
    const errorMargin = Math.abs(withEnergy.data.bitcoinMined - expectedBitcoin) / expectedBitcoin;
    console.log(`Error margin: ${(errorMargin * 100).toFixed(4)}%`);
    console.log(`Test result: ${errorMargin < 0.01 ? 'PASSED ✓' : 'FAILED ✗'} (within 1% error margin)`);
    
    // Test case 2: Try another energy value
    const energy2 = 250; // MWh
    console.log(`\nScenario 2: Testing date ${testDate} with energy=${energy2} MWh`);
    
    const withEnergy2 = await axios.get(`http://localhost:5000/api/curtailment/mining-potential?date=${testDate}&minerModel=${minerModel}&energy=${energy2}`);
    console.log(`With energy parameter ${energy2} MWh: ${withEnergy2.data.bitcoinMined} BTC`);
    
    const expectedProportion2 = energy2 / realTotalEnergy;
    const expectedBitcoin2 = fullDay.data.bitcoinMined * expectedProportion2;
    
    console.log(`\nAnalysis:`);
    console.log(`Proportion: ${energy2} / ${realTotalEnergy} = ${expectedProportion2.toFixed(6)} (${(expectedProportion2 * 100).toFixed(4)}%)`);
    console.log(`Expected Bitcoin: ${fullDay.data.bitcoinMined} × ${expectedProportion2.toFixed(6)} = ${expectedBitcoin2.toFixed(8)} BTC`);
    console.log(`Actual Bitcoin: ${withEnergy2.data.bitcoinMined} BTC`);
    
    const errorMargin2 = Math.abs(withEnergy2.data.bitcoinMined - expectedBitcoin2) / expectedBitcoin2;
    console.log(`Error margin: ${(errorMargin2 * 100).toFixed(4)}%`);
    console.log(`Test result: ${errorMargin2 < 0.01 ? 'PASSED ✓' : 'FAILED ✗'} (within 1% error margin)`);
    
    console.log('\nEnergy parameter test complete.');
  } catch (error) {
    console.error('Error during testing:', error.message);
    if (error.response) {
      console.error('Response data:', error.response.data);
      console.error('Response status:', error.response.status);
    }
  }
}

// Helper function to get the total energy for a date from the API
async function getTotalEnergyForDate(date) {
  try {
    // This endpoint gives us daily curtailment data including total energy
    const response = await axios.get(`http://localhost:5000/api/summary/daily/${date}`);
    if (response.data && response.data.totalEnergyMWh) {
      return Number(response.data.totalEnergyMWh);
    }
    // Fallback if the API doesn't return the expected data
    return 1000; // Default fallback value
  } catch (error) {
    console.error('Error fetching total energy:', error.message);
    return 1000; // Default fallback value
  }
}

// Run the test
testEnergyParameter();