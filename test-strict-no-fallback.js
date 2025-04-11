/**
 * Test Strict No Fallback Behavior
 * 
 * This script tests that our implementation now properly refuses to fallback to
 * on-the-fly calculations when historical data is not available.
 */

import axios from 'axios';

async function testStrictNoFallback() {
  console.log('Testing strict no-fallback behavior...');
  
  try {
    // Test case 1: Request for a date far in the future (2026) with no historical data
    const futureDate = '2026-04-04';
    const minerModel = 'S19J_PRO';
    
    console.log(`\nScenario 1: Testing future date ${futureDate} (should fail with 400)`);
    
    try {
      const response = await axios.get(`http://localhost:5000/api/curtailment/mining-potential?date=${futureDate}&minerModel=${minerModel}`);
      console.log(`UNEXPECTED SUCCESS: Got status ${response.status}`);
      console.log('Response data:', response.data);
      console.log('Test result: FAILED ✗ (Expected 400 error but got success)');
    } catch (error) {
      if (error.response && error.response.status === 400) {
        console.log(`Got expected error response with status ${error.response.status}`);
        console.log('Error message:', error.response.data.message);
        console.log('Test result: PASSED ✓ (Correctly refused to fallback to calculations)');
      } else {
        console.error(`Unexpected error: ${error.message}`);
        if (error.response) {
          console.error('Response status:', error.response.status);
          console.error('Response data:', error.response.data);
        }
        console.log('Test result: FAILED ✗ (Got unexpected error)');
      }
    }
    
    // Test case 2: Try with energy parameter for future date
    console.log(`\nScenario 2: Testing future date ${futureDate} with energy parameter (should fail with 400)`);
    
    try {
      const response = await axios.get(`http://localhost:5000/api/curtailment/mining-potential?date=${futureDate}&minerModel=${minerModel}&energy=100`);
      console.log(`UNEXPECTED SUCCESS: Got status ${response.status}`);
      console.log('Response data:', response.data);
      console.log('Test result: FAILED ✗ (Expected 400 error but got success)');
    } catch (error) {
      if (error.response && error.response.status === 400) {
        console.log(`Got expected error response with status ${error.response.status}`);
        console.log('Error message:', error.response.data.message);
        console.log('Test result: PASSED ✓ (Correctly refused to fallback to calculations)');
      } else {
        console.error(`Unexpected error: ${error.message}`);
        if (error.response) {
          console.error('Response status:', error.response.status);
          console.error('Response data:', error.response.data);
        }
        console.log('Test result: FAILED ✗ (Got unexpected error)');
      }
    }
    
    console.log('\nStrict no-fallback test complete.');
  } catch (error) {
    console.error('Error during testing:', error.message);
  }
}

// Run the test
testStrictNoFallback();