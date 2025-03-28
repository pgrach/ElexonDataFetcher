/**
 * Script to verify Elexon API URL and authentication
 */

const { execSync } = require('child_process');

// Run a verification through TSX
console.log('Checking Elexon API configuration...');

try {
  const result = execSync('npx tsx -e "import axios from \'axios\'; async function check() { try { const API_BASE_URL = \'https://data.elexon.co.uk/bmrs/api/v1\'; console.log(\'Testing API endpoint: \' + API_BASE_URL); const response = await axios.get(API_BASE_URL + \'/datasets\'); console.log(\'API connection successful!\'); console.log(\'Available datasets: \' + response.data.data.length); return true; } catch(error) { console.error(\'API connection failed:\', error.message); if (error.response) { console.log(\'Status code:\', error.response.status); console.log(\'Response data:\', JSON.stringify(error.response.data)); } return false; } } check();"', { encoding: 'utf8' });
  
  console.log(result);
  console.log('API verification complete');
} catch (error) {
  console.error('Error executing verification:', error.message);
}

// Try to specifically check the endpoint we need
console.log('\nChecking specific balancing API endpoint...');

try {
  const result = execSync('npx tsx -e "import axios from \'axios\'; async function check() { try { const API_BASE_URL = \'https://data.elexon.co.uk/bmrs/api/v1\'; const date = \'2025-03-27\'; const period = 1; console.log(`Testing specific endpoint for period ${period} on ${date}`); const response = await axios.get(`${API_BASE_URL}/balancing/bid-offer/accepted/settlement-period/${period}/settlement-date/${date}`); console.log(\'Specific endpoint successful!\'); console.log(\'Records found: \' + (response.data.data ? response.data.data.length : 0)); return true; } catch(error) { console.error(\'Specific endpoint failed:\', error.message); if (error.response) { console.log(\'Status code:\', error.response.status); console.log(\'Response data:\', JSON.stringify(error.response.data)); } return false; } } check();"', { encoding: 'utf8' });
  
  console.log(result);
  console.log('Specific endpoint verification complete');
} catch (error) {
  console.error('Error executing specific endpoint verification:', error.message);
}