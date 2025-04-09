/**
 * Fetch Sample Elexon Data
 * 
 * This script fetches sample data from the Elexon API for a specific date and period
 * to help verify the data transformation process.
 */

import { fetchBidsOffers } from "./server/services/elexon";

// Get date and period from command line args
const date = process.argv[2] || '2025-04-01';
const period = parseInt(process.argv[3] || '35');

async function main() {
  console.log(`Fetching data for ${date}, period ${period}...`);
  
  try {
    const data = await fetchBidsOffers(date, period);
    
    // Calculate positive and negative volume/payment totals
    let negativeVolume = 0;
    let positiveVolume = 0;
    let negativePayment = 0;
    let positivePayment = 0;
    
    for (const record of data) {
      const volume = parseFloat(record.volume.toString());
      const price = parseFloat(record.originalPrice.toString());
      const payment = volume * price;
      
      if (volume < 0) {
        negativeVolume += Math.abs(volume);
        negativePayment += payment;
      } else {
        positiveVolume += volume;
        positivePayment += payment;
      }
    }
    
    console.log(`\nFound ${data.length} records`);
    console.log(`Negative volume: ${negativeVolume.toFixed(2)} MWh`);
    console.log(`Negative payment: £${negativePayment.toFixed(2)}`);
    console.log(`Positive volume: ${positiveVolume.toFixed(2)} MWh`);
    console.log(`Positive payment: £${positivePayment.toFixed(2)}`);
    
    const absoluteVolume = data.reduce((sum, record) => sum + Math.abs(parseFloat(record.volume.toString())), 0);
    const paymentFromAbsVolume = data.reduce((sum, record) => {
      const volume = Math.abs(parseFloat(record.volume.toString()));
      const price = parseFloat(record.originalPrice.toString());
      return sum + (volume * price);
    }, 0);
    
    console.log(`\nCalculation method in script:`);
    console.log(`Total absolute volume: ${absoluteVolume.toFixed(2)} MWh`);
    console.log(`Payment from abs(volume): £${paymentFromAbsVolume.toFixed(2)}`);
    
    const rawPayment = data.reduce((sum, record) => {
      const volume = parseFloat(record.volume.toString());
      const price = parseFloat(record.originalPrice.toString());
      return sum + (volume * price);
    }, 0);
    
    console.log(`Raw payment (volume * price): £${rawPayment.toFixed(2)}`);
    
    console.log(`\nSample records (first 3):`);
    console.log(JSON.stringify(data.slice(0, 3), null, 2));
  } catch (error) {
    console.error('Error fetching data:', error);
  }
}

main();