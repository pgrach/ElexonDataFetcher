/**
 * This script verifies API access for daily summaries of the reconciled data
 */

import axios from 'axios';
import { format, eachDayOfInterval, parseISO } from 'date-fns';

// Using the Replit.nix URL
const BASE_URL = 'https://fb6b1d46-0d69-4d3d-bcc5-8d0474975017-00-2eq1h79tpnfmh.spock.replit.dev/api';
const START_DATE = '2025-02-28';
const END_DATE = '2025-03-03';

async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function checkDailyData() {
  try {
    console.log(`\n=== Verifying API Access for ${START_DATE} to ${END_DATE} ===`);
    
    const days = eachDayOfInterval({
      start: parseISO(START_DATE),
      end: parseISO(END_DATE)
    });

    for (const day of days) {
      const dateStr = format(day, 'yyyy-MM-dd');
      console.log(`\n--- Checking data for ${dateStr} ---`);
      
      try {
        // Check daily summary
        const dailySummary = await axios.get(`${BASE_URL}/summary/daily/${dateStr}`);
        console.log(`Daily Summary: ${dailySummary.data.totalCurtailedEnergy.toFixed(2)} MWh, £${dailySummary.data.totalPayment.toFixed(2)}`);
        
        // Check bitcoin potential
        const bitcoinPotential = await axios.get(`${BASE_URL}/curtailment/mining-potential`, {
          params: {
            date: dateStr,
            minerModel: 'S19J_PRO',
            energy: dailySummary.data.totalCurtailedEnergy
          }
        });
        console.log(`Bitcoin Potential: ${bitcoinPotential.data.bitcoinMined.toFixed(4)} BTC, £${bitcoinPotential.data.valueAtCurrentPrice.toFixed(2)}`);
        
        // Check hourly data
        const hourlyData = await axios.get(`${BASE_URL}/curtailment/hourly/${dateStr}`);
        console.log(`Hourly Data: ${hourlyData.data.length} hours of data available`);
        
        console.log(`✅ API access successful for ${dateStr}`);
      } catch (error) {
        console.error(`❌ API access failed for ${dateStr}:`, error.message);
      }
      
      // Add a delay between requests
      await delay(1000);
    }
    
    console.log(`\n=== API Access Verification Complete ===`);
  } catch (error) {
    console.error('Error during verification:', error);
    process.exit(1);
  }
}

checkDailyData();