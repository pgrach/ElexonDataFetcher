/**
 * API Status Check Script
 * 
 * A simple script to verify connectivity with the Elexon API.
 */

import fetch from 'node-fetch';
import { setTimeout } from 'timers/promises';

async function checkElexonApiStatus() {
  console.log("=== Elexon API Status Check ===");
  
  const targetDate = '2025-05-09'; // Today
  const testPeriod = 1;
  
  // Test URLs to check
  const urls = [
    // BMRS BOD API endpoint
    `https://data.bmreports.com/bmrs/api/v1/datasets/BOD/${targetDate}/${testPeriod}`,
    
    // BMRS generation data endpoint
    `https://data.bmreports.com/bmrs/api/v1/datasets/WINDFOR/${targetDate}`,
    
    // BMRS main domain
    `https://data.bmreports.com/bmrs/api/v1/status`
  ];
  
  // Test each URL
  for (const url of urls) {
    try {
      console.log(`\nTesting connection to: ${url}`);
      
      const start = Date.now();
      const response = await fetch(url, {
        method: 'GET',
        headers: {
          'Accept': 'application/json'
        },
        timeout: 10000 // 10 second timeout
      });
      const elapsed = Date.now() - start;
      
      if (response.ok) {
        console.log(`✅ Connection successful (${elapsed}ms)`);
        console.log(`Status: ${response.status} ${response.statusText}`);
        
        // Try to parse response as JSON
        try {
          const data = await response.json();
          console.log('Response data sample:', JSON.stringify(data).substring(0, 200) + '...');
        } catch (e) {
          console.log('Could not parse response as JSON:', e.message);
        }
      } else {
        console.log(`❌ Connection failed with status ${response.status} ${response.statusText} (${elapsed}ms)`);
        try {
          const text = await response.text();
          console.log('Error response:', text.substring(0, 200));
        } catch (e) {
          console.log('Could not read error response:', e.message);
        }
      }
    } catch (error) {
      console.log(`❌ Connection error: ${error.message}`);
      
      // Try DNS lookup
      try {
        const { lookup } = await import('dns/promises');
        console.log('Performing DNS lookup for data.bmreports.com...');
        const hostname = 'data.bmreports.com';
        
        try {
          const result = await lookup(hostname);
          console.log(`DNS lookup success: ${hostname} resolves to ${result.address}`);
        } catch (dnsError) {
          console.log(`DNS lookup failed: ${dnsError.message}`);
        }
      } catch (importError) {
        console.log('Could not perform DNS lookup:', importError.message);
      }
    }
    
    // Short pause between requests
    await setTimeout(1000);
  }
  
  console.log("\n=== Status Check Complete ===");
}

checkElexonApiStatus();