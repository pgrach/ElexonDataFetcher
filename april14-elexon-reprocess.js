/**
 * Focused April 14 Elexon Reprocessing Script
 * 
 * This script performs a single-purpose reprocessing of April 14, 2025 data,
 * focusing only on maximizing data capture from Elexon with minimal changes.
 * 
 * Usage: node april14-elexon-reprocess.js
 */

import pg from 'pg';
import https from 'https';
import fs from 'fs';

const { Pool } = pg;

// Constants
const TARGET_DATE = '2025-04-14';
const API_KEY = process.env.ELEXON_API_KEY;
const LOG_FILE = `./logs/elexon_reprocess_${TARGET_DATE.replace(/-/g, '')}_${new Date().toISOString().replace(/:/g, '-')}.log`;

// Configure the database connection
const pool = new Pool({
  connectionString: process.env.DATABASE_URL
});

// Set up logging
if (!fs.existsSync('./logs')) {
  fs.mkdirSync('./logs', { recursive: true });
}
fs.writeFileSync(LOG_FILE, `=== Elexon Reprocessing Log for ${TARGET_DATE} ===\n`);

const log = (message) => {
  const timestamp = new Date().toISOString();
  const formattedMessage = `[${timestamp}] ${message}`;
  console.log(formattedMessage);
  fs.appendFileSync(LOG_FILE, formattedMessage + '\n');
};

// Fetch data from Elexon with proper error handling and retries
async function fetchElexonData(date, period, retries = 3) {
  return new Promise((resolve, reject) => {
    const url = `https://api.bmreports.com/BMRS/B1610/v2?APIKey=${API_KEY}&SettlementDate=${date}&Period=${period}&ServiceType=xml`;
    
    log(`Fetching data for ${date} period ${period} from Elexon: ${url}`);
    
    const request = https.get(url, (response) => {
      if (response.statusCode !== 200) {
        const error = new Error(`API returned status code ${response.statusCode}`);
        error.statusCode = response.statusCode;
        return reject(error);
      }
      
      let data = '';
      response.on('data', chunk => data += chunk);
      response.on('end', () => {
        try {
          // Parse the XML data using regex for simplicity in this script
          const records = parseElexonXML(data);
          log(`Successfully retrieved ${records.length} records for period ${period}`);
          resolve(records);
        } catch (err) {
          reject(new Error(`Failed to parse response: ${err.message}`));
        }
      });
    });
    
    request.on('error', (err) => {
      reject(new Error(`Request failed: ${err.message}`));
    });
    
    request.end();
  }).catch(async (error) => {
    if (retries > 0) {
      log(`Error fetching data (${retries} retries left): ${error.message}`);
      await new Promise(resolve => setTimeout(resolve, 2000)); // Wait before retrying
      return fetchElexonData(date, period, retries - 1);
    }
    throw error;
  });
}

// Simple XML parser for Elexon data
function parseElexonXML(xmlData) {
  const records = [];
  
  // Extract all B1610 items
  const itemRegex = /<item>[\s\S]*?<\/item>/g;
  const items = xmlData.match(itemRegex) || [];
  
  for (const item of items) {
    try {
      // Extract fields
      const bmuIdMatch = item.match(/<bm_unit_id>(.*?)<\/bm_unit_id>/);
      const volumeMatch = item.match(/<volume>(.*?)<\/volume>/);
      const priceMatch = item.match(/<price>(.*?)<\/price>/);
      const acceptFlagMatch = item.match(/<acceptance_number>(.*?)<\/acceptance_number>/);
      const soFlagMatch = item.match(/<so_flag>(.*?)<\/so_flag>/);
      const cadlFlagMatch = item.match(/<cadl_flag>(.*?)<\/cadl_flag>/);
      
      if (bmuIdMatch && volumeMatch && priceMatch) {
        const record = {
          id: bmuIdMatch[1],
          volume: parseFloat(volumeMatch[1]),
          originalPrice: parseFloat(priceMatch[1]),
          finalPrice: parseFloat(priceMatch[1]), // Same as original for now
          soFlag: soFlagMatch ? soFlagMatch[1] === 'Y' : false,
          cadlFlag: cadlFlagMatch ? cadlFlagMatch[1] === 'Y' : false,
          leadPartyName: 'Unknown' // Default value
        };
        
        // Only include curtailment records (negative volume with flags)
        if (record.volume < 0 && (record.soFlag || record.cadlFlag)) {
          records.push(record);
        }
      }
    } catch (err) {
      log(`Error parsing item: ${err.message}`);
    }
  }
  
  return records;
}

// Process and store curtailment records for all periods in a day
async function processFullDay() {
  try {
    // Connect to database
    const client = await pool.connect();
    log('Connected to database');
    
    // Step 1: Clear existing data for the target date
    await client.query('DELETE FROM curtailment_records WHERE settlement_date = $1', [TARGET_DATE]);
    log(`Removed existing curtailment records for ${TARGET_DATE}`);
    
    // Step 2: Process all 48 settlement periods
    let totalRecords = 0;
    let totalVolume = 0;
    let totalPayment = 0;
    const processedPeriods = new Set();
    
    // Process periods that have historically shown curtailment first (for efficiency)
    const priorityPeriods = [1, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36];
    const remainingPeriods = Array.from({ length: 48 }, (_, i) => i + 1).filter(p => !priorityPeriods.includes(p));
    const allPeriods = [...priorityPeriods, ...remainingPeriods];
    
    for (const period of allPeriods) {
      try {
        const records = await fetchElexonData(TARGET_DATE, period);
        
        if (records.length > 0) {
          processedPeriods.add(period);
          log(`Period ${period}: Found ${records.length} curtailment records`);
          
          let periodVolume = 0;
          let periodPayment = 0;
          
          // Insert each record
          for (const record of records) {
            const absVolume = Math.abs(record.volume);
            const payment = absVolume * record.originalPrice;
            
            periodVolume += absVolume;
            periodPayment += payment;
            
            await client.query(
              `INSERT INTO curtailment_records 
               (settlement_date, settlement_period, farm_id, lead_party_name, 
                volume, payment, original_price, final_price, so_flag, cadl_flag)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
              [
                TARGET_DATE, 
                period, 
                record.id, 
                record.leadPartyName, 
                record.volume.toString(), 
                payment.toString(),
                record.originalPrice.toString(),
                record.finalPrice.toString(),
                record.soFlag,
                record.cadlFlag
              ]
            );
            
            totalRecords++;
          }
          
          totalVolume += periodVolume;
          totalPayment += periodPayment;
          
          log(`Period ${period}: Stored ${records.length} records (${periodVolume.toFixed(2)} MWh, £${periodPayment.toFixed(2)})`);
        } else {
          log(`Period ${period}: No curtailment records found`);
        }
      } catch (error) {
        log(`Error processing period ${period}: ${error.message}`);
      }
    }
    
    log(`Processed ${totalRecords} total curtailment records across ${processedPeriods.size} periods`);
    log(`Total volume: ${totalVolume.toFixed(2)} MWh`);
    log(`Total payment: £${totalPayment.toFixed(2)}`);
    
    // Step 3: Update daily summary
    if (totalRecords > 0) {
      await client.query(
        `INSERT INTO daily_summaries 
         (summary_date, total_curtailed_energy, total_payment, last_updated) 
         VALUES ($1, $2, $3, NOW())
         ON CONFLICT (summary_date) 
         DO UPDATE SET 
           total_curtailed_energy = $2, 
           total_payment = $3, 
           last_updated = NOW()`,
        [TARGET_DATE, totalVolume.toString(), (-totalPayment).toString()]
      );
      log(`Updated daily summary for ${TARGET_DATE}`);
      
      // Step 4: Update monthly summary
      const yearMonth = TARGET_DATE.substring(0, 7);
      const monthlySummary = await client.query(
        `SELECT SUM(total_curtailed_energy::numeric) as energy, SUM(total_payment::numeric) as payment
         FROM daily_summaries 
         WHERE date_trunc('month', summary_date::date) = date_trunc('month', $1::date)`,
        [TARGET_DATE]
      );
      
      if (monthlySummary.rows.length > 0 && monthlySummary.rows[0].energy) {
        await client.query(
          `INSERT INTO monthly_summaries 
           (year_month, total_curtailed_energy, total_payment, updated_at) 
           VALUES ($1, $2, $3, NOW())
           ON CONFLICT (year_month) 
           DO UPDATE SET 
             total_curtailed_energy = $2, 
             total_payment = $3, 
             updated_at = NOW()`,
          [yearMonth, monthlySummary.rows[0].energy, monthlySummary.rows[0].payment]
        );
        log(`Updated monthly summary for ${yearMonth}`);
      }
    }
    
    // Final verification
    const verification = await client.query(
      `SELECT COUNT(*) as record_count, 
              COUNT(DISTINCT settlement_period) as period_count,
              SUM(ABS(volume::numeric)) as total_volume, 
              SUM(payment::numeric) as total_payment
       FROM curtailment_records 
       WHERE settlement_date = $1`,
      [TARGET_DATE]
    );
    
    log('\nFinal Database Verification:');
    log(`Records: ${verification.rows[0].record_count}`);
    log(`Periods: ${verification.rows[0].period_count}`);
    log(`Total Volume: ${parseFloat(verification.rows[0].total_volume).toFixed(2)} MWh`);
    log(`Total Payment: £${parseFloat(verification.rows[0].total_payment).toFixed(2)}`);
    
    // Release the client back to the pool
    client.release();
    log('\nProcessing complete');
    
  } catch (error) {
    log(`Fatal error: ${error.message}\n${error.stack}`);
    process.exit(1);
  }
}

// Run the processing
processFullDay().then(() => {
  log('Script execution completed');
  process.exit(0);
}).catch(error => {
  log(`Script execution failed: ${error.message}\n${error.stack}`);
  process.exit(1);
});