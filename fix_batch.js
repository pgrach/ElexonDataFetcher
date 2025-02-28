/**
 * Simple script to run Bitcoin calculation reconciliation for a batch of specific dates
 * This uses the main fix_all_bitcoin_calculations.ts script functionality
 */

const { execSync } = require('child_process');

// Configuration
const SPECIFIC_DATES = [
  '2023-01-15', // Completely missing
  '2022-05-10', // Incomplete
  '2024-09-15', // Incomplete
  '2025-01-20'  // Incomplete recent
];

// Database query to check status for these specific dates
const CHECK_QUERY = `
WITH curtailment_dates AS (
  SELECT 
    settlement_date::text as date,
    COUNT(*) as curtailment_count
  FROM curtailment_records
  WHERE settlement_date IN ('${SPECIFIC_DATES.join("','")}')
  GROUP BY settlement_date
),
bitcoin_dates AS (
  SELECT 
    settlement_date::text as date,
    miner_model,
    COUNT(*) as bitcoin_count
  FROM historical_bitcoin_calculations
  WHERE settlement_date IN ('${SPECIFIC_DATES.join("','")}')
  GROUP BY settlement_date, miner_model
)
SELECT 
  c.date,
  c.curtailment_count,
  jsonb_object_agg(b.miner_model, b.bitcoin_count) FILTER (WHERE b.miner_model IS NOT NULL) as model_counts,
  CASE
    WHEN MIN(b.bitcoin_count) = c.curtailment_count THEN 'Complete' 
    WHEN MIN(b.bitcoin_count) IS NULL THEN 'Missing'
    ELSE 'Incomplete'
  END as status
FROM curtailment_dates c
LEFT JOIN bitcoin_dates b ON c.date = b.date
GROUP BY c.date, c.curtailment_count
ORDER BY c.date;
`;

/**
 * Run SQL query via PostgreSQL client
 */
function runQuery(query) {
  try {
    // Use execSync to run the query via psql
    const output = execSync(`psql "$DATABASE_URL" -c "${query}"`).toString();
    return output;
  } catch (error) {
    console.error('Error running query:', error.message);
    return null;
  }
}

/**
 * Process a specific date using the fix_all_bitcoin_calculations.ts script
 */
function processSingleDate(date) {
  console.log(`\nProcessing date: ${date}`);
  try {
    // We'll call the fix_all_bitcoin_calculations.ts directly with a date parameter
    const command = `PROCESS_SINGLE_DATE=${date} node -r ts-node/register fix_all_bitcoin_calculations.ts`;
    execSync(command, { stdio: 'inherit' });
    return true;
  } catch (error) {
    console.error(`Error processing ${date}:`, error.message);
    return false;
  }
}

/**
 * Main function
 */
async function main() {
  console.log("=== Bitcoin Calculation Reconciliation for Specific Dates ===");
  console.log(`Dates to process: ${SPECIFIC_DATES.join(', ')}`);

  // Check initial status
  console.log("\nInitial status:");
  const initialStatus = runQuery(CHECK_QUERY);
  console.log(initialStatus);

  // Process each date
  console.log("\nProcessing dates:");
  for (const date of SPECIFIC_DATES) {
    processSingleDate(date);
  }

  // Check final status
  console.log("\nFinal status:");
  const finalStatus = runQuery(CHECK_QUERY);
  console.log(finalStatus);
  
  console.log("\n=== Processing Complete ===");
}

// Run the script
main().catch(console.error);