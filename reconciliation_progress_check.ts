/**
 * Reconciliation Progress Check Tool
 * 
 * This script provides a quick overview of the current reconciliation status,
 * showing completion percentages for specific dates and overall progress.
 */

import pg from 'pg';
const { Pool } = pg;

// Connection pool
const pool = new Pool({
  connectionString: process.env.DATABASE_URL
});

// Format number with commas
function formatNumber(num: number): string {
  return num.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

// Format percentage with two decimal places
function formatPercentage(value: number): string {
  return (value * 100).toFixed(2) + '%';
}

/**
 * Get overall status of bitcoin calculations vs curtailment records
 */
async function getOverallStatus() {
  const client = await pool.connect();
  try {
    const query = `
      WITH curtailment_stats AS (
        SELECT 
          COUNT(*) * 3 AS expected_count -- multiply by 3 because we need one calculation for each miner model
        FROM 
          curtailment_records
      ),
      calculation_stats AS (
        SELECT 
          COUNT(*) AS actual_count
        FROM 
          historical_bitcoin_calculations
      )
      SELECT 
        cs.expected_count,
        ca.actual_count,
        CASE 
          WHEN cs.expected_count > 0 THEN 
            ROUND((ca.actual_count::numeric / cs.expected_count::numeric), 4)
          ELSE 0 
        END AS completion_percentage
      FROM 
        curtailment_stats cs,
        calculation_stats ca;
    `;
    
    const result = await client.query(query);
    
    if (result.rows.length > 0) {
      const stats = result.rows[0];
      console.log('=== Overall Reconciliation Status ===');
      console.log(`Expected calculations: ${formatNumber(stats.expected_count)}`);
      console.log(`Actual calculations: ${formatNumber(stats.actual_count)}`);
      console.log(`Completion percentage: ${formatPercentage(stats.completion_percentage)}`);
      console.log('');
    }
  } catch (error) {
    console.error('Error getting overall status:', error);
  } finally {
    client.release();
  }
}

/**
 * Get reconciliation status for a specific date
 */
async function getDateStatus(date: string) {
  const client = await pool.connect();
  try {
    const query = `
      WITH date_curtailment_stats AS (
        SELECT 
          COUNT(*) * 3 AS expected_count -- multiply by 3 because we need one calculation for each miner model
        FROM 
          curtailment_records
        WHERE 
          settlement_date = $1
      ),
      date_calculation_stats AS (
        SELECT 
          COUNT(*) AS actual_count
        FROM 
          historical_bitcoin_calculations
        WHERE 
          settlement_date = $1
      )
      SELECT 
        dcs.expected_count,
        dca.actual_count,
        CASE 
          WHEN dcs.expected_count > 0 THEN 
            ROUND((dca.actual_count::numeric / dcs.expected_count::numeric), 4)
          ELSE 0 
        END AS completion_percentage
      FROM 
        date_curtailment_stats dcs,
        date_calculation_stats dca;
    `;
    
    const result = await client.query(query, [date]);
    
    if (result.rows.length > 0) {
      const stats = result.rows[0];
      console.log(`=== Reconciliation Status for ${date} ===`);
      console.log(`Expected calculations: ${stats.expected_count}`);
      console.log(`Actual calculations: ${stats.actual_count}`);
      console.log(`Completion percentage: ${formatPercentage(stats.completion_percentage)}`);
      console.log('');
    }
  } catch (error) {
    console.error(`Error getting status for ${date}:`, error);
  } finally {
    client.release();
  }
}

/**
 * Get top dates with missing calculations
 */
async function getTopMissingDates(limit: number = 10) {
  const client = await pool.connect();
  try {
    const query = `
      WITH date_stats AS (
        SELECT 
          cr.settlement_date,
          COUNT(*) * 3 AS expected_count,
          (
            SELECT COUNT(*) 
            FROM historical_bitcoin_calculations hbc
            WHERE hbc.settlement_date = cr.settlement_date
          ) AS actual_count
        FROM 
          curtailment_records cr
        GROUP BY 
          cr.settlement_date
      )
      SELECT 
        settlement_date,
        expected_count,
        actual_count,
        CASE 
          WHEN expected_count > 0 THEN 
            ROUND((actual_count::numeric / expected_count::numeric), 4)
          ELSE 0 
        END AS completion_percentage,
        (expected_count - actual_count) AS missing_count
      FROM 
        date_stats
      WHERE 
        expected_count > actual_count
      ORDER BY 
        missing_count DESC
      LIMIT $1;
    `;
    
    const result = await client.query(query, [limit]);
    
    if (result.rows.length > 0) {
      console.log(`=== Top ${limit} Dates with Missing Calculations ===`);
      console.table(result.rows.map(row => ({
        date: row.settlement_date.toISOString().split('T')[0],
        expected: row.expected_count,
        actual: row.actual_count,
        completion: formatPercentage(row.completion_percentage),
        missing: row.missing_count
      })));
      console.log('');
    } else {
      console.log('No dates with missing calculations found');
      console.log('');
    }
  } catch (error) {
    console.error('Error getting top missing dates:', error);
  } finally {
    client.release();
  }
}

/**
 * Get December 2023 reconciliation status
 */
async function getDecemberStatus() {
  const client = await pool.connect();
  try {
    const query = `
      WITH december_stats AS (
        SELECT 
          cr.settlement_date,
          COUNT(*) * 3 AS expected_count,
          (
            SELECT COUNT(*) 
            FROM historical_bitcoin_calculations hbc
            WHERE hbc.settlement_date = cr.settlement_date
          ) AS actual_count
        FROM 
          curtailment_records cr
        WHERE 
          cr.settlement_date >= '2023-12-01' AND cr.settlement_date <= '2023-12-31'
        GROUP BY 
          cr.settlement_date
      )
      SELECT 
        settlement_date,
        expected_count,
        actual_count,
        CASE 
          WHEN expected_count > 0 THEN 
            ROUND((actual_count::numeric / expected_count::numeric), 4)
          ELSE 0 
        END AS completion_percentage,
        (expected_count - actual_count) AS missing_count
      FROM 
        december_stats
      ORDER BY 
        settlement_date;
    `;
    
    const result = await client.query(query);
    
    if (result.rows.length > 0) {
      console.log('=== December 2023 Reconciliation Status ===');
      
      // Summary stats for December
      const totalExpected = result.rows.reduce((sum, row) => sum + parseInt(row.expected_count), 0);
      const totalActual = result.rows.reduce((sum, row) => sum + parseInt(row.actual_count), 0);
      const overallCompletion = totalExpected > 0 ? totalActual / totalExpected : 0;
      
      console.log(`Overall December completion: ${formatPercentage(overallCompletion)} (${formatNumber(totalActual)} / ${formatNumber(totalExpected)})`);
      
      // Show summary for each date
      console.table(result.rows.map(row => ({
        date: row.settlement_date.toISOString().split('T')[0],
        expected: row.expected_count,
        actual: row.actual_count,
        completion: formatPercentage(row.completion_percentage),
        missing: row.missing_count
      })));
      console.log('');
    } else {
      console.log('No December data found');
      console.log('');
    }
  } catch (error) {
    console.error('Error getting December status:', error);
  } finally {
    client.release();
  }
}

/**
 * Main function
 */
async function main() {
  try {
    // Get command line arguments
    const args = process.argv.slice(2);
    
    if (args.length === 0 || args[0] === 'all' || args[0] === 'status') {
      await getOverallStatus();
      await getTopMissingDates(10);
    }
    
    if (args.length > 0) {
      if (args[0] === 'date' && args.length > 1) {
        await getDateStatus(args[1]);
      } else if (args[0] === 'december') {
        await getDecemberStatus();
      } else if (args[0] === 'top' && args.length > 1) {
        await getTopMissingDates(parseInt(args[1]));
      }
    }
    
  } catch (error) {
    console.error('Fatal error:', error);
    process.exit(1);
  } finally {
    await pool.end();
  }
}

// Run the script
main()
  .then(() => {
    console.log('Progress check completed.');
    process.exit(0);
  })
  .catch(error => {
    console.error('Fatal error:', error);
    process.exit(1);
  });