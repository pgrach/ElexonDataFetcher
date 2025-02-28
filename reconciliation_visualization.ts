/**
 * Reconciliation Visualization Tool
 * 
 * This script generates a visual representation of the reconciliation progress
 * to help identify patterns and priority areas for further reconciliation work.
 */

import pg from 'pg';
import fs from 'fs/promises';
import { format } from 'date-fns';

// Create database connection
const dbUrl = process.env.DATABASE_URL;
if (!dbUrl) {
  console.error('DATABASE_URL environment variable is not set');
  process.exit(1);
}

const pool = new pg.Pool({
  connectionString: dbUrl,
});

async function generateMonthlyHeatmap() {
  const client = await pool.connect();
  
  try {
    // Get monthly reconciliation statistics
    const query = `
      WITH monthly_stats AS (
        SELECT 
          TO_CHAR(cr.settlement_date, 'YYYY-MM') AS year_month,
          COUNT(DISTINCT (cr.settlement_date, cr.settlement_period, cr.farm_id)) * 3 AS expected_count,
          COUNT(DISTINCT (hbc.settlement_date, hbc.settlement_period, hbc.farm_id, hbc.miner_model)) AS actual_count
        FROM 
          curtailment_records cr
        LEFT JOIN 
          historical_bitcoin_calculations hbc 
          ON cr.settlement_date = hbc.settlement_date
          AND cr.settlement_period = hbc.settlement_period
          AND cr.farm_id = hbc.farm_id
        GROUP BY 
          TO_CHAR(cr.settlement_date, 'YYYY-MM')
      )
      SELECT 
        year_month,
        expected_count AS expected_calculations,
        actual_count AS actual_calculations,
        CASE 
          WHEN expected_count = 0 THEN 0
          ELSE ROUND((actual_count::numeric / expected_count) * 100, 2)
        END AS completion_percentage
      FROM 
        monthly_stats
      ORDER BY 
        year_month;
    `;
    
    const result = await client.query(query);
    
    // Format the months summary
    const monthsData: Record<string, {
      expectedCalculations: number;
      actualCalculations: number;
      completionPercentage: number;
      visualRepresentation: string;
    }> = {};
    
    result.rows.forEach((row) => {
      const percentage = parseFloat(row.completion_percentage);
      
      // Generate visual representation (ASCII heatmap)
      let visual = '';
      const blocks = Math.round(percentage / 10);
      
      for (let i = 0; i < 10; i++) {
        if (i < blocks) {
          visual += '█'; // Full block for completed percentage
        } else {
          visual += '░'; // Light block for incomplete percentage
        }
      }
      
      monthsData[row.year_month] = {
        expectedCalculations: parseInt(row.expected_calculations),
        actualCalculations: parseInt(row.actual_calculations),
        completionPercentage: percentage,
        visualRepresentation: visual
      };
    });
    
    return monthsData;
  } finally {
    client.release();
  }
}

async function generateDailyHeatmap(yearMonth: string) {
  const client = await pool.connect();
  
  try {
    // Get daily reconciliation statistics for the month
    const query = `
      WITH date_reconciliation AS (
        SELECT 
          cr.settlement_date, 
          COUNT(DISTINCT (cr.settlement_period, cr.farm_id)) * 3 AS expected_count,
          COUNT(DISTINCT (hbc.settlement_period, hbc.farm_id, hbc.miner_model)) AS actual_count
        FROM 
          curtailment_records cr
        LEFT JOIN 
          historical_bitcoin_calculations hbc ON cr.settlement_date = hbc.settlement_date
          AND cr.settlement_period = hbc.settlement_period
          AND cr.farm_id = hbc.farm_id
        WHERE 
          TO_CHAR(cr.settlement_date, 'YYYY-MM') = $1
        GROUP BY 
          cr.settlement_date
      )
      SELECT 
        settlement_date::text as date,
        expected_count,
        actual_count,
        CASE 
          WHEN expected_count = 0 THEN 0
          ELSE ROUND((actual_count::numeric / expected_count) * 100, 2)
        END AS completion_percentage
      FROM 
        date_reconciliation
      ORDER BY 
        settlement_date;
    `;
    
    const result = await client.query(query, [yearMonth]);
    
    // Format the days summary
    const daysData: Record<string, {
      expectedCalculations: number;
      actualCalculations: number;
      completionPercentage: number;
      visualRepresentation: string;
    }> = {};
    
    result.rows.forEach((row) => {
      const percentage = parseFloat(row.completion_percentage);
      
      // Generate visual representation (ASCII heatmap)
      let visual = '';
      const blocks = Math.round(percentage / 10);
      
      for (let i = 0; i < 10; i++) {
        if (i < blocks) {
          visual += '█'; // Full block for completed percentage
        } else {
          visual += '░'; // Light block for incomplete percentage
        }
      }
      
      daysData[row.date] = {
        expectedCalculations: parseInt(row.expected_count),
        actualCalculations: parseInt(row.actual_count),
        completionPercentage: percentage,
        visualRepresentation: visual
      };
    });
    
    return daysData;
  } finally {
    client.release();
  }
}

function formatPercentage(value: number): string {
  return `${value.toFixed(2)}%`;
}

function formatNumber(value: number): string {
  return value.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ',');
}

async function generateVisualizationReport() {
  console.log("=== Generating Reconciliation Visualization ===\n");
  
  try {
    // Generate monthly report
    const monthlyData = await generateMonthlyHeatmap();
    
    let report = '# Reconciliation Progress Visualization\n\n';
    report += `*Generated on: ${format(new Date(), 'yyyy-MM-dd HH:mm:ss')}*\n\n`;
    
    // Overall summary
    report += '## Monthly Progress Heatmap\n\n';
    report += '```\n';
    report += 'Month       | Progress  | Percentage | Calculations\n';
    report += '------------|-----------|------------|-------------\n';
    
    Object.entries(monthlyData).forEach(([month, data]) => {
      report += `${month} | ${data.visualRepresentation} | ${formatPercentage(data.completionPercentage)} | ${formatNumber(data.actualCalculations)}/${formatNumber(data.expectedCalculations)}\n`;
    });
    
    report += '```\n\n';
    
    // Generate daily report for December 2023
    const decemberData = await generateDailyHeatmap('2023-12');
    
    report += '## December 2023 Daily Progress\n\n';
    report += '*Priority month based on missing calculations*\n\n';
    report += '```\n';
    report += 'Date       | Progress  | Percentage | Calculations\n';
    report += '------------|-----------|------------|-------------\n';
    
    Object.entries(decemberData).forEach(([date, data]) => {
      report += `${date} | ${data.visualRepresentation} | ${formatPercentage(data.completionPercentage)} | ${formatNumber(data.actualCalculations)}/${formatNumber(data.expectedCalculations)}\n`;
    });
    
    report += '```\n\n';
    
    // Write to a file
    await fs.writeFile('reconciliation_visualization.md', report);
    console.log('Visualization report generated: reconciliation_visualization.md');
    
  } catch (error) {
    console.error("Error generating visualization:", error);
  } finally {
    // Close the pool
    await pool.end();
  }
}

// Run the main function if script is executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
  generateVisualizationReport()
    .then(() => {
      console.log("\n=== Visualization Generation Complete ===");
      process.exit(0);
    })
    .catch(error => {
      console.error("Fatal error:", error);
      process.exit(1);
    });
}