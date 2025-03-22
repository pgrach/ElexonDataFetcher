/**
 * Migration Script for Wind Generation Summary Tables
 * 
 * This script adds wind generation columns to the daily, monthly, and yearly summary tables
 * to incorporate wind generation data alongside curtailment data.
 */

import { db } from "./db";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { logger } from "./server/utils/logger";

// Get the current file's directory
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

async function runMigration() {
  try {
    logger.info("Starting wind generation summary tables migration", { module: "migration" });
    
    // Read and execute the SQL migration file
    const migrationFile = path.join(__dirname, "migrations", "20250322_add_wind_generation_to_summaries.sql");
    const sql = fs.readFileSync(migrationFile, "utf8");
    
    logger.info("Executing SQL statements to add wind generation columns to summary tables", { module: "migration" });
    await db.execute(sql);
    
    logger.info("Successfully added wind generation columns to summary tables", { module: "migration" });
    
    // Verify the columns exist
    const dailyColumns = await db.execute(`
      SELECT column_name 
      FROM information_schema.columns 
      WHERE table_name = 'daily_summaries' 
      AND column_name IN ('total_wind_generation', 'wind_onshore_generation', 'wind_offshore_generation')
    `);
    
    const monthlyColumns = await db.execute(`
      SELECT column_name 
      FROM information_schema.columns 
      WHERE table_name = 'monthly_summaries' 
      AND column_name IN ('total_wind_generation', 'wind_onshore_generation', 'wind_offshore_generation')
    `);
    
    const yearlyColumns = await db.execute(`
      SELECT column_name 
      FROM information_schema.columns 
      WHERE table_name = 'yearly_summaries' 
      AND column_name IN ('total_wind_generation', 'wind_onshore_generation', 'wind_offshore_generation')
    `);
    
    logger.info(`Verification complete:`, { 
      module: "migration",
      dailyColumns: dailyColumns.rows.length,
      monthlyColumns: monthlyColumns.rows.length,
      yearlyColumns: yearlyColumns.rows.length
    });
    
    console.log("Migration completed successfully!");
    process.exit(0);
  } catch (error) {
    logger.error(`Migration failed: ${error.message}`, { 
      module: "migration",
      error
    });
    console.error("Migration failed:", error);
    process.exit(1);
  }
}

runMigration().catch(error => {
  console.error("Unhandled error during migration:", error);
  process.exit(1);
});