/**
 * Update Historical Bitcoin Calculations Table Constraints
 * 
 * This script runs a migration to update the unique constraint on
 * the historical_bitcoin_calculations table to include the curtailment_id column.
 * This is necessary to support the proper 1:1 relationship between curtailment records
 * and bitcoin calculations, as we need one calculation per curtailment record
 * per miner model.
 * 
 * Usage:
 *   npx tsx migrate_constraints.ts
 */

import { drizzle } from "drizzle-orm/postgres-js";
import { sql } from "drizzle-orm";
import postgres from "postgres";
import * as fs from "fs";
import * as path from "path";

// Database connection
const connectionString = process.env.DATABASE_URL || "";
const client = postgres(connectionString);
const db = drizzle(client);

async function run() {
  console.log("Running migration to update constraints...");
  
  try {
    // Read the migration file
    const migrationPath = path.join(__dirname, "migrations", "add_curtailment_id_to_unique_constraint.sql");
    const migrationSql = fs.readFileSync(migrationPath, "utf-8");
    
    // Run the migration
    await db.execute(sql.raw(migrationSql));
    
    console.log("Migration completed successfully!");
  } catch (error) {
    console.error("Migration failed:", error);
    process.exit(1);
  } finally {
    // Close the database connection
    await client.end();
  }
}

// Run the migration
run().catch(console.error);