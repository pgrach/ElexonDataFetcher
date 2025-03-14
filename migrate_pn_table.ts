/**
 * Migrate Physical Notifications Table
 * 
 * This script creates the physical_notifications table in the database.
 */

import { db } from "./db";
import { sql } from "drizzle-orm";

async function createPhysicalNotificationsTable() {
  try {
    console.log("Creating physical_notifications table...");
    
    // Check if table already exists
    const tableExists = await checkTableExists('physical_notifications');
    
    if (tableExists) {
      console.log("Table physical_notifications already exists, skipping creation.");
      return;
    }
    
    // Create the table
    await db.execute(sql`
      CREATE TABLE IF NOT EXISTS physical_notifications (
        id SERIAL PRIMARY KEY,
        settlement_date DATE NOT NULL,
        settlement_period INTEGER NOT NULL,
        time_from TIMESTAMP NOT NULL,
        time_to TIMESTAMP NOT NULL,
        level_from NUMERIC NOT NULL,
        level_to NUMERIC NOT NULL,
        national_grid_bm_unit TEXT,
        bm_unit TEXT NOT NULL,
        lead_party_name TEXT,
        created_at TIMESTAMP DEFAULT NOW(),
        
        -- Create index for faster queries
        UNIQUE (settlement_date, settlement_period, bm_unit)
      );
      
      -- Create indices for common query patterns
      CREATE INDEX IF NOT EXISTS pn_date_idx ON physical_notifications (settlement_date);
      CREATE INDEX IF NOT EXISTS pn_bmu_idx ON physical_notifications (bm_unit);
    `);
    
    console.log("Successfully created physical_notifications table");
  } catch (error) {
    console.error("Error creating physical_notifications table:", error);
    throw error;
  }
}

async function checkTableExists(tableName: string): Promise<boolean> {
  try {
    const result = await db.execute(sql`
      SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name = ${tableName}
      );
    `);
    
    return result.rows[0].exists;
  } catch (error) {
    console.error(`Error checking if table ${tableName} exists:`, error);
    return false;
  }
}

async function runMigration() {
  console.log("Running Physical Notifications table migration...");
  
  try {
    await createPhysicalNotificationsTable();
    console.log("Migration completed successfully");
  } catch (error) {
    console.error("Migration failed:", error);
    process.exit(1);
  }
}

// Run the migration
runMigration().then(() => {
  console.log("Migration script completed");
  process.exit(0);
});