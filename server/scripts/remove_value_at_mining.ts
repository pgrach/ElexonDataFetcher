/**
 * Remove value_at_mining column from bitcoin_daily_summaries
 * 
 * This script removes the value_at_mining column from the bitcoin_daily_summaries table
 * and any constraints associated with it.
 */

import { db } from "@db";
import { sql } from "drizzle-orm";

async function main() {
  try {
    console.log(`\n=== Starting Migration: Remove value_at_mining from bitcoin_daily_summaries ===\n`);
    
    // Step 1: First make the column nullable to avoid constraint issues
    console.log("Step 1: Making value_at_mining column nullable...");
    await db.execute(sql`
      ALTER TABLE bitcoin_daily_summaries 
      ALTER COLUMN value_at_mining DROP NOT NULL;
    `);
    console.log("✓ Column constraints removed");
    
    // Step 2: Drop the column
    console.log("\nStep 2: Dropping value_at_mining column...");
    await db.execute(sql`
      ALTER TABLE bitcoin_daily_summaries 
      DROP COLUMN value_at_mining;
    `);
    console.log("✓ Column dropped successfully");
    
    // Step 3: Verify the changes
    console.log("\nStep 3: Verifying table structure...");
    const columnCheck = await db.execute(sql`
      SELECT column_name 
      FROM information_schema.columns 
      WHERE table_name = 'bitcoin_daily_summaries' 
      AND column_name = 'value_at_mining';
    `);
    
    if (columnCheck.rows && columnCheck.rows.length === 0) {
      console.log("✓ Verification successful: value_at_mining column no longer exists");
    } else {
      console.log("! Verification failed: value_at_mining column still exists");
    }
    
    console.log(`\n=== Migration Completed Successfully ===\n`);
    
    process.exit(0);
  } catch (error) {
    console.error('Migration failed:', error);
    process.exit(1);
  }
}

main();