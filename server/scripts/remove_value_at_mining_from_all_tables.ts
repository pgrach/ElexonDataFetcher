/**
 * Remove value_at_mining column from all Bitcoin summary tables
 * 
 * This script removes the value_at_mining column from:
 * - bitcoin_monthly_summaries
 * - bitcoin_yearly_summaries
 */

import { db } from "@db";
import { sql } from "drizzle-orm";

async function removeColumnFromTable(tableName: string) {
  try {
    console.log(`\n=== Processing table: ${tableName} ===`);
    
    // Step 1: First make the column nullable to avoid constraint issues
    console.log(`Step 1: Making value_at_mining column nullable in ${tableName}...`);
    await db.execute(sql`
      ALTER TABLE ${sql.identifier(tableName)} 
      ALTER COLUMN value_at_mining DROP NOT NULL;
    `);
    console.log(`✓ Column constraints removed from ${tableName}`);
    
    // Step 2: Drop the column
    console.log(`\nStep 2: Dropping value_at_mining column from ${tableName}...`);
    await db.execute(sql`
      ALTER TABLE ${sql.identifier(tableName)} 
      DROP COLUMN value_at_mining;
    `);
    console.log(`✓ Column dropped successfully from ${tableName}`);
    
    // Step 3: Verify the changes
    console.log(`\nStep 3: Verifying table structure for ${tableName}...`);
    const columnCheck = await db.execute(sql`
      SELECT column_name 
      FROM information_schema.columns 
      WHERE table_name = ${tableName} 
      AND column_name = 'value_at_mining';
    `);
    
    if (columnCheck.rows && columnCheck.rows.length === 0) {
      console.log(`✓ Verification successful: value_at_mining column no longer exists in ${tableName}`);
    } else {
      console.log(`! Verification failed: value_at_mining column still exists in ${tableName}`);
    }
    
    return true;
  } catch (error) {
    console.error(`Error processing ${tableName}:`, error);
    return false;
  }
}

async function main() {
  try {
    console.log(`\n=== Starting Migration: Remove value_at_mining from all Bitcoin tables ===\n`);
    
    const tables = [
      'bitcoin_monthly_summaries',
      'bitcoin_yearly_summaries'
    ];
    
    let allSuccessful = true;
    
    for (const tableName of tables) {
      const success = await removeColumnFromTable(tableName);
      if (!success) {
        allSuccessful = false;
      }
    }
    
    if (allSuccessful) {
      console.log(`\n=== Migration Completed Successfully for All Tables ===\n`);
    } else {
      console.log(`\n=== Migration Completed with Some Errors ===\n`);
    }
    
    process.exit(0);
  } catch (error) {
    console.error('Migration failed:', error);
    process.exit(1);
  }
}

main();