/**
 * Migration Runner for Farm ID to BMU ID renaming
 * 
 * This script runs the SQL migration to rename farm_id to bmu_id
 * across the database tables.
 * 
 * Usage:
 *   npx tsx run_rename_migration.ts
 */

import pg from 'pg';
import * as fs from 'fs';
import * as path from 'path';
import { fileURLToPath } from 'url';

const { Pool } = pg;
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

async function runMigration() {
  // Create a connection to the database
  const pool = new Pool({
    connectionString: process.env.DATABASE_URL
  });

  try {
    // Read the migration file
    const migrationFile = path.join(__dirname, 'migrations', 'rename_farm_id_to_bmu_id.sql');
    const sql = fs.readFileSync(migrationFile, 'utf8');

    console.log('Starting migration: Renaming farm_id to bmu_id in database tables');
    
    // Begin transaction
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      
      // Execute the migration
      await client.query(sql);
      
      // Commit the transaction
      await client.query('COMMIT');
      console.log('Migration completed successfully');
    } catch (err) {
      // Rollback in case of error
      await client.query('ROLLBACK');
      console.error('Migration failed:', err);
      throw err;
    } finally {
      client.release();
    }
  } catch (err) {
    console.error('Error running migration:', err);
    process.exit(1);
  } finally {
    // Close the pool
    await pool.end();
  }
}

// Run the migration
runMigration().catch(err => {
  console.error('Unhandled error:', err);
  process.exit(1);
});