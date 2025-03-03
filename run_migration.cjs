/**
 * PostgreSQL Database Migration Runner (CommonJS version)
 * 
 * This script runs SQL migration scripts to create or update tables
 * for the mining potential optimization.
 */

const pg = require('pg');
const fs = require('fs');
const path = require('path');

const { Pool } = pg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL
});

async function runMigration() {
  console.log('Starting database migration...');
  
  try {
    const migrationFile = path.join(__dirname, 'create_materialized_tables.sql');
    const sql = fs.readFileSync(migrationFile, 'utf8');
    
    console.log(`Executing migration script: ${migrationFile}`);
    
    const client = await pool.connect();
    try {
      // Execute the migration as a single transaction
      await client.query('BEGIN');
      await client.query(sql);
      await client.query('COMMIT');
      
      console.log('Migration completed successfully');
    } catch (error) {
      await client.query('ROLLBACK');
      console.error('Migration failed:', error);
      throw error;
    } finally {
      client.release();
    }
    
    console.log('Migration completed successfully');
  } catch (error) {
    console.error('Error executing migration:', error);
    process.exit(1);
  } finally {
    await pool.end();
  }
}

runMigration().catch(console.error);