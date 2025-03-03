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
    const migrationFile = path.join(__dirname, 'migrations', 'add_materialized_views.sql');
    const sql = fs.readFileSync(migrationFile, 'utf8');
    
    console.log(`Executing migration script: ${migrationFile}`);
    
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      
      // Execute the migration script
      const result = await client.query(sql);
      console.log('Migration completed successfully');
      
      // Check if result contains notices/messages
      if (result) {
        console.log('SQL execution completed');
      }
      
      await client.query('COMMIT');
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