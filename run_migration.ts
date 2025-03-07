/**
 * PostgreSQL Database Migration Runner
 * 
 * This script runs SQL migration scripts to create or update tables
 * for the mining potential optimization.
 */

import pg from 'pg';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const { Pool } = pg;
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const pool = new Pool({
  connectionString: process.env.DATABASE_URL
});

async function runMigration() {
  console.log('Starting database migration...');
  
  try {
    // Specify which migration file to run
    const migrationFile = process.argv[2] || 'remove_average_difficulty.sql';
    const fullPath = path.join(__dirname, 'db', 'migrations', migrationFile);
    
    // Check if the file exists
    if (!fs.existsSync(fullPath)) {
      console.error(`Migration file not found: ${fullPath}`);
      console.log('Available migrations:');
      const migrationsDir = path.join(__dirname, 'db', 'migrations');
      const files = fs.readdirSync(migrationsDir);
      files.forEach(file => console.log(` - ${file}`));
      process.exit(1);
    }
    
    const sql = fs.readFileSync(fullPath, 'utf8');
    console.log(`Executing migration script: ${migrationFile}`);
    
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      
      // Execute the migration script
      const result = await client.query(sql);
      
      // Check if result contains notices/messages
      if (result) {
        console.log('SQL execution completed');
      }
      
      await client.query('COMMIT');
      console.log('Migration committed successfully');
    } catch (error) {
      await client.query('ROLLBACK');
      console.error('Migration failed:', error);
      throw error;
    } finally {
      client.release();
    }
  } catch (error) {
    console.error('Error executing migration:', error);
    process.exit(1);
  } finally {
    await pool.end();
  }
}

runMigration().catch(console.error);