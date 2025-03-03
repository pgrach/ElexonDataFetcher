/**
 * PostgreSQL Database Index Optimization Script
 * 
 * This script runs SQL migration to remove redundant indexes and improve query performance.
 */

const fs = require('fs');
const path = require('path');
const { Pool } = require('pg');
require('dotenv').config();

async function runIndexOptimization() {
  const migrationFile = path.join(__dirname, 'migrations', 'remove_redundant_indexes.sql');
  
  if (!fs.existsSync(migrationFile)) {
    console.error(`Migration file not found: ${migrationFile}`);
    process.exit(1);
  }
  
  const migrationSql = fs.readFileSync(migrationFile, 'utf8');
  
  const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
  });
  
  try {
    console.log('Starting execution of index optimization migration...');
    console.log('-'.repeat(50));
    
    console.log('Measuring database size before optimization...');
    const beforeStats = await pool.query(`
      SELECT pg_size_pretty(pg_database_size(current_database())) as db_size,
             pg_size_pretty(pg_total_relation_size('curtailment_records')) as curtailment_size,
             pg_size_pretty(pg_total_relation_size('historical_bitcoin_calculations')) as bitcoin_size;
    `);
    console.log(`Database size before: ${beforeStats.rows[0].db_size}`);
    console.log(`Curtailment records table size: ${beforeStats.rows[0].curtailment_size}`);
    console.log(`Historical bitcoin calculations table size: ${beforeStats.rows[0].bitcoin_size}`);
    
    console.log('-'.repeat(50));
    console.log('Executing index optimization migration...');
    
    // Start a transaction
    await pool.query('BEGIN');
    
    // Split the migration SQL into individual statements and execute them
    const statements = migrationSql.split(';').filter(stmt => stmt.trim());
    
    for (let stmt of statements) {
      const trimStmt = stmt.trim();
      if (trimStmt) {
        console.log(`Executing: ${trimStmt}`);
        await pool.query(trimStmt);
      }
    }
    
    // Commit the transaction
    await pool.query('COMMIT');
    
    console.log('-'.repeat(50));
    console.log('Measuring database size after optimization...');
    const afterStats = await pool.query(`
      SELECT pg_size_pretty(pg_database_size(current_database())) as db_size,
             pg_size_pretty(pg_total_relation_size('curtailment_records')) as curtailment_size,
             pg_size_pretty(pg_total_relation_size('historical_bitcoin_calculations')) as bitcoin_size;
    `);
    console.log(`Database size after: ${afterStats.rows[0].db_size}`);
    console.log(`Curtailment records table size: ${afterStats.rows[0].curtailment_size}`);
    console.log(`Historical bitcoin calculations table size: ${afterStats.rows[0].bitcoin_size}`);
    
    console.log('-'.repeat(50));
    console.log('Optimization migration completed successfully!');
    
    // Check remaining indexes
    const remainingIndexes = await pool.query(`
      SELECT 
          tablename, 
          indexname, 
          indexdef
      FROM 
          pg_indexes
      WHERE 
          tablename IN ('curtailment_records', 'historical_bitcoin_calculations')
      ORDER BY 
          tablename, indexname;
    `);
    
    console.log('-'.repeat(50));
    console.log('Remaining indexes:');
    remainingIndexes.rows.forEach(idx => {
      console.log(`${idx.tablename}.${idx.indexname}`);
    });
    
  } catch (error) {
    console.error('Error during migration execution:', error);
    
    // Rollback transaction on error
    try {
      await pool.query('ROLLBACK');
      console.log('Migration rolled back due to error');
    } catch (rollbackError) {
      console.error('Error during rollback:', rollbackError);
    }
    
    process.exit(1);
  } finally {
    // Release the pool
    await pool.end();
  }
}

// Execute the migration function
runIndexOptimization().catch(err => {
  console.error('Unhandled error during migration:', err);
  process.exit(1);
});