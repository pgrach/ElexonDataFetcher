/**
 * Database Migration for Wind Generation Data Pipeline
 * 
 * This script creates the wind_generation_data table for storing aggregated
 * wind generation data from Elexon's B1630 API endpoint.
 */
import { db } from './db';
import { sql } from 'drizzle-orm';
import { logger } from './server/utils/logger';

async function runMigration() {
  try {
    logger.info('Starting wind generation data table migration', { module: 'windDataMigration' });
    
    // Check if table already exists
    const tableExists = await db.execute(sql`
      SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name = 'wind_generation_data'
      );
    `);
    
    if (tableExists.rows[0].exists) {
      logger.info('wind_generation_data table already exists, skipping creation', { module: 'windDataMigration' });
      return;
    }
    
    // Create the wind_generation_data table
    await db.execute(sql`
      CREATE TABLE IF NOT EXISTS "wind_generation_data" (
        "id" SERIAL PRIMARY KEY,
        "settlement_date" DATE NOT NULL,
        "settlement_period" INTEGER NOT NULL,
        "wind_onshore" NUMERIC NOT NULL,
        "wind_offshore" NUMERIC NOT NULL,
        "total_wind" NUMERIC NOT NULL,
        "last_updated" TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
        "data_source" TEXT DEFAULT 'ELEXON' NOT NULL
      );
    `);
    
    // Create index for fast lookups
    await db.execute(sql`
      CREATE UNIQUE INDEX IF NOT EXISTS wind_generation_date_period_idx 
      ON "wind_generation_data" ("settlement_date", "settlement_period");
    `);
    
    logger.info('Successfully created wind_generation_data table and indexes', { module: 'windDataMigration' });
  } catch (error) {
    logger.error('Error creating wind_generation_data table', { 
      module: 'windDataMigration',
      error: error instanceof Error ? error : new Error(String(error))
    });
    throw error;
  }
}

// Run the migration if the script is executed directly
// Note: Using import.meta.url for ES module compatibility
if (import.meta.url.endsWith(process.argv[1])) {
  runMigration()
    .then(() => {
      console.log('Migration completed successfully');
      process.exit(0);
    })
    .catch((err) => {
      console.error('Migration failed:', err);
      process.exit(1);
    });
}

export { runMigration };