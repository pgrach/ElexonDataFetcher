/**
 * Wind Generation Data Migration
 * 
 * This script ensures the wind_generation_data table is properly set up
 */

import { db } from '../../../../db';
import { sql } from 'drizzle-orm';
import { logger } from '../../../utils/logger';

/**
 * Run the migration to ensure the wind_generation_data table exists
 */
export async function runMigration(): Promise<void> {
  try {
    logger.info('Running wind generation data migration...', { module: 'windDataMigration' });
    
    // Create the table with all required columns if it doesn't exist
    await db.execute(sql`
      CREATE TABLE IF NOT EXISTS wind_generation_data (
        id SERIAL PRIMARY KEY,
        settlement_date DATE NOT NULL,
        settlement_period INTEGER NOT NULL,
        wind_onshore NUMERIC NOT NULL,
        wind_offshore NUMERIC NOT NULL,
        total_wind NUMERIC NOT NULL,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
        data_source TEXT DEFAULT 'ELEXON' NOT NULL,
        UNIQUE (settlement_date, settlement_period)
      )
    `);
    
    // Create index if it doesn't exist
    await db.execute(sql`
      CREATE INDEX IF NOT EXISTS idx_wind_settlement_date ON wind_generation_data (settlement_date)
    `);
    
    logger.info('Wind generation data migration completed successfully', { module: 'windDataMigration' });
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    logger.error(`Migration failed: ${errorMessage}`, { module: 'windDataMigration' });
    throw error;
  }
}