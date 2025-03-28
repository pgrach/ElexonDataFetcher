/**
 * Wind Data Migration Script
 * 
 * This script ensures the wind_generation_data table exists and has the correct schema.
 * It's imported by the server on startup to guarantee data structures are in place
 * before the application tries to access wind generation data.
 */

import { sql } from 'drizzle-orm';
import { db } from './db';
import { logger } from './server/utils/logger';
import { fileURLToPath } from 'url';

/**
 * Execute the wind data table migration
 */
export async function runMigration(): Promise<void> {
  try {
    logger.info('Checking wind_generation_data table structure', {
      module: 'windDataMigration'
    });

    // Check if table exists
    const tableExists = await db.execute(sql`
      SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public'
        AND table_name = 'wind_generation_data'
      ) as exists
    `);

    if (!tableExists.rows[0]?.exists) {
      logger.info('wind_generation_data table does not exist, creating it', {
        module: 'windDataMigration'
      });

      // Create the table
      await db.execute(sql`
        CREATE TABLE IF NOT EXISTS wind_generation_data (
          id SERIAL PRIMARY KEY,
          settlement_date DATE NOT NULL,
          settlement_period INTEGER NOT NULL,
          wind_onshore NUMERIC NOT NULL,
          wind_offshore NUMERIC NOT NULL,
          total_wind NUMERIC NOT NULL,
          last_updated TIMESTAMP NOT NULL DEFAULT NOW(),
          data_source TEXT NOT NULL DEFAULT 'ELEXON',
          CONSTRAINT unique_settlement_period UNIQUE (settlement_date, settlement_period)
        )
      `);

      logger.info('wind_generation_data table created successfully', {
        module: 'windDataMigration'
      });

      // Create index for performance
      await db.execute(sql`
        CREATE INDEX IF NOT EXISTS idx_wind_settlement_date ON wind_generation_data(settlement_date)
      `);

      logger.info('wind_generation_data index created successfully', {
        module: 'windDataMigration'
      });
    } else {
      logger.info('wind_generation_data table already exists, checking schema', {
        module: 'windDataMigration'
      });
      
      // Check if data_source column exists (added in later version)
      const hasDataSourceColumn = await db.execute(sql`
        SELECT EXISTS (
          SELECT FROM information_schema.columns
          WHERE table_schema = 'public'
          AND table_name = 'wind_generation_data'
          AND column_name = 'data_source'
        ) as exists
      `);

      if (!hasDataSourceColumn.rows[0]?.exists) {
        logger.info('Adding data_source column to wind_generation_data table', {
          module: 'windDataMigration'
        });
        
        await db.execute(sql`
          ALTER TABLE wind_generation_data 
          ADD COLUMN data_source TEXT NOT NULL DEFAULT 'ELEXON'
        `);
        
        logger.info('data_source column added successfully', {
          module: 'windDataMigration'
        });
      }
    }

    logger.info('Wind data migration completed successfully', {
      module: 'windDataMigration'
    });
  } catch (error) {
    // Log the error without trying to access the error object
    const errorMessage = error instanceof Error ? error.message : String(error);
    logger.error(`Failed to run wind data migration: ${errorMessage}`, {
      module: 'windDataMigration'
    });
    
    throw error;
  }
}

// For ES Modules, check if this is the main file
const checkIfMainModule = () => {
  try {
    return process.argv[1] === fileURLToPath(import.meta.url);
  } catch (error) {
    return false;
  }
};

// Run migration if this is the main module
if (checkIfMainModule()) {
  runMigration()
    .then(() => {
      console.log('Migration completed successfully');
      process.exit(0);
    })
    .catch(error => {
      console.error('Migration failed:', error);
      process.exit(1);
    });
}