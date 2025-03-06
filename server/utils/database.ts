/**
 * Database utility functions for Bitcoin Mining Analytics platform
 * 
 * This module provides standardized methods for common database operations,
 * connection pooling, query optimization, and error handling.
 */

import { db } from '@db/index';
import { sql, InferSelectModel } from 'drizzle-orm';
import { DatabaseError } from './errors';
import { logger } from './logger';
import { trackPerformance } from '../middleware/performanceMonitor';

/**
 * Execute a query with standardized error handling and performance tracking
 */
export async function executeQuery<T>(
  queryName: string,
  queryFn: () => Promise<T>,
  context: Record<string, any> = {}
): Promise<T> {
  try {
    // Use the performance tracking wrapper
    const trackedQueryFn = trackPerformance(
      `DB:${queryName}`, 
      queryFn, 
      200 // Lower threshold for database operations (200ms)
    );
    
    return await trackedQueryFn();
  } catch (error: any) {
    // Log and wrap database errors
    logger.error(`Database query '${queryName}' failed`, {
      module: 'database',
      context: {
        ...context,
        error: error.message,
        code: error.code
      },
      error
    });
    
    throw DatabaseError.fromPgError(error, {
      queryName,
      ...context
    });
  }
}

/**
 * Check if a table exists
 */
export async function tableExists(tableName: string): Promise<boolean> {
  return executeQuery(
    'tableExists',
    async () => {
      const result = await db.execute<{ exists: boolean }>(sql`
        SELECT EXISTS (
          SELECT 1 FROM information_schema.tables 
          WHERE table_schema = 'public' 
          AND table_name = ${tableName}
        )
      `);
      
      return result[0]?.exists || false;
    },
    { tableName }
  );
}

/**
 * Get count of records in a table that match a condition
 */
export async function getRecordCount(
  tableName: string,
  whereClause: string,
  params: any[] = []
): Promise<number> {
  return executeQuery(
    'getRecordCount',
    async () => {
      const result = await db.execute<{ count: number }>(
        sql`SELECT COUNT(*) as count FROM ${sql.identifier(tableName)} WHERE ${sql.raw(whereClause)}`,
        params
      );
      
      return result[0]?.count || 0;
    },
    { tableName, whereClause, params }
  );
}

/**
 * Perform a batch insert operation with optimized performance
 */
export async function batchInsert<T extends Record<string, any>>(
  tableName: string,
  records: T[],
  chunkSize: number = 100
): Promise<number> {
  if (!records.length) {
    return 0;
  }
  
  // Get column names from the first record
  const columns = Object.keys(records[0]);
  
  // Process in chunks to avoid overwhelming the database
  let totalInserted = 0;
  
  for (let i = 0; i < records.length; i += chunkSize) {
    const chunk = records.slice(i, i + chunkSize);
    
    await executeQuery(
      'batchInsert',
      async () => {
        // Create the bulk insert query
        const values = chunk.map(record => 
          '(' + columns.map(col => `$${col}`).join(', ') + ')'
        ).join(', ');
        
        const placeholders: any[] = [];
        chunk.forEach(record => {
          columns.forEach(col => {
            placeholders.push(record[col]);
          });
        });
        
        // Build and execute the query
        const query = `
          INSERT INTO ${tableName} (${columns.join(', ')})
          VALUES ${values}
          ON CONFLICT DO NOTHING
        `;
        
        const result = await db.execute(sql.raw(query, placeholders));
        return result;
      },
      { tableName, recordCount: chunk.length }
    );
    
    totalInserted += chunk.length;
  }
  
  return totalInserted;
}

/**
 * Safely execute a transaction with proper error handling
 */
export async function withTransaction<T>(
  name: string,
  fn: () => Promise<T>,
  context: Record<string, any> = {}
): Promise<T> {
  return executeQuery(
    `transaction:${name}`,
    async () => {
      return await db.transaction(async (tx) => {
        return await fn();
      });
    },
    context
  );
}

/**
 * Check database connection health
 */
export async function checkDatabaseHealth(): Promise<{
  healthy: boolean;
  connectionTime: number;
  message?: string;
}> {
  try {
    const startTime = Date.now();
    await db.execute(sql`SELECT 1`);
    const connectionTime = Date.now() - startTime;
    
    return {
      healthy: true,
      connectionTime
    };
  } catch (error: any) {
    logger.error('Database health check failed', {
      module: 'database',
      error
    });
    
    return {
      healthy: false,
      connectionTime: -1,
      message: error.message
    };
  }
}

/**
 * Get database statistics for monitoring
 */
export async function getDatabaseStats(): Promise<{
  tableStats: Array<{
    tableName: string;
    rowCount: number;
    sizeBytes: number;
  }>;
  connectionCount: number;
  uptime: number;
}> {
  return executeQuery(
    'getDatabaseStats',
    async () => {
      // Get table statistics
      const tableStatsQuery = await db.execute<{
        tableName: string;
        rowCount: number;
        sizeBytes: number;
      }>(sql`
        SELECT 
          relname as "tableName",
          n_live_tup as "rowCount",
          pg_total_relation_size(relid) as "sizeBytes"
        FROM pg_stat_user_tables
        ORDER BY n_live_tup DESC
      `);
      
      // Get connection count
      const connectionQuery = await db.execute<{ count: number }>(sql`
        SELECT count(*) as count FROM pg_stat_activity
      `);
      
      // Get database uptime
      const uptimeQuery = await db.execute<{ uptime: number }>(sql`
        SELECT extract(epoch from current_timestamp - pg_postmaster_start_time()) as uptime
      `);
      
      return {
        tableStats: tableStatsQuery,
        connectionCount: connectionQuery[0]?.count || 0,
        uptime: uptimeQuery[0]?.uptime || 0
      };
    },
    {}
  );
}