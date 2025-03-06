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
      const result = await db.execute(sql`
        SELECT EXISTS (
          SELECT 1 FROM information_schema.tables 
          WHERE table_schema = 'public' 
          AND table_name = ${tableName}
        ) as exists
      `);
      
      // Safe access to the result
      if (result && Array.isArray(result) && result.length > 0) {
        return (result[0] as { exists: boolean }).exists;
      }
      
      return false;
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
      // Create a dynamic SQL query with parameters properly integrated
      const dynamicParams: any[] = [];
      
      // Replace ? placeholders with $n placeholders if they exist
      let formattedWhereClause = whereClause;
      if (whereClause.includes('?')) {
        let paramIndex = 0;
        formattedWhereClause = whereClause.replace(/\?/g, () => {
          dynamicParams.push(params[paramIndex]);
          return `$${++paramIndex}`;
        });
      } else {
        // If no ? placeholders, use the params as is
        dynamicParams.push(...params);
      }
      
      // Construct the SQL query with raw where clause
      const query = sql`
        SELECT COUNT(*) as count 
        FROM ${sql.identifier(tableName)} 
        WHERE ${sql.raw(formattedWhereClause)}
      `;
      
      // Execute the query with integrated parameters
      const result = await db.execute(query);
      
      // Safe access to the result
      if (result && Array.isArray(result) && result.length > 0) {
        return Number((result[0] as { count: number | string }).count) || 0;
      }
      
      return 0;
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
    
    // Create a context object for monitoring
    const batchContext = { tableName, recordCount: chunk.length };
    
    // Execute the batch insert with proper context
    await executeQuery(
      'batchInsert',
      async () => {
        // Use a transaction for better performance
        return await db.transaction(async (tx) => {
          // Process each record separately but within a transaction
          for (const record of chunk) {
            // Prepare the data for insertion
            const data: Record<string, any> = {};
            columns.forEach(col => {
              data[col] = record[col];
            });
            
            // Manually construct parameters and incorporate them into the query
            const params = columns.map(col => record[col]);
            
            // Build a SQL template literal with the properly escaped values
            const values = params.map((val, idx) => {
              if (val === null) return 'NULL';
              if (typeof val === 'number') return val.toString();
              if (typeof val === 'boolean') return val ? 'TRUE' : 'FALSE';
              if (typeof val === 'string') return `'${val.replace(/'/g, "''")}'`;
              return `'${JSON.stringify(val).replace(/'/g, "''")}'`;
            }).join(', ');
            
            // Create a safe query with SQL tag
            const safeQuery = sql`
              INSERT INTO ${sql.identifier(tableName)} (${sql.join(columns.map(col => sql.identifier(col)), sql`, `)})
              VALUES (${sql.raw(values)})
              ON CONFLICT DO NOTHING
            `;
            
            // Execute the query
            await tx.execute(safeQuery);
          }
          
          return { rowCount: chunk.length };
        });
      },
      batchContext
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
      const tableStatsQuery = await db.execute(sql`
        SELECT 
          relname as "tableName",
          n_live_tup as "rowCount",
          pg_total_relation_size(relid) as "sizeBytes"
        FROM pg_stat_user_tables
        ORDER BY n_live_tup DESC
      `);
      
      // Get connection count
      const connectionQuery = await db.execute(sql`
        SELECT count(*) as count FROM pg_stat_activity
      `);
      
      // Get database uptime
      const uptimeQuery = await db.execute(sql`
        SELECT extract(epoch from current_timestamp - pg_postmaster_start_time()) as uptime
      `);
      
      // Safe access with proper type conversions
      let connectionCount = 0;
      let uptime = 0;
      
      if (connectionQuery && Array.isArray(connectionQuery) && connectionQuery.length > 0) {
        connectionCount = Number((connectionQuery[0] as any).count) || 0;
      }
      
      if (uptimeQuery && Array.isArray(uptimeQuery) && uptimeQuery.length > 0) {
        uptime = Number((uptimeQuery[0] as any).uptime) || 0;
      }
      
      // Convert tableStatsQuery to proper array format
      const tableStats: Array<{
        tableName: string;
        rowCount: number;
        sizeBytes: number;
      }> = [];
      
      if (tableStatsQuery && Array.isArray(tableStatsQuery)) {
        tableStatsQuery.forEach(row => {
          tableStats.push({
            tableName: String((row as any).tableName || ''),
            rowCount: Number((row as any).rowCount || 0),
            sizeBytes: Number((row as any).sizeBytes || 0)
          });
        });
      }
      
      return {
        tableStats,
        connectionCount,
        uptime
      };
    },
    {}
  );
}