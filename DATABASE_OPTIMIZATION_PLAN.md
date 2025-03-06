# Database Optimization Implementation Plan

## Overview

This document outlines a detailed implementation plan for further optimizing the database structure and queries in the Bitcoin Mining Analytics platform. Building on the previous optimization that replaced materialized views with direct queries, this plan focuses on additional improvements to enhance performance, maintainability, and reliability.

## Current State

The previous database optimization achieved:
- Removal of unnecessary materialized view tables
- Elimination of redundant indexes
- Replacement of materialized views with optimized direct queries
- Overall performance improvements (40-75% faster queries)

## Opportunities for Further Optimization

### 1. Advanced Connection Pooling and Transaction Management

**Current Challenges**:
- Basic connection pooling configuration
- Inconsistent transaction management across services
- Connection leaks in some error scenarios

**Proposed Solution**:

1. Create a dedicated database service for unified connection management:

```typescript
// server/services/database/connectionManager.ts

import { Pool, PoolClient } from 'pg';
import { logger } from '../../utils/logger';
import { DatabaseError } from '../../utils/errors';

// Connection pool with optimized settings
let pool: Pool;

interface TransactionResult<T> {
  success: boolean;
  result?: T;
  error?: Error;
}

/**
 * Initialize the database connection pool
 */
export function initializePool(): Pool {
  if (!pool) {
    pool = new Pool({
      connectionString: process.env.DATABASE_URL,
      max: parseInt(process.env.DB_POOL_MAX || '20', 10),
      idleTimeoutMillis: parseInt(process.env.DB_IDLE_TIMEOUT || '30000', 10),
      connectionTimeoutMillis: parseInt(process.env.DB_CONNECTION_TIMEOUT || '15000', 10),
      allowExitOnIdle: false
    });
    
    // Add error handler to pool
    pool.on('error', (err) => {
      logger.error('Database pool error', {
        module: 'database',
        error: DatabaseError.fromPgError(err)
      });
    });
    
    // Add health check for pool
    setInterval(async () => {
      try {
        const client = await pool.connect();
        client.release();
        logger.debug('Database connection pool health check passed', {
          module: 'database',
          context: { poolSize: pool.totalCount, idleConnections: pool.idleCount }
        });
      } catch (error) {
        logger.error('Database connection pool health check failed', {
          module: 'database',
          error: DatabaseError.fromPgError(error)
        });
      }
    }, 60000); // Check every minute
  }
  
  return pool;
}

/**
 * Get the connection pool
 */
export function getPool(): Pool {
  if (!pool) {
    return initializePool();
  }
  return pool;
}

/**
 * Execute a function within a transaction
 */
export async function withTransaction<T>(
  operation: (client: PoolClient) => Promise<T>
): Promise<TransactionResult<T>> {
  const client = await getPool().connect();
  
  try {
    await client.query('BEGIN');
    const result = await operation(client);
    await client.query('COMMIT');
    return { success: true, result };
  } catch (error) {
    await client.query('ROLLBACK');
    logger.error('Transaction failed', {
      module: 'database',
      error: error instanceof Error ? error : new Error(String(error))
    });
    return { 
      success: false, 
      error: error instanceof Error ? error : new Error(String(error))
    };
  } finally {
    client.release();
  }
}

/**
 * Close the database pool (typically on app shutdown)
 */
export async function closePool(): Promise<void> {
  if (pool) {
    await pool.end();
    pool = undefined;
    logger.info('Database connection pool closed', { module: 'database' });
  }
}
```

2. Update the database service to use the connection manager:

```typescript
// db/index.ts
import { drizzle } from 'drizzle-orm/node-postgres';
import { NodePgDatabase } from 'drizzle-orm/node-postgres/driver';
import { getPool, withTransaction } from '../server/services/database/connectionManager';
import * as schema from './schema';

// Export the query builder instance
export const db = drizzle({ pool: getPool(), schema });

// Export transaction support
export { withTransaction };

// Type for the database
export type Database = NodePgDatabase<typeof schema>;
```

### 2. Query Optimization Techniques

**Current Challenges**:
- Some queries could be further optimized
- Limited use of query parameters for better query plan caching
- Inadequate query plan monitoring

**Proposed Solution**:

1. Create a query optimization utility:

```typescript
// server/services/database/queryOptimizer.ts

import { sql } from 'drizzle-orm';
import { logger } from '../../utils/logger';
import { getPool } from './connectionManager';

/**
 * Measure query performance and log it for analysis
 */
export async function measureQueryPerformance<T>(
  name: string,
  query: () => Promise<T>
): Promise<T> {
  const start = process.hrtime();
  
  try {
    const result = await query();
    const [seconds, nanoseconds] = process.hrtime(start);
    const duration = seconds * 1000 + nanoseconds / 1000000;
    
    logger.debug(`Query ${name} completed in ${duration.toFixed(2)}ms`, {
      module: 'database',
      context: { queryName: name, duration }
    });
    
    return result;
  } catch (error) {
    const [seconds, nanoseconds] = process.hrtime(start);
    const duration = seconds * 1000 + nanoseconds / 1000000;
    
    logger.error(`Query ${name} failed after ${duration.toFixed(2)}ms`, {
      module: 'database',
      context: { queryName: name, duration },
      error
    });
    
    throw error;
  }
}

/**
 * Analyze a specific query plan
 */
export async function explainQuery(queryText: string, params: any[] = []): Promise<any> {
  const pool = getPool();
  const client = await pool.connect();
  
  try {
    const result = await client.query(`EXPLAIN (ANALYZE, VERBOSE, FORMAT JSON) ${queryText}`, params);
    return result.rows[0]['QUERY PLAN'][0];
  } finally {
    client.release();
  }
}

/**
 * Create common table expressions for recurring query patterns
 */
export function curtailmentBitcoinJoinCTE(dateCondition: any) {
  return sql`
    WITH curtailment_data AS (
      SELECT
        settlement_date,
        settlement_period,
        farm_id,
        SUM(ABS(volume)) as total_volume,
        SUM(payment) as total_payment
      FROM curtailment_records
      WHERE ${dateCondition}
      GROUP BY settlement_date, settlement_period, farm_id
    ),
    bitcoin_data AS (
      SELECT
        settlement_date,
        settlement_period,
        farm_id,
        miner_model,
        SUM(bitcoin_mined) as total_bitcoin,
        MAX(difficulty) as max_difficulty
      FROM historical_bitcoin_calculations
      WHERE ${dateCondition}
      GROUP BY settlement_date, settlement_period, farm_id, miner_model
    )
  `;
}
```

2. Create optimized query patterns for common operations:

```typescript
// server/services/database/optimizedQueries.ts

import { sql } from 'drizzle-orm';
import { db } from '../../../db';
import { curtailmentRecords, historicalBitcoinCalculations } from '../../../db/schema';
import { format } from 'date-fns';

/**
 * Get optimized daily summary data
 */
export async function getDailyOptimizedSummary(date: string) {
  const query = db.execute(sql`
    WITH daily_curtailment AS (
      SELECT
        settlement_date,
        COUNT(DISTINCT settlement_period) as period_count,
        COUNT(DISTINCT farm_id) as farm_count,
        SUM(ABS(volume)) as total_volume,
        SUM(payment) as total_payment
      FROM curtailment_records
      WHERE settlement_date = ${date}
      GROUP BY settlement_date
    ),
    daily_bitcoin AS (
      SELECT
        settlement_date,
        miner_model,
        COUNT(DISTINCT settlement_period) as period_count,
        SUM(bitcoin_mined) as total_bitcoin,
        MAX(difficulty) as max_difficulty
      FROM historical_bitcoin_calculations
      WHERE settlement_date = ${date}
      GROUP BY settlement_date, miner_model
    )
    SELECT
      dc.settlement_date,
      dc.period_count,
      dc.farm_count,
      dc.total_volume,
      dc.total_payment,
      json_agg(
        json_build_object(
          'minerModel', db.miner_model,
          'bitcoinMined', db.total_bitcoin,
          'difficulty', db.max_difficulty
        )
      ) as miner_data
    FROM daily_curtailment dc
    LEFT JOIN daily_bitcoin db ON dc.settlement_date = db.settlement_date
    GROUP BY dc.settlement_date, dc.period_count, dc.farm_count, dc.total_volume, dc.total_payment
  `);
  
  return query;
}

/**
 * Get optimized monthly summary with single query
 */
export async function getMonthlyOptimizedSummary(yearMonth: string) {
  // Parse year and month
  const [year, month] = yearMonth.split('-').map(n => parseInt(n, 10));
  const startDate = new Date(year, month - 1, 1);
  const endDate = new Date(year, month, 0); // Last day of month
  const formattedStartDate = format(startDate, 'yyyy-MM-dd');
  const formattedEndDate = format(endDate, 'yyyy-MM-dd');
  
  const query = db.execute(sql`
    WITH monthly_curtailment AS (
      SELECT
        DATE_TRUNC('month', settlement_date) as month,
        COUNT(DISTINCT settlement_date) as day_count,
        COUNT(DISTINCT settlement_period) as period_count,
        COUNT(DISTINCT farm_id) as farm_count,
        SUM(ABS(volume)) as total_volume,
        SUM(payment) as total_payment
      FROM curtailment_records
      WHERE settlement_date BETWEEN ${formattedStartDate} AND ${formattedEndDate}
      GROUP BY DATE_TRUNC('month', settlement_date)
    ),
    monthly_bitcoin AS (
      SELECT
        DATE_TRUNC('month', settlement_date) as month,
        miner_model,
        COUNT(DISTINCT settlement_date) as day_count,
        SUM(bitcoin_mined) as total_bitcoin,
        AVG(difficulty) as avg_difficulty
      FROM historical_bitcoin_calculations
      WHERE settlement_date BETWEEN ${formattedStartDate} AND ${formattedEndDate}
      GROUP BY DATE_TRUNC('month', settlement_date), miner_model
    )
    SELECT
      TO_CHAR(mc.month, 'YYYY-MM') as year_month,
      mc.day_count,
      mc.period_count,
      mc.farm_count,
      mc.total_volume,
      mc.total_payment,
      json_agg(
        json_build_object(
          'minerModel', mb.miner_model,
          'bitcoinMined', mb.total_bitcoin,
          'avgDifficulty', mb.avg_difficulty
        )
      ) as miner_data
    FROM monthly_curtailment mc
    LEFT JOIN monthly_bitcoin mb ON mc.month = mb.month
    GROUP BY mc.month, mc.day_count, mc.period_count, mc.farm_count, mc.total_volume, mc.total_payment
  `);
  
  return query;
}
```

### 3. Data Partitioning for Large Tables

**Current Challenges**:
- Growing table sizes affecting query performance
- Full table scans becoming expensive as data grows
- Inefficient querying of historical data

**Proposed Solution**:

Create a partitioning migration script:

```typescript
// scripts/database/partitionTables.ts

import { getPool } from '../../server/services/database/connectionManager';
import { logger } from '../../server/utils/logger';

async function createPartitioningForHistoricalTables() {
  const pool = getPool();
  const client = await pool.connect();
  
  try {
    // Start transaction
    await client.query('BEGIN');
    
    // 1. Create new partitioned curtailment_records table
    await client.query(`
      CREATE TABLE curtailment_records_partitioned (
        id SERIAL,
        settlement_date DATE NOT NULL,
        settlement_period INTEGER NOT NULL,
        farm_id TEXT NOT NULL,
        volume NUMERIC NOT NULL,
        payment NUMERIC NOT NULL,
        original_price NUMERIC NOT NULL,
        final_price NUMERIC NOT NULL,
        so_flag BOOLEAN NOT NULL,
        cadl_flag BOOLEAN,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        lead_party_name TEXT
      ) PARTITION BY RANGE (settlement_date);
    `);
    
    // 2. Create partitions by year
    const years = [2022, 2023, 2024, 2025];
    for (const year of years) {
      await client.query(`
        CREATE TABLE curtailment_records_y${year}
        PARTITION OF curtailment_records_partitioned
        FOR VALUES FROM ('${year}-01-01') TO ('${year+1}-01-01');
      `);
    }
    
    // 3. Create future partition
    await client.query(`
      CREATE TABLE curtailment_records_future
      PARTITION OF curtailment_records_partitioned
      FOR VALUES FROM ('2026-01-01') TO ('2030-01-01');
    `);
    
    // 4. Create indexes on partitioned table
    await client.query(`
      CREATE INDEX curtailment_partitioned_date_idx ON curtailment_records_partitioned(settlement_date);
      CREATE INDEX curtailment_partitioned_period_idx ON curtailment_records_partitioned(settlement_period);
      CREATE INDEX curtailment_partitioned_farm_idx ON curtailment_records_partitioned(farm_id);
    `);
    
    // 5. Migrate data to partitioned table
    await client.query(`
      INSERT INTO curtailment_records_partitioned
      SELECT * FROM curtailment_records;
    `);
    
    // 6. Rename tables to swap them
    await client.query(`
      ALTER TABLE curtailment_records RENAME TO curtailment_records_old;
      ALTER TABLE curtailment_records_partitioned RENAME TO curtailment_records;
    `);
    
    // 7. Repeat similar process for historical_bitcoin_calculations
    // ...
    
    // Commit transaction
    await client.query('COMMIT');
    
    logger.info('Successfully partitioned tables', { module: 'database' });
  } catch (error) {
    await client.query('ROLLBACK');
    logger.error('Failed to partition tables', {
      module: 'database',
      error
    });
    throw error;
  } finally {
    client.release();
  }
}

// Run the function if script is executed directly
if (require.main === module) {
  createPartitioningForHistoricalTables()
    .then(() => process.exit(0))
    .catch(err => {
      console.error(err);
      process.exit(1);
    });
}
```

### 4. Query Result Caching

**Current Challenges**:
- Repeated identical queries
- High database load during peak usage
- Computation-intensive aggregations

**Proposed Solution**:

Create a cache service:

```typescript
// server/services/cache/queryCache.ts

import NodeCache from 'node-cache';
import { logger } from '../../utils/logger';

// TTL values in seconds
const TTL = {
  SHORT: 60, // 1 minute
  MEDIUM: 300, // 5 minutes
  LONG: 3600, // 1 hour
  VERY_LONG: 86400 // 1 day
};

// Cache instance
const cache = new NodeCache({
  stdTTL: TTL.MEDIUM,
  checkperiod: 120,
  useClones: false
});

/**
 * Get item from cache or compute it
 */
export async function getCachedResult<T>(
  key: string,
  compute: () => Promise<T>,
  ttl: number = TTL.MEDIUM
): Promise<T> {
  // Check if key exists in cache
  const cachedValue = cache.get<T>(key);
  if (cachedValue !== undefined) {
    logger.debug(`Cache hit for key: ${key}`, { module: 'cache' });
    return cachedValue;
  }
  
  // Compute value and store in cache
  logger.debug(`Cache miss for key: ${key}`, { module: 'cache' });
  const result = await compute();
  cache.set(key, result, ttl);
  return result;
}

/**
 * Invalidate cache keys by pattern
 */
export function invalidateCache(pattern: string): void {
  const keys = cache.keys().filter(key => key.includes(pattern));
  if (keys.length > 0) {
    logger.debug(`Invalidating ${keys.length} cache keys matching: ${pattern}`, { module: 'cache' });
    keys.forEach(key => cache.del(key));
  }
}

/**
 * Clear entire cache
 */
export function clearCache(): void {
  logger.info(`Clearing entire cache`, { module: 'cache' });
  cache.flushAll();
}

/**
 * Get cache statistics
 */
export function getCacheStats(): any {
  const stats = cache.getStats();
  return {
    hits: stats.hits,
    misses: stats.misses,
    keys: cache.keys().length,
    ksize: stats.ksize,
    vsize: stats.vsize
  };
}

// Export TTL constants
export { TTL };
```

Use the cache in optimized services:

```typescript
// server/services/optimizedMiningService.ts (updated)

import { getCachedResult, TTL, invalidateCache } from '../cache/queryCache';
// ... other imports

/**
 * Get daily mining potential data with caching
 */
export async function getDailyMiningPotential(date: string, minerModel: string, farmId?: string): Promise<any> {
  const cacheKey = `daily_mining:${date}:${minerModel}:${farmId || 'all'}`;
  
  return getCachedResult(
    cacheKey,
    async () => {
      // Original function logic...
    },
    TTL.MEDIUM
  );
}

/**
 * Get monthly mining potential data with caching
 */
export async function getMonthlyMiningPotential(yearMonth: string, minerModel: string, farmId?: string): Promise<any> {
  const cacheKey = `monthly_mining:${yearMonth}:${minerModel}:${farmId || 'all'}`;
  
  return getCachedResult(
    cacheKey,
    async () => {
      // Original function logic...
    },
    TTL.LONG
  );
}

/**
 * Invalidate caches after data updates
 */
export function invalidateMiningCaches(date: string): void {
  // Invalidate daily cache
  invalidateCache(`daily_mining:${date}`);
  
  // Invalidate monthly cache for this month
  const yearMonth = date.substring(0, 7);
  invalidateCache(`monthly_mining:${yearMonth}`);
  
  // Invalidate yearly cache for this year
  const year = date.substring(0, 4);
  invalidateCache(`yearly_mining:${year}`);
}
```

## Implementation Plan

### Phase 1: Create Core Infrastructure (1 week)

1. Implement connection manager and transaction utilities
2. Create query monitoring and performance analysis tools
3. Implement basic caching service

### Phase 2: Query Optimization (1 week)

1. Analyze and optimize the most frequently used queries
2. Implement optimized query patterns with common table expressions
3. Apply parameterization to all queries

### Phase 3: Advanced Optimizations (2 weeks)

1. Add partitioning to large tables
2. Implement complete caching strategy
3. Optimize query execution plans

### Phase 4: Testing and Verification (1 week)

1. Conduct load testing with production-scale data
2. Measure performance improvements
3. Verify data integrity after optimizations

## Expected Benefits

1. **Improved Query Performance**: 30-50% faster query execution
2. **Reduced Database Load**: Lower CPU and memory usage on database server
3. **Better Scalability**: Partitioning will allow better handling of growing data
4. **Improved Developer Experience**: Simplified database interaction patterns
5. **Enhanced Reliability**: Better error handling and connection management

## Risk Mitigation

1. **Data Integrity**: All optimizations will include transaction protection
2. **Backward Compatibility**: Maintain compatibility with existing code
3. **Rollback Plan**: Create scripts to revert changes if issues arise
4. **Performance Benchmarking**: Measure before and after to confirm improvements
5. **Phased Deployment**: Implement changes incrementally with thorough testing between phases

## Success Criteria

The optimization will be considered successful when:

1. Query performance improves by at least 30% for key operations
2. System can handle 3x the current data volume without performance degradation
3. All optimizations are implemented without disrupting existing functionality
4. Database load is reduced during peak usage periods