/**
 * Connection Timeout Analyzer
 * 
 * This tool analyzes database connection timeouts and provides diagnostics
 * to help identify and resolve issues during the reconciliation process.
 * 
 * Usage:
 * npx tsx connection_timeout_analyzer.ts [command]
 * 
 * Commands:
 *   analyze      - Run a full connection analysis
 *   monitor      - Start monitoring connections in real-time
 *   test         - Test connection with various query complexities
 */

import pg from 'pg';
import { performance } from 'perf_hooks';
import os from 'os';
import fs from 'fs';

// Configuration
const MAX_TEST_DURATION = 120000; // 2 minutes
const CONNECTION_TIMEOUT = 10000; // 10 seconds
const QUERY_TIMEOUT = 30000; // 30 seconds
const LOG_FILE = './connection_analysis.log';

// Initialize database pool with careful settings
const pool = new pg.Pool({
  connectionString: process.env.DATABASE_URL,
  max: 5, // Lower number of connections for testing
  idleTimeoutMillis: 30000, // 30 seconds
  connectionTimeoutMillis: CONNECTION_TIMEOUT, 
  query_timeout: QUERY_TIMEOUT,
  allowExitOnIdle: true
});

// Log function with timestamp
function log(message: string, level: 'info' | 'warning' | 'error' | 'success' = 'info'): void {
  const timestamp = new Date().toISOString();
  const formattedMessage = `[${timestamp}] [${level.toUpperCase()}] ${message}`;
  
  // Console output with colors
  switch (level) {
    case 'error':
      console.error('\x1b[31m%s\x1b[0m', formattedMessage);
      break;
    case 'warning':
      console.warn('\x1b[33m%s\x1b[0m', formattedMessage);
      break;
    case 'success':
      console.log('\x1b[32m%s\x1b[0m', formattedMessage);
      break;
    default:
      console.log('\x1b[36m%s\x1b[0m', formattedMessage);
  }
  
  // Append to log file
  fs.appendFileSync(LOG_FILE, formattedMessage + '\n');
}

/**
 * Get system resource information
 */
function getSystemInfo() {
  return {
    platform: os.platform(),
    cpuCount: os.cpus().length,
    cpuModel: os.cpus()[0].model,
    totalMemory: (os.totalmem() / (1024 * 1024 * 1024)).toFixed(2) + ' GB',
    freeMemory: (os.freemem() / (1024 * 1024 * 1024)).toFixed(2) + ' GB',
    memoryUsagePercent: (100 - (os.freemem() / os.totalmem() * 100)).toFixed(1) + '%',
    uptime: (os.uptime() / 3600).toFixed(2) + ' hours',
    loadAverage: os.loadavg()
  };
}

/**
 * Get pool statistics
 */
async function getPoolStats() {
  // We have to use any type because the pg Pool doesn't expose these metrics in the type definitions
  const poolAny = pool as any;
  
  return {
    totalCount: poolAny.totalCount || 'Unknown',
    idleCount: poolAny.idleCount || 'Unknown', 
    waitingCount: poolAny.waitingCount || 'Unknown',
    maxConnections: pool.options.max
  };
}

/**
 * Test a simple query
 */
async function testSimpleQuery(): Promise<{ success: boolean; duration: number; error?: any }> {
  const start = performance.now();
  try {
    await pool.query('SELECT 1 as test');
    return { success: true, duration: performance.now() - start };
  } catch (error) {
    return { success: false, duration: performance.now() - start, error };
  }
}

/**
 * Test a moderate complexity query
 */
async function testModerateQuery(): Promise<{ success: boolean; duration: number; error?: any }> {
  const start = performance.now();
  try {
    await pool.query(`
      SELECT 
        COUNT(*) as count, 
        MIN(settlement_date) as earliest_date, 
        MAX(settlement_date) as latest_date
      FROM curtailment_records
    `);
    return { success: true, duration: performance.now() - start };
  } catch (error) {
    return { success: false, duration: performance.now() - start, error };
  }
}

/**
 * Test a complex query (similar to what's used in reconciliation)
 */
async function testComplexQuery(): Promise<{ success: boolean; duration: number; error?: any }> {
  const start = performance.now();
  try {
    await pool.query(`
      WITH curtailment_summary AS (
        SELECT 
          settlement_date,
          COUNT(DISTINCT (settlement_period || '-' || farm_id)) as unique_combinations
        FROM curtailment_records
        GROUP BY settlement_date
      ),
      bitcoin_summary AS (
        SELECT 
          settlement_date,
          COUNT(*) as calculation_count
        FROM historical_bitcoin_calculations
        GROUP BY settlement_date
      )
      SELECT 
        cs.settlement_date,
        cs.unique_combinations * 3 as expected_count,
        COALESCE(bs.calculation_count, 0) as actual_count,
        CASE 
          WHEN cs.unique_combinations = 0 THEN 100
          ELSE ROUND((COALESCE(bs.calculation_count, 0) * 100.0) / (cs.unique_combinations * 3), 2)
        END as completion_percentage
      FROM curtailment_summary cs
      LEFT JOIN bitcoin_summary bs ON cs.settlement_date = bs.settlement_date
      ORDER BY cs.settlement_date DESC
      LIMIT 30
    `);
    return { success: true, duration: performance.now() - start };
  } catch (error) {
    return { success: false, duration: performance.now() - start, error };
  }
}

/**
 * Test multiple concurrent connections
 */
async function testConcurrentConnections(count: number = 3): Promise<{ 
  success: boolean; 
  successCount: number;
  failureCount: number;
  averageDuration: number; 
  errors: any[]
}> {
  const start = performance.now();
  const promises: Promise<any>[] = [];
  
  for (let i = 0; i < count; i++) {
    promises.push(
      pool.query(`
        SELECT 
          COUNT(*) as count 
        FROM curtailment_records
        WHERE settlement_date > '2023-01-01'
      `)
    );
  }
  
  const results = await Promise.allSettled(promises);
  const fulfilled = results.filter(r => r.status === 'fulfilled').length;
  const rejected = results.filter(r => r.status === 'rejected').length;
  
  const errors = results
    .filter((r): r is PromiseRejectedResult => r.status === 'rejected')
    .map(r => r.reason);
  
  return {
    success: rejected === 0,
    successCount: fulfilled,
    failureCount: rejected,
    averageDuration: (performance.now() - start) / count,
    errors
  };
}

/**
 * Test for network latency
 */
async function testNetworkLatency(iterations: number = 10): Promise<{
  averageLatency: number;
  minLatency: number;
  maxLatency: number;
  failedAttempts: number;
}> {
  const latencies: number[] = [];
  let failedAttempts = 0;
  
  for (let i = 0; i < iterations; i++) {
    const start = performance.now();
    try {
      await pool.query('SELECT 1');
      latencies.push(performance.now() - start);
    } catch (error) {
      failedAttempts++;
    }
    // Small delay between tests
    await new Promise(resolve => setTimeout(resolve, 100));
  }
  
  const avgLatency = latencies.length > 0 
    ? latencies.reduce((sum, val) => sum + val, 0) / latencies.length 
    : 0;
  
  return {
    averageLatency: avgLatency,
    minLatency: latencies.length > 0 ? Math.min(...latencies) : 0,
    maxLatency: latencies.length > 0 ? Math.max(...latencies) : 0,
    failedAttempts
  };
}

/**
 * Test for memory usage during query
 */
async function testMemoryUsage(): Promise<{
  beforeMemory: number;
  afterMemory: number;
  difference: number;
  success: boolean;
}> {
  const beforeMemory = process.memoryUsage().heapUsed / 1024 / 1024;
  
  try {
    // Complex query with large result set
    await pool.query(`
      SELECT *
      FROM curtailment_records
      ORDER BY settlement_date DESC
      LIMIT 1000
    `);
    
    const afterMemory = process.memoryUsage().heapUsed / 1024 / 1024;
    
    return {
      beforeMemory,
      afterMemory,
      difference: afterMemory - beforeMemory,
      success: true
    };
  } catch (error) {
    const afterMemory = process.memoryUsage().heapUsed / 1024 / 1024;
    
    return {
      beforeMemory,
      afterMemory,
      difference: afterMemory - beforeMemory,
      success: false
    };
  }
}

/**
 * Get the database table sizes
 */
async function getDatabaseTableSizes(): Promise<{ [key: string]: { rows: number; size: string } }> {
  try {
    const result = await pool.query(`
      SELECT
        relname AS table_name,
        n_live_tup AS row_count,
        pg_size_pretty(pg_total_relation_size(relid)) AS total_size
      FROM pg_stat_user_tables
      ORDER BY pg_total_relation_size(relid) DESC;
    `);
    
    const sizes: { [key: string]: { rows: number; size: string } } = {};
    
    for (const row of result.rows) {
      sizes[row.table_name] = {
        rows: parseInt(row.row_count),
        size: row.total_size
      };
    }
    
    return sizes;
  } catch (error) {
    log(`Error getting table sizes: ${error}`, 'error');
    return {};
  }
}

/**
 * Check the current active queries
 */
async function checkActiveQueries(): Promise<any[]> {
  try {
    const result = await pool.query(`
      SELECT 
        pid,
        now() - query_start AS duration,
        state,
        query
      FROM pg_stat_activity
      WHERE state <> 'idle'
        AND pid <> pg_backend_pid()
      ORDER BY duration DESC;
    `);
    
    return result.rows;
  } catch (error) {
    log(`Error checking active queries: ${error}`, 'error');
    return [];
  }
}

/**
 * Format duration in milliseconds to human-readable format
 */
function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms.toFixed(2)}ms`;
  const seconds = ms / 1000;
  if (seconds < 60) return `${seconds.toFixed(2)}s`;
  const minutes = seconds / 60;
  return `${minutes.toFixed(2)}min`;
}

/**
 * Diagnose a timeout error
 */
function diagnoseTimeoutError(error: any): string[] {
  const diagnosis: string[] = [];
  const errorMessage = error?.message || String(error);
  
  if (errorMessage.includes('timeout') || errorMessage.includes('Connection terminated')) {
    diagnosis.push('Connection timeout detected');
    
    if (errorMessage.includes('query_timeout')) {
      diagnosis.push('Query execution exceeded the timeout limit');
      diagnosis.push('Consider optimizing your query or increasing the query_timeout setting');
    } else if (errorMessage.includes('connection timeout')) {
      diagnosis.push('Initial connection to the database failed');
      diagnosis.push('Check network connectivity and database server status');
    } else if (errorMessage.includes('statement timeout')) {
      diagnosis.push('Statement execution exceeded the timeout limit set in PostgreSQL');
      diagnosis.push('Review long-running queries or increase statement_timeout in PostgreSQL');
    } else if (errorMessage.includes('idle timeout')) {
      diagnosis.push('Connection was closed because it was idle for too long');
      diagnosis.push('This is generally not a problem, connections are automatically re-established');
    }
  } else if (errorMessage.includes('too many clients')) {
    diagnosis.push('Connection pool exhausted - Too many clients');
    diagnosis.push('Reduce concurrent requests or increase max pool size');
  } else if (errorMessage.includes('out of memory')) {
    diagnosis.push('Database server is out of memory');
    diagnosis.push('Reduce query complexity or increase server resources');
  }
  
  if (diagnosis.length === 0) {
    diagnosis.push('Unknown connection issue');
    diagnosis.push(`Error message: ${errorMessage}`);
  }
  
  return diagnosis;
}

/**
 * Run a comprehensive analysis of database connectivity
 */
async function runConnectionAnalysis(): Promise<void> {
  log('Starting comprehensive database connection analysis', 'info');
  log('This may take a few minutes...', 'info');
  
  // System Information
  const sysInfo = getSystemInfo();
  log('\n=== System Information ===', 'info');
  Object.entries(sysInfo).forEach(([key, value]) => {
    log(`${key}: ${value}`, 'info');
  });
  
  // Pool Statistics
  const poolStats = await getPoolStats();
  log('\n=== Connection Pool Statistics ===', 'info');
  Object.entries(poolStats).forEach(([key, value]) => {
    log(`${key}: ${value}`, 'info');
  });
  
  // Database Size Information
  log('\n=== Database Size Information ===', 'info');
  const tableSizes = await getDatabaseTableSizes();
  
  Object.entries(tableSizes).forEach(([table, info]) => {
    log(`${table}: ${info.rows.toLocaleString()} rows, ${info.size}`, 'info');
  });
  
  // Test Simple Query
  log('\n=== Simple Query Test ===', 'info');
  const simpleResult = await testSimpleQuery();
  if (simpleResult.success) {
    log(`Simple query executed successfully in ${formatDuration(simpleResult.duration)}`, 'success');
  } else {
    log(`Simple query failed in ${formatDuration(simpleResult.duration)}`, 'error');
    log(`Error: ${simpleResult.error}`, 'error');
    
    // Diagnose the error
    const diagnosis = diagnoseTimeoutError(simpleResult.error);
    diagnosis.forEach(line => log(line, 'warning'));
  }
  
  // Test Moderate Query
  log('\n=== Moderate Query Test ===', 'info');
  const moderateResult = await testModerateQuery();
  if (moderateResult.success) {
    log(`Moderate query executed successfully in ${formatDuration(moderateResult.duration)}`, 'success');
  } else {
    log(`Moderate query failed in ${formatDuration(moderateResult.duration)}`, 'error');
    log(`Error: ${moderateResult.error}`, 'error');
    
    const diagnosis = diagnoseTimeoutError(moderateResult.error);
    diagnosis.forEach(line => log(line, 'warning'));
  }
  
  // Test Complex Query
  log('\n=== Complex Query Test ===', 'info');
  const complexResult = await testComplexQuery();
  if (complexResult.success) {
    log(`Complex query executed successfully in ${formatDuration(complexResult.duration)}`, 'success');
  } else {
    log(`Complex query failed in ${formatDuration(complexResult.duration)}`, 'error');
    log(`Error: ${complexResult.error}`, 'error');
    
    const diagnosis = diagnoseTimeoutError(complexResult.error);
    diagnosis.forEach(line => log(line, 'warning'));
  }
  
  // Test Concurrent Connections
  log('\n=== Concurrent Connections Test (3 connections) ===', 'info');
  const concurrentResult = await testConcurrentConnections(3);
  if (concurrentResult.success) {
    log(`All concurrent queries executed successfully in average ${formatDuration(concurrentResult.averageDuration)}`, 'success');
  } else {
    log(`${concurrentResult.failureCount} out of ${concurrentResult.failureCount + concurrentResult.successCount} concurrent queries failed`, 'error');
    log(`Average duration: ${formatDuration(concurrentResult.averageDuration)}`, 'info');
    
    concurrentResult.errors.forEach((error, i) => {
      log(`Error in query ${i+1}: ${error}`, 'error');
      const diagnosis = diagnoseTimeoutError(error);
      diagnosis.forEach(line => log(line, 'warning'));
    });
  }
  
  // Test Network Latency
  log('\n=== Network Latency Test ===', 'info');
  const latencyResult = await testNetworkLatency();
  if (latencyResult.failedAttempts === 0) {
    log(`Network latency test completed successfully`, 'success');
    log(`Average latency: ${formatDuration(latencyResult.averageLatency)}`, 'info');
    log(`Min latency: ${formatDuration(latencyResult.minLatency)}`, 'info');
    log(`Max latency: ${formatDuration(latencyResult.maxLatency)}`, 'info');
    
    if (latencyResult.averageLatency > 1000) {
      log(`⚠️ Average latency is high (${formatDuration(latencyResult.averageLatency)})`, 'warning');
      log(`High latency can contribute to timeout issues during reconciliation`, 'warning');
    }
  } else {
    log(`${latencyResult.failedAttempts} out of 10 latency test attempts failed`, 'error');
    if (latencyResult.averageLatency > 0) {
      log(`Average latency for successful attempts: ${formatDuration(latencyResult.averageLatency)}`, 'info');
    }
  }
  
  // Memory Usage Test
  log('\n=== Memory Usage Test ===', 'info');
  const memoryResult = await testMemoryUsage();
  if (memoryResult.success) {
    log(`Memory test completed successfully`, 'success');
    log(`Memory before: ${memoryResult.beforeMemory.toFixed(2)} MB`, 'info');
    log(`Memory after: ${memoryResult.afterMemory.toFixed(2)} MB`, 'info');
    log(`Memory difference: ${memoryResult.difference.toFixed(2)} MB`, 'info');
    
    if (memoryResult.difference > 50) {
      log(`⚠️ Large memory increase detected (${memoryResult.difference.toFixed(2)} MB)`, 'warning');
      log(`Large memory usage can lead to out-of-memory errors during reconciliation`, 'warning');
    }
  } else {
    log(`Memory test failed`, 'error');
  }
  
  // Check Active Queries
  log('\n=== Active Queries ===', 'info');
  const activeQueries = await checkActiveQueries();
  if (activeQueries.length === 0) {
    log(`No active queries found`, 'info');
  } else {
    log(`Found ${activeQueries.length} active queries:`, 'info');
    activeQueries.forEach((query, i) => {
      log(`Query ${i+1}: Running for ${formatDuration(query.duration)}`, 'info');
      log(`State: ${query.state}`, 'info');
      log(`SQL: ${query.query.substring(0, 100)}...`, 'info');
    });
    
    if (activeQueries.some(q => q.duration > 30000)) {
      log(`⚠️ Long-running queries detected`, 'warning');
      log(`Long-running queries may block other operations and cause timeouts`, 'warning');
    }
  }
  
  // Summary and Recommendations
  log('\n=== Analysis Summary and Recommendations ===', 'info');
  
  // Calculate timeout risk score
  let timeoutRiskScore = 0;
  
  if (!simpleResult.success) timeoutRiskScore += 3;
  if (!moderateResult.success) timeoutRiskScore += 2;
  if (!complexResult.success) timeoutRiskScore += 2;
  if (!concurrentResult.success) timeoutRiskScore += 2;
  if (latencyResult.averageLatency > 1000) timeoutRiskScore += 1;
  if (latencyResult.failedAttempts > 0) timeoutRiskScore += 2;
  if (memoryResult.difference > 50) timeoutRiskScore += 1;
  if (activeQueries.some(q => q.duration > 30000)) timeoutRiskScore += 1;
  
  // Provide risk assessment
  if (timeoutRiskScore >= 8) {
    log(`⚠️ High risk of timeouts during reconciliation (Risk score: ${timeoutRiskScore}/14)`, 'error');
  } else if (timeoutRiskScore >= 4) {
    log(`⚠️ Moderate risk of timeouts during reconciliation (Risk score: ${timeoutRiskScore}/14)`, 'warning');
  } else {
    log(`✅ Low risk of timeouts during reconciliation (Risk score: ${timeoutRiskScore}/14)`, 'success');
  }
  
  // Provide recommendations
  log('\nRecommendations:', 'info');
  
  if (!simpleResult.success || !moderateResult.success) {
    log('1. Check database connectivity and credentials', 'warning');
  }
  
  if (!complexResult.success) {
    log('2. Break down complex reconciliation queries into smaller batches', 'warning');
    log('   - Use smaller batch sizes in reconciliation tools', 'info');
    log('   - Process one date at a time for problematic periods', 'info');
  }
  
  if (!concurrentResult.success) {
    log('3. Reduce concurrency during reconciliation', 'warning');
    log('   - Process batches sequentially instead of in parallel', 'info');
    log('   - Set MAX_CONCURRENT_PROCESSES to 1 in reconciliation tools', 'info');
  }
  
  if (latencyResult.averageLatency > 1000 || latencyResult.failedAttempts > 0) {
    log('4. Network latency issues detected', 'warning');
    log('   - Increase connection timeout settings', 'info');
    log('   - Consider running reconciliation during off-peak hours', 'info');
  }
  
  if (memoryResult.difference > 50) {
    log('5. High memory usage detected', 'warning');
    log('   - Process smaller batches to reduce memory footprint', 'info');
    log('   - Consider implementing pagination in queries', 'info');
  }
  
  if (activeQueries.some(q => q.duration > 30000)) {
    log('6. Long-running queries detected', 'warning');
    log('   - Check for locks or conflicts in the database', 'info');
    log('   - Terminate long-running queries before starting reconciliation', 'info');
  }
  
  log('\nFor best results:', 'info');
  log('- Use the efficient_reconciliation.ts script with small batch sizes', 'info');
  log('- Process one date at a time for problematic dates', 'info');
  log('- Implement checkpoint-based processing to resume after timeouts', 'info');
  log('- Monitor database performance during reconciliation', 'info');
  
  log('\nAnalysis complete. Results saved to connection_analysis.log', 'success');
}

/**
 * Main function to handle command line arguments
 */
async function main() {
  try {
    const command = process.argv[2]?.toLowerCase() || 'analyze';
    
    log(`Starting connection timeout analyzer with command: ${command}`, 'info');
    
    switch (command) {
      case "analyze":
        await runConnectionAnalysis();
        break;
        
      case "test":
        log('\n=== Quick Connection Test ===', 'info');
        const simpleResult = await testSimpleQuery();
        if (simpleResult.success) {
          log(`Simple query executed successfully in ${formatDuration(simpleResult.duration)}`, 'success');
        } else {
          log(`Simple query failed in ${formatDuration(simpleResult.duration)}`, 'error');
          log(`Error: ${simpleResult.error}`, 'error');
          
          const diagnosis = diagnoseTimeoutError(simpleResult.error);
          diagnosis.forEach(line => log(line, 'warning'));
        }
        
        log('\n=== Complex Query Test ===', 'info');
        const complexResult = await testComplexQuery();
        if (complexResult.success) {
          log(`Complex query executed successfully in ${formatDuration(complexResult.duration)}`, 'success');
        } else {
          log(`Complex query failed in ${formatDuration(complexResult.duration)}`, 'error');
          log(`Error: ${complexResult.error}`, 'error');
          
          const diagnosis = diagnoseTimeoutError(complexResult.error);
          diagnosis.forEach(line => log(line, 'warning'));
        }
        break;
        
      case "monitor":
        log("Starting connection monitoring...", 'info');
        
        // Initial test
        await testSimpleQuery();
        
        // Setup periodic checks
        const monitorInterval = setInterval(async () => {
          try {
            const simpleResult = await testSimpleQuery();
            const activeQueries = await checkActiveQueries();
            const poolStats = await getPoolStats();
            
            log('\n=== Connection Status Update ===', 'info');
            log(`Time: ${new Date().toISOString()}`, 'info');
            log(`Simple Query: ${simpleResult.success ? 'OK' : 'FAILED'} (${formatDuration(simpleResult.duration)})`, 
              simpleResult.success ? 'success' : 'error');
            log(`Active Queries: ${activeQueries.length}`, 'info');
            log(`Pool Status: ${poolStats.idleCount}/${poolStats.totalCount} idle, ${poolStats.waitingCount} waiting`, 'info');
            
            if (!simpleResult.success) {
              const diagnosis = diagnoseTimeoutError(simpleResult.error);
              diagnosis.forEach(line => log(line, 'warning'));
            }
          } catch (error) {
            log(`Monitoring error: ${error}`, 'error');
          }
        }, 60000);
        
        log("Monitoring active. Press Ctrl+C to stop.", 'info');
        process.stdin.resume();
        
        // Setup cleanup
        process.on('SIGINT', () => {
          clearInterval(monitorInterval);
          log("Monitoring stopped", 'info');
          pool.end();
          process.exit(0);
        });
        break;
        
      default:
        log("Connection Timeout Analyzer", 'info');
        log("\nCommands:", 'info');
        log("  analyze      - Run a full connection analysis", 'info');
        log("  test         - Test connection with various query complexities", 'info');
        log("  monitor      - Start monitoring connections in real-time", 'info');
        log("\nExample: npx tsx connection_timeout_analyzer.ts analyze", 'info');
        
        // Default behavior - run simple analysis
        await testSimpleQuery();
    }
  } catch (error) {
    log(`Fatal error: ${error}`, 'error');
    throw error;
  } finally {
    // Clean up pool if not monitoring
    if (process.argv[2]?.toLowerCase() !== 'monitor') {
      await pool.end();
      log("Database connection pool closed", 'info');
    }
  }
}

// Run the main function if script is executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
  main()
    .then(() => {
      if (process.argv[2]?.toLowerCase() !== 'monitor') {
        log("\n=== Connection Analysis Complete ===", 'success');
        process.exit(0);
      }
    })
    .catch(error => {
      log(`Fatal error: ${error}`, 'error');
      process.exit(1);
    });
}

export { 
  runConnectionAnalysis,
  testSimpleQuery,
  testComplexQuery,
  testConcurrentConnections,
  diagnoseTimeoutError
};