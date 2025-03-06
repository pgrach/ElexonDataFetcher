/**
 * Performance monitoring middleware
 * 
 * This middleware tracks query performance and identifies slow operations
 * to aid in optimization efforts.
 */

import { Request, Response, NextFunction } from 'express';
import { logger } from '../utils/logger';

// Configuration for performance thresholds
const THRESHOLD_WARNING_MS = 500;   // Warning threshold for request time
const THRESHOLD_ERROR_MS = 2000;    // Error threshold for request time
const THRESHOLD_SIZE_KB = 1024;     // Warning threshold for response size (1MB)

/**
 * Middleware to monitor API performance
 */
export function performanceMonitor(req: Request, res: Response, next: NextFunction) {
  // Skip for non-API routes
  if (!req.path.startsWith('/api')) {
    return next();
  }
  
  // Record start time
  const startTime = Date.now();
  let responseSize = 0;
  
  // Store original write and end methods
  const originalWrite = res.write;
  const originalEnd = res.end;
  
  // Instead of overriding methods, use the 'finish' event
  res.on('finish', () => {
    // Calculate response time
    const responseTime = Date.now() - startTime;
    
    // Get response size from headers if available
    const contentLength = res.getHeader('content-length');
    if (contentLength) {
      responseSize = Number(contentLength);
    }
    
    // Log slow responses
    if (responseTime > THRESHOLD_ERROR_MS) {
      logger.error(`SLOW API: ${req.method} ${req.path} took ${responseTime}ms to complete`, {
        module: 'performance',
        context: {
          method: req.method,
          path: req.path,
          query: req.query,
          params: req.params,
          responseTime,
          responseSize,
          statusCode: res.statusCode
        }
      });
    } else if (responseTime > THRESHOLD_WARNING_MS) {
      logger.warning(`Slow API: ${req.method} ${req.path} took ${responseTime}ms to complete`, {
        module: 'performance',
        context: {
          method: req.method, 
          path: req.path,
          responseTime
        }
      });
    }
    
    // Log large responses
    if (responseSize > THRESHOLD_SIZE_KB * 1024) {
      logger.warning(`Large API response: ${req.method} ${req.path} returned ${Math.round(responseSize / 1024)}KB`, {
        module: 'performance',
        context: {
          method: req.method,
          path: req.path,
          responseSize
        }
      });
    }
  });
  
  next();
}

/**
 * Create a performance monitoring wrapper for any function
 * Use this to track performance of specific operations
 */
export function trackPerformance<T extends (...args: any[]) => Promise<any>>(
  name: string,
  fn: T,
  thresholdMs: number = THRESHOLD_WARNING_MS
): T {
  return (async (...args: Parameters<T>): Promise<ReturnType<T>> => {
    const startTime = Date.now();
    try {
      const result = await fn(...args);
      const duration = Date.now() - startTime;
      
      if (duration > thresholdMs) {
        logger.warning(`Slow operation: ${name} took ${duration}ms`, {
          module: 'performance',
          context: { 
            operation: name,
            duration,
            args: args.map(arg => 
              typeof arg === 'object' ? 
                // Safely stringify objects, limiting their size
                JSON.stringify(arg).substring(0, 200) + (JSON.stringify(arg).length > 200 ? '...' : '') : 
                String(arg)
            )
          }
        });
      }
      
      return result;
    } catch (error) {
      const duration = Date.now() - startTime;
      logger.error(`Failed operation: ${name} failed after ${duration}ms`, {
        module: 'performance',
        context: { operation: name, duration },
        error: error as Error
      });
      throw error;
    }
  }) as T;
}