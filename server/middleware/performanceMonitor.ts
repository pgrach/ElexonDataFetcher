/**
 * Performance monitoring middleware
 * 
 * This middleware tracks query performance and identifies slow operations
 * to aid in optimization efforts.
 */

import { Request, Response, NextFunction } from 'express';
import { logger } from '../utils/logger';

// Thresholds for performance monitoring (in milliseconds)
const WARNING_THRESHOLD = 1000;  // 1 second
const CRITICAL_THRESHOLD = 5000; // 5 seconds

export function performanceMonitor(req: Request, res: Response, next: NextFunction) {
  // Track request start time
  const startTime = Date.now();
  
  // Track response time and log after completion
  const startAt = process.hrtime();
  
  // Set header before response is sent
  res.setHeader('X-Response-Time-Tracking', 'enabled');
  
  // Track completion
  res.on('finish', () => {
    const diff = process.hrtime(startAt);
    const duration = Math.round(diff[0] * 1e3 + diff[1] * 1e-6); // Convert to ms
    
    // Log slow requests
    if (duration > CRITICAL_THRESHOLD) {
      logger.warning(`Critical slow request: ${req.method} ${req.originalUrl} took ${duration}ms`, {
        module: 'performance',
        context: {
          method: req.method,
          url: req.originalUrl,
          duration,
          query: req.query,
          params: req.params,
          statusCode: res.statusCode
        }
      });
    } else if (duration > WARNING_THRESHOLD) {
      logger.debug(`Slow request: ${req.method} ${req.originalUrl} took ${duration}ms`, {
        module: 'performance',
        context: {
          method: req.method,
          url: req.originalUrl,
          duration,
          statusCode: res.statusCode
        }
      });
    }
  });
  
  // Let's also set up a timing hook that adds performance header before sending
  const originalSend = res.send;
  res.send = function(...args) {
    // Add performance header - this is done before sending the response
    const diff = process.hrtime(startAt);
    const duration = Math.round(diff[0] * 1e3 + diff[1] * 1e-6);
    res.setHeader('X-Response-Time', `${duration}ms`);
    
    // Call the original send function
    return originalSend.apply(this, args);
  };
  
  next();
}

/**
 * Create a performance monitoring wrapper for any function
 * Use this to track performance of specific operations
 */
export function trackPerformance<T extends (...args: any[]) => Promise<any>>(
  operationName: string,
  fn: T,
  warningThreshold: number = WARNING_THRESHOLD
): T {
  return (async (...args: Parameters<T>): Promise<ReturnType<T>> => {
    const startTime = Date.now();
    
    try {
      // Execute the original function
      const result = await fn(...args);
      
      // Calculate execution time
      const duration = Date.now() - startTime;
      
      // Log slow operations
      if (duration > CRITICAL_THRESHOLD) {
        logger.warning(`Critical slow operation: ${operationName} took ${duration}ms`, {
          module: 'performance',
          context: {
            operation: operationName,
            duration,
            args: args.map(arg => 
              typeof arg === 'object' ? 
                `${arg.constructor?.name || typeof arg}` : 
                String(arg)
            ).join(', ')
          }
        });
      } else if (duration > warningThreshold) {
        logger.debug(`Slow operation: ${operationName} took ${duration}ms`, {
          module: 'performance',
          context: {
            operation: operationName,
            duration
          }
        });
      }
      
      return result;
    } catch (error) {
      // Still log performance on error
      const duration = Date.now() - startTime;
      
      logger.error(`Operation ${operationName} failed after ${duration}ms`, {
        module: 'performance',
        context: {
          operation: operationName,
          duration
        },
        error: error as Error
      });
      
      throw error;
    }
  }) as T;
}