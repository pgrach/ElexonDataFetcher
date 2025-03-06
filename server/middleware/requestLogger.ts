/**
 * Request logging middleware
 * 
 * This middleware logs all incoming API requests with useful metadata
 * for debugging and monitoring purposes.
 */

import { Request, Response, NextFunction } from 'express';
import { logger } from '../utils/logger';

// Add custom property to Response for tracking
interface RequestWithId extends Request {
  requestId?: string;
}

/**
 * Request logger middleware
 */
export function requestLogger(req: Request, res: Response, next: NextFunction) {
  // Record request start time
  const startTime = Date.now();
  
  // Generate request ID for correlation
  const requestId = `req_${Date.now()}_${Math.random().toString(36).substring(2, 15)}`;
  (req as RequestWithId).requestId = requestId;
  req.headers['x-request-id'] = requestId;
  
  // Log request received
  logger.info(`Request received: ${req.method} ${req.path}`, {
    module: 'api',
    context: {
      requestId,
      method: req.method,
      path: req.originalUrl || req.url,
      ip: req.ip,
      userAgent: req.headers['user-agent'],
      query: req.query,
      params: req.params
    }
  });
  
  // Log response when it completes
  res.on('finish', () => {
    const responseTime = Date.now() - startTime;
    
    // Log with appropriate level based on status code
    const logLevel = 
      res.statusCode >= 500 ? 'error' :
      res.statusCode >= 400 ? 'warning' : 'info';
    
    logger[logLevel as 'info' | 'warning' | 'error'](`Request completed: ${req.method} ${req.path} ${res.statusCode} (${responseTime}ms)`, {
      module: 'api',
      context: {
        requestId,
        method: req.method,
        path: req.originalUrl || req.url,
        statusCode: res.statusCode,
        responseTime,
        requestSize: req.headers['content-length'] ? 
          parseInt(req.headers['content-length'] as string, 10) : undefined
      }
    });
  });
  
  next();
}