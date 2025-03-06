/**
 * Request logging middleware
 * 
 * This middleware logs all incoming API requests with useful metadata
 * for debugging and monitoring purposes.
 */

import { Request, Response, NextFunction } from 'express';
import { v4 as uuidv4 } from 'uuid';
import { logger } from '../utils/logger';

interface RequestWithId extends Request {
  requestId?: string;
}

/**
 * Request logger middleware
 */
export function requestLogger(req: Request, res: Response, next: NextFunction) {
  const requestId = uuidv4();
  const requestWithId = req as RequestWithId;
  requestWithId.requestId = requestId;
  
  // Add request ID to response headers
  res.setHeader('X-Request-ID', requestId);
  
  // Log the incoming request
  logger.info(`${req.method} ${req.originalUrl}`, {
    module: 'request',
    context: {
      requestId,
      method: req.method,
      url: req.originalUrl,
      path: req.path,
      query: req.query,
      params: req.params,
      ip: req.ip,
      userAgent: req.get('user-agent')
    }
  });
  
  // Track response time and status
  const startTime = Date.now();
  
  // Track completion
  res.on('finish', () => {
    const duration = Date.now() - startTime;
    const statusCode = res.statusCode;
    
    // Define log level based on status code
    const isError = statusCode >= 500;
    const isWarning = statusCode >= 400 && statusCode < 500;
    
    const logFn = isError ? 
      logger.error.bind(logger) : 
      (isWarning ? logger.warning.bind(logger) : logger.debug.bind(logger));
    
    // Log the response
    logFn(`${req.method} ${req.originalUrl} ${statusCode} ${duration}ms`, {
      module: 'response',
      context: {
        requestId,
        method: req.method,
        url: req.originalUrl,
        statusCode,
        duration
      }
    });
  });
  
  next();
}