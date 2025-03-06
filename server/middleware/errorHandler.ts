/**
 * Global error handling middleware for Express
 * 
 * This middleware catches and processes all errors that occur during request handling,
 * providing standardized error responses and comprehensive logging.
 */

import { Request, Response, NextFunction } from 'express';
import { logger } from '../utils/logger';
import { AppError, ApiError, DatabaseError, ValidationError } from '../utils/errors';

/**
 * Global API error handler middleware
 */
export function errorHandler(err: Error, req: Request, res: Response, next: NextFunction) {
  // Default to internal server error
  let statusCode = 500;
  let responseBody: any = {
    error: {
      message: 'Internal server error',
      statusCode: 500
    }
  };
  
  // Log the error
  if (err instanceof AppError) {
    // Use our structured error
    logger.logError(err, {
      module: 'api',
      context: {
        path: req.path,
        method: req.method,
        query: req.query,
        params: req.params,
        ip: req.ip,
        userId: (req as any).user?.id
      }
    });
    
    if (err instanceof ApiError) {
      // Use the API error format
      statusCode = err.statusCode;
      responseBody = err.toResponse();
    } else if (err instanceof ValidationError) {
      // Default validation errors to 400
      statusCode = 400;
      responseBody = {
        error: {
          message: err.message,
          statusCode: 400,
          details: process.env.NODE_ENV !== 'production' ? err.context : undefined
        }
      };
    } else if (err instanceof DatabaseError) {
      // Database errors are 500 by default
      responseBody = {
        error: {
          message: 'Database operation failed',
          statusCode: 500
        }
      };
    }
  } else {
    // Unknown error type
    logger.error('Unhandled error in API request', {
      module: 'api',
      error: err,
      context: {
        path: req.path,
        method: req.method,
        query: req.query,
        params: req.params
      }
    });
  }
  
  // Scrub error details in production
  if (process.env.NODE_ENV === 'production' && statusCode === 500) {
    responseBody.error.message = 'Internal server error';
    delete responseBody.error.details;
  }
  
  res.status(statusCode).json(responseBody);
}