/**
 * Global error handling middleware for Express
 * 
 * This middleware catches and processes all errors that occur during request handling,
 * providing standardized error responses and comprehensive logging.
 */

import { Request, Response, NextFunction } from 'express';
import { ApiError, DatabaseError, ValidationError, AppError } from '../utils/errors';
import { logger } from '../utils/logger';

/**
 * Global API error handler middleware
 */
export function errorHandler(err: Error, req: Request, res: Response, next: NextFunction) {
  // Default status code and error structure
  let statusCode = 500;
  let errorResponse: {
    error: {
      message: string;
      type?: string;
      statusCode?: number;
      timestamp: string;
      stack?: string;
    }
  } = {
    error: {
      message: err.message || 'Internal server error',
      type: err.name,
      timestamp: new Date().toISOString()
    }
  };
  
  // Enhance logging and response based on error type
  if (err instanceof ApiError) {
    // Handle API errors with status code
    statusCode = err.statusCode;
    const apiResponse = err.toResponse();
    errorResponse.error = {
      ...errorResponse.error,
      message: apiResponse.error.message,
      statusCode: apiResponse.error.statusCode,
      timestamp: apiResponse.error.timestamp
    };
    
    logger.error(`API Error: ${err.message}`, {
      module: 'api',
      context: {
        path: req.path,
        method: req.method,
        statusCode,
        ...err.context
      },
      error: err
    });
  } else if (err instanceof ValidationError) {
    // Handle validation errors as 400 Bad Request
    statusCode = 400;
    errorResponse.error.type = 'ValidationError';
    
    logger.warning(`Validation Error: ${err.message}`, {
      module: 'validation',
      context: {
        path: req.path,
        method: req.method,
        ...err.context
      },
      error: err
    });
  } else if (err instanceof DatabaseError) {
    // Handle database errors (but hide implementation details)
    statusCode = 503;
    errorResponse.error.message = 'Database operation failed';
    
    // Log the actual database error details
    logger.error(`Database Error: ${err.message}`, {
      module: 'database',
      context: {
        path: req.path,
        method: req.method,
        ...err.context
      },
      error: err
    });
  } else if (err instanceof AppError) {
    // Handle other application errors
    statusCode = 500;
    
    logger.error(`Application Error: ${err.toLogFormat()}`, {
      module: err.category,
      context: {
        path: req.path,
        method: req.method,
        ...err.context
      },
      error: err
    });
  } else {
    // Handle unexpected errors
    logger.error(`Unexpected Error: ${err.message}`, {
      module: 'server',
      context: {
        path: req.path,
        method: req.method,
        stack: err.stack
      },
      error: err
    });
  }
  
  // In development, include stack trace
  if (process.env.NODE_ENV !== 'production') {
    errorResponse.error.stack = err.stack || '';
  }
  
  // Send error response
  res.status(statusCode).json(errorResponse);
}