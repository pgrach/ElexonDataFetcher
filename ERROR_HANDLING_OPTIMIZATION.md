# Error Handling and Logging Standardization

## Overview

This document outlines a plan to standardize error handling and logging across the Bitcoin Mining Analytics platform. Currently, there are inconsistent patterns for error handling, logging, and reporting, which can make debugging and maintenance challenging.

## Current Challenges

1. **Inconsistent Error Handling**: Different services use different error handling techniques
2. **Mixed Logging Approaches**: Some services use console.log, others use custom logging functions
3. **Incomplete Error Context**: Many errors lack critical context for debugging
4. **No Standardized Error Classification**: Errors are not categorized by type or severity

## Proposed Solution

### 1. Create a Centralized Error Module

Create a new module at `server/utils/errors.ts` that includes:

```typescript
// server/utils/errors.ts

enum ErrorSeverity {
  INFO = 'info',
  WARNING = 'warning',
  ERROR = 'error',
  CRITICAL = 'critical'
}

enum ErrorCategory {
  DATABASE = 'database',
  API = 'api',
  VALIDATION = 'validation',
  NETWORK = 'network',
  RECONCILIATION = 'reconciliation',
  CALCULATION = 'calculation',
  CONFIGURATION = 'configuration',
  AUTHENTICATION = 'authentication',
  UNKNOWN = 'unknown'
}

type ErrorContext = Record<string, any>;

interface ErrorOptions {
  severity?: ErrorSeverity;
  category?: ErrorCategory;
  context?: ErrorContext;
  originalError?: Error;
}

/**
 * Base application error class with standardized properties
 */
export class AppError extends Error {
  severity: ErrorSeverity;
  category: ErrorCategory;
  context: ErrorContext;
  timestamp: Date;
  originalError?: Error;
  
  constructor(message: string, options: ErrorOptions = {}) {
    super(message);
    this.name = this.constructor.name;
    this.severity = options.severity || ErrorSeverity.ERROR;
    this.category = options.category || ErrorCategory.UNKNOWN;
    this.context = options.context || {};
    this.originalError = options.originalError;
    this.timestamp = new Date();
    
    // Capture stack trace
    if (Error.captureStackTrace) {
      Error.captureStackTrace(this, this.constructor);
    }
  }
  
  /**
   * Format error for logging
   */
  toLogFormat(): string {
    return JSON.stringify({
      name: this.name,
      message: this.message,
      severity: this.severity,
      category: this.category,
      context: this.context,
      timestamp: this.timestamp.toISOString(),
      stack: this.stack,
      originalError: this.originalError ? {
        message: this.originalError.message,
        stack: this.originalError.stack
      } : undefined
    }, null, 2);
  }
}

/**
 * Database-related error
 */
export class DatabaseError extends AppError {
  constructor(message: string, options: ErrorOptions = {}) {
    super(message, {
      ...options,
      category: ErrorCategory.DATABASE
    });
  }
  
  /**
   * Create DatabaseError from pg error
   */
  static fromPgError(error: any, context: ErrorContext = {}): DatabaseError {
    return new DatabaseError(
      `Database error: ${error.message}`,
      {
        severity: ErrorSeverity.ERROR,
        context: {
          ...context,
          code: error.code,
          detail: error.detail,
          hint: error.hint,
          position: error.position,
          table: error.table
        },
        originalError: error
      }
    );
  }
}

/**
 * API-related error
 */
export class ApiError extends AppError {
  statusCode: number;
  
  constructor(message: string, statusCode: number = 500, options: ErrorOptions = {}) {
    super(message, {
      ...options,
      category: ErrorCategory.API
    });
    this.statusCode = statusCode;
  }
  
  /**
   * Format as API response
   */
  toResponse() {
    return {
      error: {
        message: this.message,
        statusCode: this.statusCode,
        category: this.category,
        ...(process.env.NODE_ENV !== 'production' ? { details: this.context } : {})
      }
    };
  }
}

/**
 * Validation error
 */
export class ValidationError extends AppError {
  constructor(message: string, options: ErrorOptions = {}) {
    super(message, {
      ...options,
      category: ErrorCategory.VALIDATION,
      severity: ErrorSeverity.WARNING
    });
  }
  
  /**
   * Create from Zod error
   */
  static fromZodError(error: any, context: ErrorContext = {}): ValidationError {
    return new ValidationError(
      `Validation error: ${error.message}`,
      {
        context: {
          ...context,
          issues: error.errors
        },
        originalError: error
      }
    );
  }
}

/**
 * Reconciliation-specific error
 */
export class ReconciliationError extends AppError {
  constructor(message: string, options: ErrorOptions = {}) {
    super(message, {
      ...options,
      category: ErrorCategory.RECONCILIATION
    });
  }
}

// Export error types
export {
  ErrorSeverity,
  ErrorCategory,
  type ErrorContext,
  type ErrorOptions
};
```

### 2. Create a Centralized Logging Module

Create a new module at `server/utils/logger.ts`:

```typescript
// server/utils/logger.ts

import fs from 'fs';
import path from 'path';
import { ErrorSeverity } from './errors';

// Log levels and colors for console output
enum LogLevel {
  DEBUG = 'debug',
  INFO = 'info',
  WARNING = 'warning',
  ERROR = 'error',
  CRITICAL = 'critical'
}

interface LogOptions {
  level?: LogLevel;
  module?: string;
  context?: Record<string, any>;
  error?: Error;
  timestamp?: Date;
}

interface LogEntry {
  message: string;
  level: LogLevel;
  module: string;
  context?: Record<string, any>;
  error?: {
    message: string;
    stack?: string;
    name?: string;
  };
  timestamp: string;
}

// Map from ErrorSeverity to LogLevel
const severityToLevel: Record<ErrorSeverity, LogLevel> = {
  [ErrorSeverity.INFO]: LogLevel.INFO,
  [ErrorSeverity.WARNING]: LogLevel.WARNING,
  [ErrorSeverity.ERROR]: LogLevel.ERROR,
  [ErrorSeverity.CRITICAL]: LogLevel.CRITICAL
};

// Terminal colors
const colors = {
  reset: '\x1b[0m',
  black: '\x1b[30m',
  red: '\x1b[31m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  blue: '\x1b[34m',
  magenta: '\x1b[35m',
  cyan: '\x1b[36m',
  white: '\x1b[37m',
  bold: '\x1b[1m'
};

// Map log levels to colors
const levelColors: Record<LogLevel, string> = {
  [LogLevel.DEBUG]: colors.cyan,
  [LogLevel.INFO]: colors.green,
  [LogLevel.WARNING]: colors.yellow,
  [LogLevel.ERROR]: colors.red,
  [LogLevel.CRITICAL]: `${colors.red}${colors.bold}`
};

/**
 * Main logger class
 */
class Logger {
  private logDir: string;
  private enableConsole: boolean;
  private currentLogFile: string | null = null;
  private currentLogStream: fs.WriteStream | null = null;
  
  constructor(logDir = './logs', enableConsole = true) {
    this.logDir = logDir;
    this.enableConsole = enableConsole;
    
    // Create log directory if it doesn't exist
    if (!fs.existsSync(this.logDir)) {
      fs.mkdirSync(this.logDir, { recursive: true });
    }
  }
  
  /**
   * Log a message
   */
  log(message: string, options: LogOptions = {}): void {
    const {
      level = LogLevel.INFO,
      module = 'app',
      context = {},
      error,
      timestamp = new Date()
    } = options;
    
    // Format the log entry
    const entry: LogEntry = {
      message,
      level,
      module,
      context,
      timestamp: timestamp.toISOString()
    };
    
    // Add error details if provided
    if (error) {
      entry.error = {
        message: error.message,
        stack: error.stack,
        name: error.name
      };
    }
    
    // Write to file
    this.writeToFile(entry);
    
    // Write to console if enabled
    if (this.enableConsole) {
      this.writeToConsole(entry);
    }
  }
  
  /**
   * Log for a specific severity
   */
  debug(message: string, options: Omit<LogOptions, 'level'> = {}): void {
    this.log(message, { ...options, level: LogLevel.DEBUG });
  }
  
  info(message: string, options: Omit<LogOptions, 'level'> = {}): void {
    this.log(message, { ...options, level: LogLevel.INFO });
  }
  
  warning(message: string, options: Omit<LogOptions, 'level'> = {}): void {
    this.log(message, { ...options, level: LogLevel.WARNING });
  }
  
  error(message: string, options: Omit<LogOptions, 'level'> = {}): void {
    this.log(message, { ...options, level: LogLevel.ERROR });
  }
  
  critical(message: string, options: Omit<LogOptions, 'level'> = {}): void {
    this.log(message, { ...options, level: LogLevel.CRITICAL });
  }
  
  /**
   * Log an error object
   */
  logError(error: Error, options: Omit<LogOptions, 'error'> = {}): void {
    const level = error['severity'] ? 
      severityToLevel[error['severity']] : 
      LogLevel.ERROR;
    
    this.log(
      error.message,
      {
        ...options,
        level,
        error,
        context: {
          ...options.context,
          ...(error['context'] || {})
        }
      }
    );
  }
  
  /**
   * Get the current log file name based on date
   */
  private getLogFileName(): string {
    const date = new Date();
    const formattedDate = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}`;
    return path.join(this.logDir, `app_${formattedDate}.log`);
  }
  
  /**
   * Write log entry to file
   */
  private writeToFile(entry: LogEntry): void {
    try {
      const logFileName = this.getLogFileName();
      
      // If log file has changed, close current stream and open new one
      if (this.currentLogFile !== logFileName) {
        if (this.currentLogStream) {
          this.currentLogStream.end();
        }
        
        this.currentLogFile = logFileName;
        this.currentLogStream = fs.createWriteStream(logFileName, { flags: 'a' });
      }
      
      // Write the entry as JSON
      this.currentLogStream!.write(JSON.stringify(entry) + '\n');
    } catch (err) {
      console.error(`Failed to write to log file: ${err}`);
    }
  }
  
  /**
   * Write log entry to console with colors
   */
  private writeToConsole(entry: LogEntry): void {
    try {
      const levelColor = levelColors[entry.level] || colors.reset;
      const timestamp = entry.timestamp.split('T')[1].replace('Z', '');
      
      // Format: [TIMESTAMP] [LEVEL] [MODULE] MESSAGE
      const prefix = `${colors.cyan}[${timestamp}]${colors.reset} ${levelColor}[${entry.level.toUpperCase()}]${colors.reset} ${colors.blue}[${entry.module}]${colors.reset}`;
      
      // Basic message
      console.log(`${prefix} ${entry.message}`);
      
      // Context (if non-empty and in debug/error modes)
      if (entry.level === LogLevel.DEBUG || entry.level === LogLevel.ERROR || entry.level === LogLevel.CRITICAL) {
        if (Object.keys(entry.context || {}).length > 0) {
          console.log(`${colors.cyan}Context:${colors.reset}`, entry.context);
        }
      }
      
      // Error stack (if present)
      if (entry.error?.stack && (entry.level === LogLevel.ERROR || entry.level === LogLevel.CRITICAL)) {
        console.log(`${colors.red}Stack:${colors.reset} ${entry.error.stack}`);
      }
    } catch (err) {
      console.error(`Failed to write to console: ${err}`);
    }
  }
}

// Create and export a singleton logger instance
export const logger = new Logger();

// Export types for consumers
export {
  LogLevel,
  type LogOptions,
  type LogEntry
};
```

### 3. Create Middleware for API Error Handling

```typescript
// server/middleware/errorHandler.ts

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
        userId: req.user?.id
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
```

### 4. Create Request Logging Middleware

```typescript
// server/middleware/requestLogger.ts

import { Request, Response, NextFunction } from 'express';
import { logger } from '../utils/logger';
import { v4 as uuidv4 } from 'uuid';

// Add request ID to the global Express namespace
declare global {
  namespace Express {
    interface Request {
      id: string;
    }
  }
}

/**
 * Middleware to log API requests
 */
export function requestLogger(req: Request, res: Response, next: NextFunction) {
  // Generate a unique ID for this request
  req.id = uuidv4();
  
  // Log start of request
  const startTime = Date.now();
  
  logger.info(`${req.method} ${req.path}`, {
    module: 'api',
    context: {
      requestId: req.id,
      method: req.method,
      path: req.path,
      query: req.query,
      params: req.params,
      ip: req.ip,
      userAgent: req.headers['user-agent']
    }
  });
  
  // Log when request completes
  res.on('finish', () => {
    const duration = Date.now() - startTime;
    const level = res.statusCode >= 400 ? 'warning' : 'info';
    
    logger[level](`${req.method} ${req.path} ${res.statusCode} - ${duration}ms`, {
      module: 'api',
      context: {
        requestId: req.id,
        method: req.method,
        path: req.path,
        statusCode: res.statusCode,
        duration,
        contentLength: res.get('Content-Length')
      }
    });
  });
  
  next();
}
```

### 5. Implementation Plan

1. **Create Utility Modules**:
   - Create the error handling module
   - Create the logging module
   - Create middleware for API error handling

2. **Update Express Setup**:
   - Add the middleware to the Express application

3. **Update Service Layer**:
   - Refactor database service to use standardized error handling
   - Refactor API services to use standardized error handling
   - Update reconciliation services to use the new logging system

4. **Update Script Entry Points**:
   - Add error handling to CLI scripts
   - Update logging in scheduled tasks

5. **Testing**:
   - Test error scenarios to ensure proper handling
   - Verify log output in various scenarios

### 6. Example Updates

#### Database Connection:

```typescript
// db/index.ts (updated version)

import { drizzle } from 'drizzle-orm/node-postgres';
import { sql } from 'drizzle-orm';
import { Pool } from 'pg';
import { logger } from '../server/utils/logger';
import { DatabaseError } from '../server/utils/errors';

// Create a pool with proper error handling
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: 10,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 15000
});

// Add error handler to pool
pool.on('error', (err) => {
  logger.error('Database pool error', {
    module: 'database',
    error: DatabaseError.fromPgError(err)
  });
});

// Create Drizzle instance with the pool
export const db = drizzle({
  pool,
  // Add query logging for development
  logger: {
    logQuery(query, params) {
      if (process.env.NODE_ENV === 'development') {
        logger.debug('Database query', {
          module: 'database',
          context: { query, params }
        });
      }
    }
  }
});

// Test database connection and return version
export async function testConnection() {
  try {
    const result = await db.execute(sql`SELECT version()`);
    logger.info('Database connection successful', {
      module: 'database',
      context: { version: result.rows[0].version }
    });
    return result.rows[0].version;
  } catch (error) {
    const dbError = DatabaseError.fromPgError(error, { operation: 'testConnection' });
    logger.error('Database connection failed', {
      module: 'database',
      error: dbError
    });
    throw dbError;
  }
}
```

#### API Route Example:

```typescript
// server/routes/optimizedMiningRoutes.ts (updated example)

import express, { Request, Response } from 'express';
import { z } from 'zod';
import { logger } from '../utils/logger';
import { ApiError, ValidationError } from '../utils/errors';
import { getDailyMiningPotential } from '../services/optimizedMiningService';

const router = express.Router();

// Validation schema
const dailyMiningRequestSchema = z.object({
  date: z.string().regex(/^\d{4}-\d{2}-\d{2}$/, "Invalid date format. Use YYYY-MM-DD"),
  minerModel: z.string().min(1, "Miner model is required"),
  farmId: z.string().optional()
});

router.get('/daily', async (req: Request, res: Response) => {
  try {
    // Validate request
    const result = dailyMiningRequestSchema.safeParse(req.query);
    if (!result.success) {
      throw ValidationError.fromZodError(result.error, { query: req.query });
    }
    
    const { date, minerModel, farmId } = result.data;
    
    logger.info(`Processing daily mining potential request`, {
      module: 'mining',
      context: { date, minerModel, farmId }
    });
    
    const data = await getDailyMiningPotential(date, minerModel, farmId);
    return res.json(data);
  } catch (error) {
    // Let global error handler manage this
    if (error instanceof ValidationError || error instanceof ApiError) {
      throw error;
    }
    
    // Wrap unknown errors
    throw new ApiError(
      'Failed to retrieve daily mining potential',
      500,
      {
        context: { query: req.query },
        originalError: error instanceof Error ? error : new Error(String(error))
      }
    );
  }
});

export default router;
```

## Benefits

1. **Consistent Error Handling**: All errors follow the same format and handling pattern
2. **Improved Debugging**: Errors include rich context for faster debugging
3. **Better Logging**: Structured logs that can be easily analyzed
4. **API Error Consistency**: Standardized API error responses
5. **Request Tracing**: Request IDs allow tracing requests through the system

## Success Criteria

This initiative will be successful when:

1. All services use the standardized error handling
2. Logs are consistently formatted and include proper context
3. API errors follow a standardized format
4. Debugging is simplified through improved error context

## Implementation Timeline

1. **Week 1**: Create utility modules and update core infrastructure
2. **Week 2**: Update services and API routes
3. **Week 3**: Update scripts and test error scenarios
4. **Week 4**: Final testing and documentation