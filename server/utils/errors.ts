/**
 * Standard error handling utilities for the Bitcoin Mining Analytics platform
 * 
 * This module provides a standardized approach to error handling across the application,
 * with consistent error classification, context enrichment, and formatting.
 */

export enum ErrorSeverity {
  INFO = 'info',
  WARNING = 'warning',
  ERROR = 'error',
  CRITICAL = 'critical'
}

export enum ErrorCategory {
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

export type ErrorContext = Record<string, any>;

export interface ErrorOptions {
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
 * Network-related error
 */
export class NetworkError extends AppError {
  constructor(message: string, options: ErrorOptions = {}) {
    super(message, {
      ...options,
      category: ErrorCategory.NETWORK
    });
  }
  
  /**
   * Create from fetch/axios error
   */
  static fromFetchError(error: any, url: string, context: ErrorContext = {}): NetworkError {
    return new NetworkError(
      `Network error: ${error.message}`,
      {
        context: {
          ...context,
          url,
          status: error.status || error.response?.status,
          statusText: error.statusText || error.response?.statusText
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

/**
 * Bitcoin calculation error
 */
export class CalculationError extends AppError {
  constructor(message: string, options: ErrorOptions = {}) {
    super(message, {
      ...options,
      category: ErrorCategory.CALCULATION
    });
  }
}

/**
 * Configuration error
 */
export class ConfigurationError extends AppError {
  constructor(message: string, options: ErrorOptions = {}) {
    super(message, {
      ...options,
      category: ErrorCategory.CONFIGURATION
    });
  }
}