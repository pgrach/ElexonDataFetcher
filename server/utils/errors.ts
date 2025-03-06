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
    this.timestamp = new Date();
    this.originalError = options.originalError;
  }
  
  /**
   * Format error for logging
   */
  toLogFormat(): string {
    return `[${this.severity.toUpperCase()}] [${this.category}] ${this.message}`;
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
    const message = error.message || 'Unknown database error';
    const severity = error.code?.startsWith('08') ? 
      ErrorSeverity.CRITICAL : ErrorSeverity.ERROR;
    
    return new DatabaseError(message, {
      severity,
      context: {
        ...context,
        code: error.code,
        detail: error.detail,
        hint: error.hint,
        position: error.position
      },
      originalError: error
    });
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
        timestamp: this.timestamp.toISOString()
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
      severity: options.severity || ErrorSeverity.WARNING
    });
  }
  
  /**
   * Create from Zod error
   */
  static fromZodError(error: any, context: ErrorContext = {}): ValidationError {
    const message = error.errors?.[0]?.message || 'Validation failed';
    
    return new ValidationError(message, {
      context: {
        ...context,
        validationErrors: error.errors
      },
      originalError: error
    });
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
    const message = error.message || `Request to ${url} failed`;
    const isTimeout = message.includes('timeout') || message.includes('ETIMEDOUT');
    
    return new NetworkError(message, {
      severity: isTimeout ? ErrorSeverity.WARNING : ErrorSeverity.ERROR,
      context: {
        ...context,
        url,
        status: error.status || error.response?.status,
        statusText: error.statusText || error.response?.statusText
      },
      originalError: error
    });
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
      category: ErrorCategory.CONFIGURATION,
      severity: options.severity || ErrorSeverity.CRITICAL
    });
  }
}