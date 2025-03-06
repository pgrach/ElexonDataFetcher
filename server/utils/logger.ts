/**
 * Standardized logging system for Bitcoin Mining Analytics platform
 * 
 * This module provides consistent logging patterns, formatting, and output management
 * to improve debugging, monitoring, and error tracking across the application.
 */

import fs from 'fs';
import path from 'path';

export enum LogLevel {
  DEBUG = 'debug',
  INFO = 'info',
  WARNING = 'warning',
  ERROR = 'error',
  CRITICAL = 'critical'
}

export interface LogOptions {
  level?: LogLevel;
  module?: string;
  context?: Record<string, any>;
  error?: Error;
  timestamp?: Date;
}

export interface LogEntry {
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

// Helper function to get a date format string for file naming
function getDateString(date = new Date()): string {
  return date.toISOString().split('T')[0];
}

// Color codes for console output
const consoleColors = {
  [LogLevel.DEBUG]: '\x1b[36m', // Cyan
  [LogLevel.INFO]: '\x1b[32m',  // Green
  [LogLevel.WARNING]: '\x1b[33m', // Yellow
  [LogLevel.ERROR]: '\x1b[31m', // Red
  [LogLevel.CRITICAL]: '\x1b[41m\x1b[37m', // White on Red background
  reset: '\x1b[0m'
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
    const timestamp = options.timestamp || new Date();
    
    const entry: LogEntry = {
      message,
      level: options.level || LogLevel.INFO,
      module: options.module || 'app',
      context: options.context,
      timestamp: timestamp.toISOString()
    };
    
    // Add error information if provided
    if (options.error) {
      entry.error = {
        message: options.error.message,
        stack: options.error.stack,
        name: options.error.name
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
    this.error(error.message, { ...options, error });
  }
  
  /**
   * Get the current log file name based on date
   */
  private getLogFileName(module?: string): string {
    const date = getDateString();
    const fileName = module ? 
      `${module}_${date}.log` : 
      `app_${date}.log`;
    
    return path.join(this.logDir, fileName);
  }
  
  /**
   * Write log entry to file
   */
  private writeToFile(entry: LogEntry): void {
    try {
      // Get the log file path based on the module
      const logFile = this.getLogFileName(entry.module);
      
      // If the log file has changed, close the current stream
      if (this.currentLogFile !== logFile && this.currentLogStream) {
        this.currentLogStream.end();
        this.currentLogStream = null;
      }
      
      // Open a new stream if needed
      if (!this.currentLogStream) {
        this.currentLogFile = logFile;
        this.currentLogStream = fs.createWriteStream(logFile, { flags: 'a' });
      }
      
      // Convert entry to a string, pretty-print with newlines and tabs
      const entryString = JSON.stringify(entry, null, 2);
      
      // Write to the log file
      this.currentLogStream.write(`${entryString}\n`);
    } catch (err) {
      // If logging to file fails, fallback to console
      console.error('Failed to write to log file:', err);
      console.error('Original log entry:', entry);
    }
  }
  
  /**
   * Write log entry to console with colors
   */
  private writeToConsole(entry: LogEntry): void {
    const color = consoleColors[entry.level] || '';
    const timestamp = new Date(entry.timestamp).toLocaleTimeString();
    
    // Format the main log line
    console.log(
      `${color}[${entry.level.toUpperCase()}]${consoleColors.reset} ` +
      `[${timestamp}] ` +
      `[${entry.module}] ${entry.message}`
    );
    
    // If there's context, print it indented
    if (entry.context && Object.keys(entry.context).length > 0) {
      console.log('  Context:', entry.context);
    }
    
    // If there's an error, print the stack trace
    if (entry.error?.stack) {
      console.log('  Error Stack:', entry.error.stack);
    }
  }
}

export const logger = new Logger();

// For convenience, export the logger methods directly
export const debug = logger.debug.bind(logger);
export const info = logger.info.bind(logger);
export const warning = logger.warning.bind(logger);
export const error = logger.error.bind(logger);
export const critical = logger.critical.bind(logger);
export const logError = logger.logError.bind(logger);