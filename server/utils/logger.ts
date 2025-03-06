/**
 * Standardized logging system for Bitcoin Mining Analytics platform
 * 
 * This module provides consistent logging patterns, formatting, and output management
 * to improve debugging, monitoring, and error tracking across the application.
 */

import fs from 'fs';
import path from 'path';
import { ErrorSeverity } from './errors';

// Log levels and colors for console output
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

// Map from ErrorSeverity to LogLevel
const severityToLevel = {
  [ErrorSeverity.INFO]: LogLevel.INFO,
  [ErrorSeverity.WARNING]: LogLevel.WARNING,
  [ErrorSeverity.ERROR]: LogLevel.ERROR,
  [ErrorSeverity.CRITICAL]: LogLevel.CRITICAL
} as const;

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
    // Check if the error has severity property and use it to determine log level
    let level = LogLevel.ERROR;
    if ((error as any).severity && 
        typeof (error as any).severity === 'string' && 
        Object.values(ErrorSeverity).includes((error as any).severity)) {
      const severity = (error as any).severity as ErrorSeverity;
      level = severityToLevel[severity] || LogLevel.ERROR;
    }
    
    this.log(
      error.message,
      {
        ...options,
        level,
        error,
        context: {
          ...options.context,
          ...((error as any)['context'] || {})
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

// No need to re-export the types as they are already exported above