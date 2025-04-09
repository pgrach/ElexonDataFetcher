"use strict";
/**
 * Standardized logging system for Bitcoin Mining Analytics platform
 *
 * This module provides consistent logging patterns, formatting, and output management
 * to improve debugging, monitoring, and error tracking across the application.
 */
var __assign = (this && this.__assign) || function () {
    __assign = Object.assign || function(t) {
        for (var s, i = 1, n = arguments.length; i < n; i++) {
            s = arguments[i];
            for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p))
                t[p] = s[p];
        }
        return t;
    };
    return __assign.apply(this, arguments);
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
var _a;
Object.defineProperty(exports, "__esModule", { value: true });
exports.logError = exports.critical = exports.error = exports.warning = exports.info = exports.debug = exports.logger = exports.LogLevel = void 0;
var fs_1 = __importDefault(require("fs"));
var path_1 = __importDefault(require("path"));
var LogLevel;
(function (LogLevel) {
    LogLevel["DEBUG"] = "debug";
    LogLevel["INFO"] = "info";
    LogLevel["WARNING"] = "warning";
    LogLevel["ERROR"] = "error";
    LogLevel["CRITICAL"] = "critical";
})(LogLevel || (exports.LogLevel = LogLevel = {}));
// Helper function to get a date format string for file naming
function getDateString(date) {
    if (date === void 0) { date = new Date(); }
    return date.toISOString().split('T')[0];
}
// Color codes for console output
var consoleColors = (_a = {},
    _a[LogLevel.DEBUG] = '\x1b[36m',
    _a[LogLevel.INFO] = '\x1b[32m',
    _a[LogLevel.WARNING] = '\x1b[33m',
    _a[LogLevel.ERROR] = '\x1b[31m',
    _a[LogLevel.CRITICAL] = '\x1b[41m\x1b[37m',
    _a.reset = '\x1b[0m',
    _a);
/**
 * Main logger class
 */
var Logger = /** @class */ (function () {
    function Logger(logDir, enableConsole) {
        if (logDir === void 0) { logDir = './logs'; }
        if (enableConsole === void 0) { enableConsole = true; }
        this.currentLogFile = null;
        this.currentLogStream = null;
        this.logDir = logDir;
        this.enableConsole = enableConsole;
        // Create log directory if it doesn't exist
        if (!fs_1.default.existsSync(this.logDir)) {
            fs_1.default.mkdirSync(this.logDir, { recursive: true });
        }
    }
    /**
     * Log a message
     */
    Logger.prototype.log = function (message, options) {
        if (options === void 0) { options = {}; }
        var timestamp = options.timestamp || new Date();
        var entry = {
            message: message,
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
    };
    /**
     * Log for a specific severity
     */
    Logger.prototype.debug = function (message, options) {
        if (options === void 0) { options = {}; }
        this.log(message, __assign(__assign({}, options), { level: LogLevel.DEBUG }));
    };
    Logger.prototype.info = function (message, options) {
        if (options === void 0) { options = {}; }
        this.log(message, __assign(__assign({}, options), { level: LogLevel.INFO }));
    };
    Logger.prototype.warning = function (message, options) {
        if (options === void 0) { options = {}; }
        this.log(message, __assign(__assign({}, options), { level: LogLevel.WARNING }));
    };
    Logger.prototype.error = function (message, options) {
        if (options === void 0) { options = {}; }
        this.log(message, __assign(__assign({}, options), { level: LogLevel.ERROR }));
    };
    Logger.prototype.critical = function (message, options) {
        if (options === void 0) { options = {}; }
        this.log(message, __assign(__assign({}, options), { level: LogLevel.CRITICAL }));
    };
    /**
     * Log an error object
     */
    Logger.prototype.logError = function (error, options) {
        if (options === void 0) { options = {}; }
        this.error(error.message, __assign(__assign({}, options), { error: error }));
    };
    /**
     * Get the current log file name based on date
     */
    Logger.prototype.getLogFileName = function (module) {
        var date = getDateString();
        var fileName = module ?
            "".concat(module, "_").concat(date, ".log") :
            "app_".concat(date, ".log");
        return path_1.default.join(this.logDir, fileName);
    };
    /**
     * Write log entry to file
     */
    Logger.prototype.writeToFile = function (entry) {
        try {
            // Get the log file path based on the module
            var logFile = this.getLogFileName(entry.module);
            // If the log file has changed, close the current stream
            if (this.currentLogFile !== logFile && this.currentLogStream) {
                this.currentLogStream.end();
                this.currentLogStream = null;
            }
            // Open a new stream if needed
            if (!this.currentLogStream) {
                this.currentLogFile = logFile;
                this.currentLogStream = fs_1.default.createWriteStream(logFile, { flags: 'a' });
            }
            // Convert entry to a string, pretty-print with newlines and tabs
            var entryString = JSON.stringify(entry, null, 2);
            // Write to the log file
            this.currentLogStream.write("".concat(entryString, "\n"));
        }
        catch (err) {
            // If logging to file fails, fallback to console
            console.error('Failed to write to log file:', err);
            console.error('Original log entry:', entry);
        }
    };
    /**
     * Write log entry to console with colors
     */
    Logger.prototype.writeToConsole = function (entry) {
        var _a;
        var color = consoleColors[entry.level] || '';
        var timestamp = new Date(entry.timestamp).toLocaleTimeString();
        // Format the main log line
        console.log("".concat(color, "[").concat(entry.level.toUpperCase(), "]").concat(consoleColors.reset, " ") +
            "[".concat(timestamp, "] ") +
            "[".concat(entry.module, "] ").concat(entry.message));
        // If there's context, print it indented
        if (entry.context && Object.keys(entry.context).length > 0) {
            console.log('  Context:', entry.context);
        }
        // If there's an error, print the stack trace
        if ((_a = entry.error) === null || _a === void 0 ? void 0 : _a.stack) {
            console.log('  Error Stack:', entry.error.stack);
        }
    };
    return Logger;
}());
exports.logger = new Logger();
// For convenience, export the logger methods directly
exports.debug = exports.logger.debug.bind(exports.logger);
exports.info = exports.logger.info.bind(exports.logger);
exports.warning = exports.logger.warning.bind(exports.logger);
exports.error = exports.logger.error.bind(exports.logger);
exports.critical = exports.logger.critical.bind(exports.logger);
exports.logError = exports.logger.logError.bind(exports.logger);
