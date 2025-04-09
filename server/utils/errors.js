"use strict";
/**
 * Standard error handling utilities for the Bitcoin Mining Analytics platform
 *
 * This module provides a standardized approach to error handling across the application,
 * with consistent error classification, context enrichment, and formatting.
 */
var __extends = (this && this.__extends) || (function () {
    var extendStatics = function (d, b) {
        extendStatics = Object.setPrototypeOf ||
            ({ __proto__: [] } instanceof Array && function (d, b) { d.__proto__ = b; }) ||
            function (d, b) { for (var p in b) if (Object.prototype.hasOwnProperty.call(b, p)) d[p] = b[p]; };
        return extendStatics(d, b);
    };
    return function (d, b) {
        if (typeof b !== "function" && b !== null)
            throw new TypeError("Class extends value " + String(b) + " is not a constructor or null");
        extendStatics(d, b);
        function __() { this.constructor = d; }
        d.prototype = b === null ? Object.create(b) : (__.prototype = b.prototype, new __());
    };
})();
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
Object.defineProperty(exports, "__esModule", { value: true });
exports.ConfigurationError = exports.CalculationError = exports.ReconciliationError = exports.NetworkError = exports.ValidationError = exports.ApiError = exports.DatabaseError = exports.AppError = exports.ErrorCategory = exports.ErrorSeverity = void 0;
var ErrorSeverity;
(function (ErrorSeverity) {
    ErrorSeverity["INFO"] = "info";
    ErrorSeverity["WARNING"] = "warning";
    ErrorSeverity["ERROR"] = "error";
    ErrorSeverity["CRITICAL"] = "critical";
})(ErrorSeverity || (exports.ErrorSeverity = ErrorSeverity = {}));
var ErrorCategory;
(function (ErrorCategory) {
    ErrorCategory["DATABASE"] = "database";
    ErrorCategory["API"] = "api";
    ErrorCategory["VALIDATION"] = "validation";
    ErrorCategory["NETWORK"] = "network";
    ErrorCategory["RECONCILIATION"] = "reconciliation";
    ErrorCategory["CALCULATION"] = "calculation";
    ErrorCategory["CONFIGURATION"] = "configuration";
    ErrorCategory["AUTHENTICATION"] = "authentication";
    ErrorCategory["UNKNOWN"] = "unknown";
})(ErrorCategory || (exports.ErrorCategory = ErrorCategory = {}));
/**
 * Base application error class with standardized properties
 */
var AppError = /** @class */ (function (_super) {
    __extends(AppError, _super);
    function AppError(message, options) {
        if (options === void 0) { options = {}; }
        var _this = _super.call(this, message) || this;
        _this.name = _this.constructor.name;
        _this.severity = options.severity || ErrorSeverity.ERROR;
        _this.category = options.category || ErrorCategory.UNKNOWN;
        _this.context = options.context || {};
        _this.timestamp = new Date();
        _this.originalError = options.originalError;
        return _this;
    }
    /**
     * Format error for logging
     */
    AppError.prototype.toLogFormat = function () {
        return "[".concat(this.severity.toUpperCase(), "] [").concat(this.category, "] ").concat(this.message);
    };
    return AppError;
}(Error));
exports.AppError = AppError;
/**
 * Database-related error
 */
var DatabaseError = /** @class */ (function (_super) {
    __extends(DatabaseError, _super);
    function DatabaseError(message, options) {
        if (options === void 0) { options = {}; }
        return _super.call(this, message, __assign(__assign({}, options), { category: ErrorCategory.DATABASE })) || this;
    }
    /**
     * Create DatabaseError from pg error
     */
    DatabaseError.fromPgError = function (error, context) {
        var _a;
        if (context === void 0) { context = {}; }
        var message = error.message || 'Unknown database error';
        var severity = ((_a = error.code) === null || _a === void 0 ? void 0 : _a.startsWith('08')) ?
            ErrorSeverity.CRITICAL : ErrorSeverity.ERROR;
        return new DatabaseError(message, {
            severity: severity,
            context: __assign(__assign({}, context), { code: error.code, detail: error.detail, hint: error.hint, position: error.position }),
            originalError: error
        });
    };
    return DatabaseError;
}(AppError));
exports.DatabaseError = DatabaseError;
/**
 * API-related error
 */
var ApiError = /** @class */ (function (_super) {
    __extends(ApiError, _super);
    function ApiError(message, statusCode, options) {
        if (statusCode === void 0) { statusCode = 500; }
        if (options === void 0) { options = {}; }
        var _this = _super.call(this, message, __assign(__assign({}, options), { category: ErrorCategory.API })) || this;
        _this.statusCode = statusCode;
        return _this;
    }
    /**
     * Format as API response
     */
    ApiError.prototype.toResponse = function () {
        return {
            error: {
                message: this.message,
                type: this.name,
                statusCode: this.statusCode,
                timestamp: this.timestamp.toISOString()
            }
        };
    };
    return ApiError;
}(AppError));
exports.ApiError = ApiError;
/**
 * Validation error
 */
var ValidationError = /** @class */ (function (_super) {
    __extends(ValidationError, _super);
    function ValidationError(message, options) {
        if (options === void 0) { options = {}; }
        return _super.call(this, message, __assign(__assign({}, options), { category: ErrorCategory.VALIDATION, severity: options.severity || ErrorSeverity.WARNING })) || this;
    }
    /**
     * Create from Zod error
     */
    ValidationError.fromZodError = function (error, context) {
        var _a, _b;
        if (context === void 0) { context = {}; }
        var message = ((_b = (_a = error.errors) === null || _a === void 0 ? void 0 : _a[0]) === null || _b === void 0 ? void 0 : _b.message) || 'Validation failed';
        return new ValidationError(message, {
            context: __assign(__assign({}, context), { validationErrors: error.errors }),
            originalError: error
        });
    };
    return ValidationError;
}(AppError));
exports.ValidationError = ValidationError;
/**
 * Network-related error
 */
var NetworkError = /** @class */ (function (_super) {
    __extends(NetworkError, _super);
    function NetworkError(message, options) {
        if (options === void 0) { options = {}; }
        return _super.call(this, message, __assign(__assign({}, options), { category: ErrorCategory.NETWORK })) || this;
    }
    /**
     * Create from fetch/axios error
     */
    NetworkError.fromFetchError = function (error, url, context) {
        var _a, _b;
        if (context === void 0) { context = {}; }
        var message = error.message || "Request to ".concat(url, " failed");
        var isTimeout = message.includes('timeout') || message.includes('ETIMEDOUT');
        return new NetworkError(message, {
            severity: isTimeout ? ErrorSeverity.WARNING : ErrorSeverity.ERROR,
            context: __assign(__assign({}, context), { url: url, status: error.status || ((_a = error.response) === null || _a === void 0 ? void 0 : _a.status), statusText: error.statusText || ((_b = error.response) === null || _b === void 0 ? void 0 : _b.statusText) }),
            originalError: error
        });
    };
    return NetworkError;
}(AppError));
exports.NetworkError = NetworkError;
/**
 * Reconciliation-specific error
 */
var ReconciliationError = /** @class */ (function (_super) {
    __extends(ReconciliationError, _super);
    function ReconciliationError(message, options) {
        if (options === void 0) { options = {}; }
        return _super.call(this, message, __assign(__assign({}, options), { category: ErrorCategory.RECONCILIATION })) || this;
    }
    return ReconciliationError;
}(AppError));
exports.ReconciliationError = ReconciliationError;
/**
 * Bitcoin calculation error
 */
var CalculationError = /** @class */ (function (_super) {
    __extends(CalculationError, _super);
    function CalculationError(message, options) {
        if (options === void 0) { options = {}; }
        return _super.call(this, message, __assign(__assign({}, options), { category: ErrorCategory.CALCULATION })) || this;
    }
    return CalculationError;
}(AppError));
exports.CalculationError = CalculationError;
/**
 * Configuration error
 */
var ConfigurationError = /** @class */ (function (_super) {
    __extends(ConfigurationError, _super);
    function ConfigurationError(message, options) {
        if (options === void 0) { options = {}; }
        return _super.call(this, message, __assign(__assign({}, options), { category: ErrorCategory.CONFIGURATION, severity: options.severity || ErrorSeverity.CRITICAL })) || this;
    }
    return ConfigurationError;
}(AppError));
exports.ConfigurationError = ConfigurationError;
