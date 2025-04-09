"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g = Object.create((typeof Iterator === "function" ? Iterator : Object).prototype);
    return g.next = verb(0), g["throw"] = verb(1), g["return"] = verb(2), typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (g && (g = 0, op[0] && (_ = 0)), _) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
var __spreadArray = (this && this.__spreadArray) || function (to, from, pack) {
    if (pack || arguments.length === 2) for (var i = 0, l = from.length, ar; i < l; i++) {
        if (ar || !(i in from)) {
            if (!ar) ar = Array.prototype.slice.call(from, 0, i);
            ar[i] = from[i];
        }
    }
    return to.concat(ar || Array.prototype.slice.call(from));
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.getDifficultyData = getDifficultyData;
var client_dynamodb_1 = require("@aws-sdk/client-dynamodb");
var lib_dynamodb_1 = require("@aws-sdk/lib-dynamodb");
var bitcoin_1 = require("../types/bitcoin");
var date_fns_1 = require("date-fns");
var MAX_RETRIES = 5;
var RETRY_DELAY_MS = 2000; // Increased from 1000 to 2000ms
// Get table name from environment variable with fallback
var DIFFICULTY_TABLE = process.env.DYNAMODB_DIFFICULTY_TABLE || "asics-dynamodb-DifficultyTable-DQ308ID3POT6";
// Initialize DynamoDB client with better configuration
var client = new client_dynamodb_1.DynamoDBClient({
    region: process.env.AWS_REGION || "us-east-1",
    logger: {
        debug: function () {
            var args = [];
            for (var _i = 0; _i < arguments.length; _i++) {
                args[_i] = arguments[_i];
            }
            return console.debug.apply(console, __spreadArray(['[DynamoDB Debug]'], args, false));
        },
        info: function () {
            var args = [];
            for (var _i = 0; _i < arguments.length; _i++) {
                args[_i] = arguments[_i];
            }
            return console.info.apply(console, __spreadArray(['[DynamoDB Info]'], args, false));
        },
        warn: function () {
            var args = [];
            for (var _i = 0; _i < arguments.length; _i++) {
                args[_i] = arguments[_i];
            }
            return console.warn.apply(console, __spreadArray(['[DynamoDB Warning]'], args, false));
        },
        error: function () {
            var args = [];
            for (var _i = 0; _i < arguments.length; _i++) {
                args[_i] = arguments[_i];
            }
            return console.error.apply(console, __spreadArray(['[DynamoDB Error]'], args, false));
        }
    },
    maxAttempts: MAX_RETRIES,
    retryMode: 'standard'
});
var docClient = lib_dynamodb_1.DynamoDBDocumentClient.from(client, {
    marshallOptions: {
        convertEmptyValues: true,
        removeUndefinedValues: true,
        convertClassInstanceToMap: true,
    },
});
function formatDateForDifficulty(dateStr) {
    try {
        var date = (0, date_fns_1.parse)(dateStr, 'yyyy-MM-dd', new Date());
        return (0, date_fns_1.format)(date, 'yyyy-MM-dd');
    }
    catch (error) {
        console.error("[DynamoDB] Error formatting difficulty date ".concat(dateStr, ":"), error);
        throw new Error("Invalid date format: ".concat(dateStr, ". Expected format: YYYY-MM-DD"));
    }
}
function verifyTableExists(tableName) {
    return __awaiter(this, void 0, void 0, function () {
        var command, response, error_1;
        var _a, _b, _c, _d;
        return __generator(this, function (_e) {
            switch (_e.label) {
                case 0:
                    _e.trys.push([0, 2, , 3]);
                    command = new client_dynamodb_1.DescribeTableCommand({ TableName: tableName });
                    return [4 /*yield*/, client.send(command)];
                case 1:
                    response = _e.sent();
                    console.info("[DynamoDB] Table ".concat(tableName, " status:"), {
                        status: (_a = response.Table) === null || _a === void 0 ? void 0 : _a.TableStatus,
                        itemCount: (_b = response.Table) === null || _b === void 0 ? void 0 : _b.ItemCount,
                        keySchema: (_d = (_c = response.Table) === null || _c === void 0 ? void 0 : _c.KeySchema) === null || _d === void 0 ? void 0 : _d.map(function (k) { return ({ name: k.AttributeName, type: k.KeyType }); })
                    });
                    return [2 /*return*/, true];
                case 2:
                    error_1 = _e.sent();
                    if (error_1 instanceof client_dynamodb_1.ResourceNotFoundException) {
                        console.error("[DynamoDB] Table ".concat(tableName, " does not exist"));
                        return [2 /*return*/, false];
                    }
                    console.error("[DynamoDB] Error verifying table ".concat(tableName, ":"), error_1);
                    throw error_1;
                case 3: return [2 /*return*/];
            }
        });
    });
}
function sleep(ms) {
    return __awaiter(this, void 0, void 0, function () {
        return __generator(this, function (_a) {
            return [2 /*return*/, new Promise(function (resolve) { return setTimeout(resolve, ms); })];
        });
    });
}
function retryOperation(operation_1) {
    return __awaiter(this, arguments, void 0, function (operation, attempt) {
        var error_2, delay, dynamoError;
        if (attempt === void 0) { attempt = 1; }
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    _a.trys.push([0, 2, , 4]);
                    return [4 /*yield*/, operation()];
                case 1: return [2 /*return*/, _a.sent()];
                case 2:
                    error_2 = _a.sent();
                    if (attempt >= MAX_RETRIES) {
                        throw error_2;
                    }
                    delay = Math.min(RETRY_DELAY_MS * Math.pow(2, attempt - 1) + Math.random() * 1000, 30000 // Max delay of 30 seconds
                    );
                    dynamoError = error_2;
                    if (dynamoError.name === 'ProvisionedThroughputExceededException') {
                        console.warn("[DynamoDB] Throughput exceeded on attempt ".concat(attempt, ", waiting ").concat(delay, "ms before retry"));
                    }
                    else {
                        console.warn("[DynamoDB] Attempt ".concat(attempt, " failed, waiting ").concat(delay, "ms:"), error_2);
                    }
                    return [4 /*yield*/, sleep(delay)];
                case 3:
                    _a.sent();
                    return [2 /*return*/, retryOperation(operation, attempt + 1)];
                case 4: return [2 /*return*/];
            }
        });
    });
}
function getDifficultyData(date) {
    return __awaiter(this, void 0, void 0, function () {
        var formattedDate, tableExists, scanCommand_1, scanResponse, sortedItems, difficulty, error_3;
        var _a;
        return __generator(this, function (_b) {
            switch (_b.label) {
                case 0:
                    _b.trys.push([0, 3, , 4]);
                    formattedDate = formatDateForDifficulty(date);
                    console.info("[DynamoDB] Fetching difficulty for date: ".concat(formattedDate));
                    return [4 /*yield*/, verifyTableExists(DIFFICULTY_TABLE)];
                case 1:
                    tableExists = _b.sent();
                    if (!tableExists) {
                        console.warn("[DynamoDB] Table ".concat(DIFFICULTY_TABLE, " does not exist, using default difficulty (").concat(bitcoin_1.DEFAULT_DIFFICULTY, ")"));
                        return [2 /*return*/, bitcoin_1.DEFAULT_DIFFICULTY];
                    }
                    scanCommand_1 = new lib_dynamodb_1.ScanCommand({
                        TableName: DIFFICULTY_TABLE,
                        FilterExpression: "#date = :date",
                        ExpressionAttributeNames: {
                            "#date": "Date"
                        },
                        ExpressionAttributeValues: {
                            ":date": formattedDate
                        }
                    });
                    console.debug('[DynamoDB] Executing difficulty scan:', {
                        table: DIFFICULTY_TABLE,
                        date: formattedDate,
                        command: 'ScanCommand'
                    });
                    return [4 /*yield*/, retryOperation(function () { return docClient.send(scanCommand_1); })];
                case 2:
                    scanResponse = _b.sent();
                    if (!((_a = scanResponse.Items) === null || _a === void 0 ? void 0 : _a.length)) {
                        console.warn("[DynamoDB] No difficulty data found for ".concat(formattedDate, ", using default: ").concat(bitcoin_1.DEFAULT_DIFFICULTY));
                        return [2 /*return*/, bitcoin_1.DEFAULT_DIFFICULTY];
                    }
                    sortedItems = scanResponse.Items.sort(function (a, b) {
                        return b.Date.localeCompare(a.Date);
                    });
                    difficulty = Number(sortedItems[0].Difficulty);
                    console.info("[DynamoDB] Found historical difficulty for ".concat(formattedDate, ":"), {
                        difficulty: difficulty.toLocaleString(),
                        id: sortedItems[0].ID,
                        totalRecords: sortedItems.length
                    });
                    if (isNaN(difficulty)) {
                        console.error("[DynamoDB] Invalid difficulty value:", sortedItems[0].Difficulty);
                        return [2 /*return*/, bitcoin_1.DEFAULT_DIFFICULTY];
                    }
                    return [2 /*return*/, difficulty];
                case 3:
                    error_3 = _b.sent();
                    console.error('[DynamoDB] Error fetching difficulty:', error_3);
                    return [2 /*return*/, bitcoin_1.DEFAULT_DIFFICULTY];
                case 4: return [2 /*return*/];
            }
        });
    });
}
