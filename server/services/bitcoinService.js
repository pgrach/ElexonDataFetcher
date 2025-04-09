"use strict";
/**
 * Bitcoin Service
 *
 * This service handles Bitcoin calculations and summary updates.
 */
var __makeTemplateObject = (this && this.__makeTemplateObject) || function (cooked, raw) {
    if (Object.defineProperty) { Object.defineProperty(cooked, "raw", { value: raw }); } else { cooked.raw = raw; }
    return cooked;
};
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
Object.defineProperty(exports, "__esModule", { value: true });
exports.processSingleDay = processSingleDay;
exports.processHistoricalCalculations = processHistoricalCalculations;
exports.calculateMonthlyBitcoinSummary = calculateMonthlyBitcoinSummary;
exports.manualUpdateYearlyBitcoinSummary = manualUpdateYearlyBitcoinSummary;
var db_1 = require("../../db");
var schema_1 = require("../../db/schema");
var drizzle_orm_1 = require("drizzle-orm");
var bitcoin_1 = require("../utils/bitcoin");
var dynamodbService_1 = require("./dynamodbService");
/**
 * Process Bitcoin calculations for a single day and miner model
 *
 * @param date - The settlement date in format 'YYYY-MM-DD'
 * @param minerModel - The miner model (e.g., 'S19J_PRO', 'S9', 'M20S')
 */
function processSingleDay(date, minerModel) {
    return __awaiter(this, void 0, void 0, function () {
        var difficulty, records, totalBitcoin, insertPromises, _i, records_1, record, mwh, bitcoinMined, yearMonth, year, error_1;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    _a.trys.push([0, 7, , 8]);
                    console.log("Processing Bitcoin calculations for ".concat(date, " with miner model ").concat(minerModel));
                    return [4 /*yield*/, (0, dynamodbService_1.getDifficultyData)(date)];
                case 1:
                    difficulty = _a.sent();
                    console.log("Using difficulty ".concat(difficulty, " for ").concat(date));
                    // Step 2: Delete any existing calculations for this date and model to avoid duplicates
                    return [4 /*yield*/, db_1.db.delete(schema_1.historicalBitcoinCalculations)
                            .where((0, drizzle_orm_1.and)((0, drizzle_orm_1.eq)(schema_1.historicalBitcoinCalculations.settlementDate, date), (0, drizzle_orm_1.eq)(schema_1.historicalBitcoinCalculations.minerModel, minerModel)))];
                case 2:
                    // Step 2: Delete any existing calculations for this date and model to avoid duplicates
                    _a.sent();
                    console.log("Deleted existing calculations for ".concat(date, " and ").concat(minerModel));
                    return [4 /*yield*/, db_1.db.select({
                            settlementPeriod: schema_1.curtailmentRecords.settlementPeriod,
                            farmId: schema_1.curtailmentRecords.farmId,
                            leadPartyName: schema_1.curtailmentRecords.leadPartyName,
                            volume: schema_1.curtailmentRecords.volume
                        })
                            .from(schema_1.curtailmentRecords)
                            .where((0, drizzle_orm_1.eq)(schema_1.curtailmentRecords.settlementDate, date))];
                case 3:
                    records = _a.sent();
                    if (records.length === 0) {
                        console.log("No curtailment records found for ".concat(date));
                        return [2 /*return*/];
                    }
                    console.log("Found ".concat(records.length, " curtailment records for ").concat(date));
                    totalBitcoin = 0;
                    insertPromises = [];
                    for (_i = 0, records_1 = records; _i < records_1.length; _i++) {
                        record = records_1[_i];
                        mwh = Math.abs(Number(record.volume));
                        // Skip records with zero or invalid volume
                        if (mwh <= 0 || isNaN(mwh)) {
                            continue;
                        }
                        bitcoinMined = (0, bitcoin_1.calculateBitcoin)(mwh, minerModel, difficulty);
                        totalBitcoin += bitcoinMined;
                        // Insert the calculation record
                        insertPromises.push(db_1.db.insert(schema_1.historicalBitcoinCalculations).values({
                            settlementDate: date,
                            settlementPeriod: Number(record.settlementPeriod),
                            minerModel: minerModel,
                            farmId: record.farmId,
                            bitcoinMined: bitcoinMined.toString(),
                            difficulty: difficulty.toString()
                        }));
                    }
                    // Execute all inserts
                    return [4 /*yield*/, Promise.all(insertPromises)];
                case 4:
                    // Execute all inserts
                    _a.sent();
                    console.log("Successfully processed ".concat(insertPromises.length, " Bitcoin calculations for ").concat(date, " and ").concat(minerModel));
                    console.log("Total Bitcoin calculated: ".concat(totalBitcoin.toFixed(8)));
                    yearMonth = date.substring(0, 7);
                    return [4 /*yield*/, calculateMonthlyBitcoinSummary(yearMonth, minerModel)];
                case 5:
                    _a.sent();
                    year = date.substring(0, 4);
                    return [4 /*yield*/, manualUpdateYearlyBitcoinSummary(year)];
                case 6:
                    _a.sent();
                    return [3 /*break*/, 8];
                case 7:
                    error_1 = _a.sent();
                    console.error("Error processing Bitcoin calculations for ".concat(date, " and ").concat(minerModel, ":"), error_1);
                    throw error_1;
                case 8: return [2 /*return*/];
            }
        });
    });
}
/**
 * Process historical calculations for a date range and multiple miner models
 *
 * @param startDate - Start date in YYYY-MM-DD format
 * @param endDate - End date in YYYY-MM-DD format
 * @param minerModels - Array of miner models to process
 */
function processHistoricalCalculations(startDate, endDate, minerModels) {
    return __awaiter(this, void 0, void 0, function () {
        var start, end, dates, current, _i, dates_1, date, _a, minerModels_1, minerModel, error_2;
        return __generator(this, function (_b) {
            switch (_b.label) {
                case 0:
                    _b.trys.push([0, 7, , 8]);
                    console.log("Processing historical calculations from ".concat(startDate, " to ").concat(endDate));
                    start = new Date(startDate);
                    end = new Date(endDate);
                    dates = [];
                    current = new Date(start);
                    while (current <= end) {
                        dates.push(current.toISOString().split('T')[0]);
                        current.setDate(current.getDate() + 1);
                    }
                    console.log("Processing ".concat(dates.length, " days and ").concat(minerModels.length, " miner models"));
                    _i = 0, dates_1 = dates;
                    _b.label = 1;
                case 1:
                    if (!(_i < dates_1.length)) return [3 /*break*/, 6];
                    date = dates_1[_i];
                    _a = 0, minerModels_1 = minerModels;
                    _b.label = 2;
                case 2:
                    if (!(_a < minerModels_1.length)) return [3 /*break*/, 5];
                    minerModel = minerModels_1[_a];
                    return [4 /*yield*/, processSingleDay(date, minerModel)];
                case 3:
                    _b.sent();
                    _b.label = 4;
                case 4:
                    _a++;
                    return [3 /*break*/, 2];
                case 5:
                    _i++;
                    return [3 /*break*/, 1];
                case 6:
                    console.log("Successfully processed historical calculations from ".concat(startDate, " to ").concat(endDate));
                    return [3 /*break*/, 8];
                case 7:
                    error_2 = _b.sent();
                    console.error("Error processing historical calculations:", error_2);
                    throw error_2;
                case 8: return [2 /*return*/];
            }
        });
    });
}
/**
 * Calculate Monthly Bitcoin Summary
 *
 * @param yearMonth - Year and month in format 'YYYY-MM'
 * @param minerModel - Miner model (e.g., 'S19J_PRO')
 */
function calculateMonthlyBitcoinSummary(yearMonth, minerModel) {
    return __awaiter(this, void 0, void 0, function () {
        var _a, year, month, startDate, endDate, result, data, yearMonth2, error_3;
        return __generator(this, function (_b) {
            switch (_b.label) {
                case 0:
                    _b.trys.push([0, 4, , 5]);
                    console.log("Calculating monthly Bitcoin summary for ".concat(yearMonth, " and ").concat(minerModel, "..."));
                    _a = yearMonth.split('-'), year = _a[0], month = _a[1];
                    if (!year || !month) {
                        throw new Error("Invalid year-month format: ".concat(yearMonth, ", expected 'YYYY-MM'"));
                    }
                    startDate = "".concat(year, "-").concat(month, "-01");
                    endDate = new Date(Number(year), Number(month), 0).toISOString().split('T')[0];
                    return [4 /*yield*/, db_1.db.execute((0, drizzle_orm_1.sql)(templateObject_1 || (templateObject_1 = __makeTemplateObject(["\n      SELECT\n        SUM(bitcoin_mined::NUMERIC) as total_bitcoin,\n        COUNT(DISTINCT settlement_date) as days_count,\n        MIN(settlement_date) as first_date,\n        MAX(settlement_date) as last_date\n      FROM\n        historical_bitcoin_calculations\n      WHERE\n        settlement_date >= ", "\n        AND settlement_date <= ", "\n        AND miner_model = ", "\n    "], ["\n      SELECT\n        SUM(bitcoin_mined::NUMERIC) as total_bitcoin,\n        COUNT(DISTINCT settlement_date) as days_count,\n        MIN(settlement_date) as first_date,\n        MAX(settlement_date) as last_date\n      FROM\n        historical_bitcoin_calculations\n      WHERE\n        settlement_date >= ", "\n        AND settlement_date <= ", "\n        AND miner_model = ", "\n    "])), startDate, endDate, minerModel))];
                case 1:
                    result = _b.sent();
                    data = result[0];
                    if (!data || !data.total_bitcoin) {
                        console.log("No Bitcoin data found for ".concat(yearMonth, " and ").concat(minerModel));
                        return [2 /*return*/];
                    }
                    yearMonth2 = "".concat(year, "-").concat(month);
                    return [4 /*yield*/, db_1.db.execute((0, drizzle_orm_1.sql)(templateObject_2 || (templateObject_2 = __makeTemplateObject(["\n      DELETE FROM bitcoin_monthly_summaries\n      WHERE year_month = ", "\n      AND miner_model = ", "\n    "], ["\n      DELETE FROM bitcoin_monthly_summaries\n      WHERE year_month = ", "\n      AND miner_model = ", "\n    "])), yearMonth2, minerModel))];
                case 2:
                    _b.sent();
                    // Insert new summary
                    return [4 /*yield*/, db_1.db.insert(schema_1.bitcoinMonthlySummaries).values({
                            yearMonth: yearMonth,
                            minerModel: minerModel,
                            bitcoinMined: data.total_bitcoin.toString(),
                            updatedAt: new Date()
                        })];
                case 3:
                    // Insert new summary
                    _b.sent();
                    console.log("Monthly Bitcoin summary updated for ".concat(yearMonth, " and ").concat(minerModel, ": ").concat(data.total_bitcoin, " BTC"));
                    return [3 /*break*/, 5];
                case 4:
                    error_3 = _b.sent();
                    console.error("Error calculating monthly Bitcoin summary:", error_3);
                    throw error_3;
                case 5: return [2 /*return*/];
            }
        });
    });
}
/**
 * Update Yearly Bitcoin Summary
 *
 * @param year - Year in format 'YYYY'
 */
function manualUpdateYearlyBitcoinSummary(year) {
    return __awaiter(this, void 0, void 0, function () {
        var yearPrefix, minerModelsResult, minerModels, i, row, _i, minerModels_2, minerModel, monthlyResult, data, tableExistsResult, tableExists, error_4;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    _a.trys.push([0, 9, , 10]);
                    console.log("Updating yearly Bitcoin summary for ".concat(year, "..."));
                    yearPrefix = "".concat(year, "-");
                    return [4 /*yield*/, db_1.db.execute((0, drizzle_orm_1.sql)(templateObject_3 || (templateObject_3 = __makeTemplateObject(["\n      SELECT DISTINCT miner_model\n      FROM bitcoin_monthly_summaries\n      WHERE year_month LIKE ", "\n    "], ["\n      SELECT DISTINCT miner_model\n      FROM bitcoin_monthly_summaries\n      WHERE year_month LIKE ", "\n    "])), yearPrefix + '%'))];
                case 1:
                    minerModelsResult = _a.sent();
                    minerModels = [];
                    for (i = 0; i < minerModelsResult.length; i++) {
                        row = minerModelsResult[i];
                        if (row.miner_model) {
                            minerModels.push(row.miner_model);
                        }
                    }
                    if (minerModels.length === 0) {
                        console.log("No miner models found for ".concat(year));
                        return [2 /*return*/];
                    }
                    console.log("Found ".concat(minerModels.length, " miner models: ").concat(minerModels.join(', ')));
                    _i = 0, minerModels_2 = minerModels;
                    _a.label = 2;
                case 2:
                    if (!(_i < minerModels_2.length)) return [3 /*break*/, 8];
                    minerModel = minerModels_2[_i];
                    return [4 /*yield*/, db_1.db.execute((0, drizzle_orm_1.sql)(templateObject_4 || (templateObject_4 = __makeTemplateObject(["\n        SELECT\n          SUM(bitcoin_mined::NUMERIC) as total_bitcoin,\n          COUNT(*) as months_count\n        FROM\n          bitcoin_monthly_summaries\n        WHERE\n          year_month LIKE ", "\n          AND miner_model = ", "\n      "], ["\n        SELECT\n          SUM(bitcoin_mined::NUMERIC) as total_bitcoin,\n          COUNT(*) as months_count\n        FROM\n          bitcoin_monthly_summaries\n        WHERE\n          year_month LIKE ", "\n          AND miner_model = ", "\n      "])), yearPrefix + '%', minerModel))];
                case 3:
                    monthlyResult = _a.sent();
                    data = null;
                    if (monthlyResult.length > 0) {
                        data = monthlyResult[0];
                    }
                    if (!data || !data.total_bitcoin) {
                        console.log("No monthly summary data found for ".concat(year, " and ").concat(minerModel));
                        return [3 /*break*/, 7];
                    }
                    return [4 /*yield*/, db_1.db.execute((0, drizzle_orm_1.sql)(templateObject_5 || (templateObject_5 = __makeTemplateObject(["\n        SELECT EXISTS (\n          SELECT FROM information_schema.tables \n          WHERE table_name = 'bitcoin_yearly_summaries'\n        ) as exists\n      "], ["\n        SELECT EXISTS (\n          SELECT FROM information_schema.tables \n          WHERE table_name = 'bitcoin_yearly_summaries'\n        ) as exists\n      "]))))];
                case 4:
                    tableExistsResult = _a.sent();
                    tableExists = tableExistsResult[0] && tableExistsResult[0].exists === true;
                    if (!tableExists) {
                        console.log("Warning: bitcoin_yearly_summaries table doesn't exist. Skipping yearly summary update.");
                        return [3 /*break*/, 7];
                    }
                    // Delete existing yearly summary if any
                    return [4 /*yield*/, db_1.db.execute((0, drizzle_orm_1.sql)(templateObject_6 || (templateObject_6 = __makeTemplateObject(["\n        DELETE FROM bitcoin_yearly_summaries\n        WHERE year = ", "\n        AND miner_model = ", "\n      "], ["\n        DELETE FROM bitcoin_yearly_summaries\n        WHERE year = ", "\n        AND miner_model = ", "\n      "])), year, minerModel))];
                case 5:
                    // Delete existing yearly summary if any
                    _a.sent();
                    // Insert new yearly summary
                    return [4 /*yield*/, db_1.db.execute((0, drizzle_orm_1.sql)(templateObject_7 || (templateObject_7 = __makeTemplateObject(["\n        INSERT INTO bitcoin_yearly_summaries \n        (year, miner_model, bitcoin_mined, updated_at)\n        VALUES (\n          ", ",\n          ", ",\n          ", ",\n          ", "\n        )\n      "], ["\n        INSERT INTO bitcoin_yearly_summaries \n        (year, miner_model, bitcoin_mined, updated_at)\n        VALUES (\n          ", ",\n          ", ",\n          ", ",\n          ", "\n        )\n      "])), year, minerModel, data.total_bitcoin.toString(), new Date().toISOString()))];
                case 6:
                    // Insert new yearly summary
                    _a.sent();
                    console.log("Yearly Bitcoin summary updated for ".concat(year, " and ").concat(minerModel, ": ").concat(data.total_bitcoin, " BTC"));
                    _a.label = 7;
                case 7:
                    _i++;
                    return [3 /*break*/, 2];
                case 8:
                    console.log("Yearly Bitcoin summary update completed for ".concat(year));
                    return [3 /*break*/, 10];
                case 9:
                    error_4 = _a.sent();
                    console.error("Error updating yearly Bitcoin summary:", error_4);
                    throw error_4;
                case 10: return [2 /*return*/];
            }
        });
    });
}
var templateObject_1, templateObject_2, templateObject_3, templateObject_4, templateObject_5, templateObject_6, templateObject_7;
