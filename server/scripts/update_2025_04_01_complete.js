"use strict";
/**
 * Complete Update Script for 2025-04-01
 *
 * This script performs a full data update for 2025-04-01 including:
 * 1. Reingesting curtailment records from Elexon API
 * 2. Updating daily summary
 * 3. Updating monthly summary (April 2025)
 * 4. Updating yearly summary (2025)
 * 5. Updating Bitcoin calculation tables
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
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.reingestCurtailmentRecords = reingestCurtailmentRecords;
var _db_1 = require("@db");
var schema_1 = require("@db/schema");
var drizzle_orm_1 = require("drizzle-orm");
var bitcoinService_1 = require("../services/bitcoinService");
var elexon_1 = require("../services/elexon");
var promises_1 = __importDefault(require("fs/promises"));
var path_1 = __importDefault(require("path"));
var url_1 = require("url");
// Constants
var TARGET_DATE = '2025-04-01';
var YEAR_MONTH = '2025-04';
var YEAR = '2025';
var BATCH_SIZE = 12; // Process 12 periods at a time
var MINER_MODELS = ['S19J_PRO', 'S9', 'M20S']; // Common miner models to calculate for
// Function to reingest curtailment records from Elexon API
function reingestCurtailmentRecords() {
    return __awaiter(this, void 0, void 0, function () {
        var startTime, __filename_1, __dirname_1, BMU_MAPPING_PATH, mappingContent, bmuMapping, validWindFarmIds_1, bmuLeadPartyMap_1, deleteResult, insertedRecordIds, totalVolume_1, totalPayment_1, recordsProcessed_1, startPeriod, endPeriod, periodPromises, _loop_1, period, endTime, error_1;
        var _this = this;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    _a.trys.push([0, 8, , 9]);
                    console.log('\n============================================');
                    console.log("STARTING CURTAILMENT REINGESTION (".concat(TARGET_DATE, ")"));
                    console.log('============================================\n');
                    startTime = Date.now();
                    __filename_1 = (0, url_1.fileURLToPath)(import.meta.url);
                    __dirname_1 = path_1.default.dirname(__filename_1);
                    BMU_MAPPING_PATH = path_1.default.join(__dirname_1, "../../data/bmu_mapping.json");
                    return [4 /*yield*/, promises_1.default.access(BMU_MAPPING_PATH).then(function () { return true; }).catch(function () { return false; })];
                case 1:
                    if (!(_a.sent())) {
                        // If bmu_mapping.json doesn't exist, try bmuMapping.json
                        BMU_MAPPING_PATH = path_1.default.join(__dirname_1, "../../data/bmuMapping.json");
                    }
                    // Load wind farm BMU IDs
                    console.log('Loading BMU mapping from:', BMU_MAPPING_PATH);
                    return [4 /*yield*/, promises_1.default.readFile(BMU_MAPPING_PATH, 'utf8')];
                case 2:
                    mappingContent = _a.sent();
                    bmuMapping = JSON.parse(mappingContent);
                    console.log("Loaded ".concat(bmuMapping.length, " BMU mappings"));
                    validWindFarmIds_1 = new Set(bmuMapping
                        .filter(function (bmu) { return bmu.fuelType === "WIND"; })
                        .map(function (bmu) { return bmu.elexonBmUnit; }));
                    bmuLeadPartyMap_1 = new Map(bmuMapping
                        .filter(function (bmu) { return bmu.fuelType === "WIND"; })
                        .map(function (bmu) { return [bmu.elexonBmUnit, bmu.leadPartyName || 'Unknown']; }));
                    console.log("Found ".concat(validWindFarmIds_1.size, " wind farm BMUs"));
                    // Step 1: Clear existing records for the target date
                    console.log("Clearing existing records for ".concat(TARGET_DATE, "..."));
                    return [4 /*yield*/, _db_1.db.delete(schema_1.curtailmentRecords)
                            .where((0, drizzle_orm_1.eq)(schema_1.curtailmentRecords.settlementDate, TARGET_DATE))];
                case 3:
                    deleteResult = _a.sent();
                    console.log("Cleared existing records");
                    insertedRecordIds = [];
                    totalVolume_1 = 0;
                    totalPayment_1 = 0;
                    recordsProcessed_1 = 0;
                    startPeriod = 1;
                    _a.label = 4;
                case 4:
                    if (!(startPeriod <= 48)) return [3 /*break*/, 7];
                    endPeriod = Math.min(startPeriod + BATCH_SIZE - 1, 48);
                    periodPromises = [];
                    console.log("Processing periods ".concat(startPeriod, " to ").concat(endPeriod, "..."));
                    _loop_1 = function (period) {
                        periodPromises.push((function () { return __awaiter(_this, void 0, void 0, function () {
                            var records, validRecords, periodTotal_1, insertResults, error_2;
                            var _this = this;
                            return __generator(this, function (_a) {
                                switch (_a.label) {
                                    case 0:
                                        _a.trys.push([0, 3, , 4]);
                                        return [4 /*yield*/, (0, elexon_1.fetchBidsOffers)(TARGET_DATE, period)];
                                    case 1:
                                        records = _a.sent();
                                        validRecords = records.filter(function (record) {
                                            return record.volume < 0 &&
                                                (record.soFlag || record.cadlFlag) &&
                                                validWindFarmIds_1.has(record.id);
                                        });
                                        periodTotal_1 = { volume: 0, payment: 0 };
                                        if (validRecords.length > 0) {
                                            console.log("[".concat(TARGET_DATE, " P").concat(period, "] Processing ").concat(validRecords.length, " records"));
                                        }
                                        return [4 /*yield*/, Promise.all(validRecords.map(function (record) { return __awaiter(_this, void 0, void 0, function () {
                                                var volume, payment, leadPartyName, result, insertError_1;
                                                return __generator(this, function (_a) {
                                                    switch (_a.label) {
                                                        case 0:
                                                            volume = Math.abs(record.volume);
                                                            payment = volume * record.originalPrice;
                                                            periodTotal_1.volume += volume;
                                                            periodTotal_1.payment += payment;
                                                            _a.label = 1;
                                                        case 1:
                                                            _a.trys.push([1, 3, , 4]);
                                                            leadPartyName = bmuLeadPartyMap_1.get(record.id) || 'Unknown';
                                                            return [4 /*yield*/, _db_1.db.insert(schema_1.curtailmentRecords).values({
                                                                    settlementDate: TARGET_DATE,
                                                                    settlementPeriod: period,
                                                                    farmId: record.id,
                                                                    leadPartyName: leadPartyName,
                                                                    soFlag: record.soFlag,
                                                                    cadlFlag: record.cadlFlag || false,
                                                                    volume: record.volume.toString(),
                                                                    originalPrice: record.originalPrice.toString(),
                                                                    payment: payment.toString(),
                                                                    finalPrice: record.finalPrice ? record.finalPrice.toString() : "0"
                                                                })];
                                                        case 2:
                                                            result = _a.sent();
                                                            // For PostgreSQL, successful insert completed
                                                            recordsProcessed_1++;
                                                            return [2 /*return*/, {
                                                                    success: true,
                                                                    id: record.id,
                                                                    period: period,
                                                                    volume: volume
                                                                }];
                                                        case 3:
                                                            insertError_1 = _a.sent();
                                                            console.error("Error inserting record for farm ".concat(record.id, " in period ").concat(period, ":"), insertError_1);
                                                            return [2 /*return*/, {
                                                                    success: false,
                                                                    id: record.id,
                                                                    period: period,
                                                                    error: insertError_1
                                                                }];
                                                        case 4: return [2 /*return*/];
                                                    }
                                                });
                                            }); }))];
                                    case 2:
                                        insertResults = _a.sent();
                                        if (periodTotal_1.volume > 0) {
                                            console.log("[".concat(TARGET_DATE, " P").concat(period, "] Total: ").concat(periodTotal_1.volume.toFixed(2), " MWh, \u00A3").concat(periodTotal_1.payment.toFixed(2)));
                                        }
                                        totalVolume_1 += periodTotal_1.volume;
                                        totalPayment_1 += periodTotal_1.payment;
                                        return [2 /*return*/, periodTotal_1];
                                    case 3:
                                        error_2 = _a.sent();
                                        console.error("Error processing period ".concat(period, " for date ").concat(TARGET_DATE, ":"), error_2);
                                        return [2 /*return*/, { volume: 0, payment: 0 }];
                                    case 4: return [2 /*return*/];
                                }
                            });
                        }); })());
                    };
                    for (period = startPeriod; period <= endPeriod; period++) {
                        _loop_1(period);
                    }
                    // Wait for all period promises to complete
                    return [4 /*yield*/, Promise.all(periodPromises)];
                case 5:
                    // Wait for all period promises to complete
                    _a.sent();
                    _a.label = 6;
                case 6:
                    startPeriod += BATCH_SIZE;
                    return [3 /*break*/, 4];
                case 7:
                    console.log("\n=== Reingestion Summary for ".concat(TARGET_DATE, " ==="));
                    console.log("Records processed: ".concat(recordsProcessed_1));
                    console.log("Total volume: ".concat(totalVolume_1.toFixed(2), " MWh"));
                    console.log("Total payment: \u00A3".concat(totalPayment_1.toFixed(2)));
                    endTime = Date.now();
                    console.log("Reingestion completed in ".concat(((endTime - startTime) / 1000).toFixed(2), " seconds."));
                    return [3 /*break*/, 9];
                case 8:
                    error_1 = _a.sent();
                    console.error('Error during reingestion:', error_1);
                    throw error_1;
                case 9: return [2 /*return*/];
            }
        });
    });
}
// Function to update the summary tables
function updateSummaryTables() {
    return __awaiter(this, void 0, void 0, function () {
        var startTime, totals, monthlyTotals, yearlyTotals, endTime, error_3;
        var _a, _b, _c, _d, _e, _f, _g, _h, _j, _k, _l, _m;
        return __generator(this, function (_o) {
            switch (_o.label) {
                case 0:
                    _o.trys.push([0, 7, , 8]);
                    console.log('\n============================================');
                    console.log("UPDATING SUMMARY TABLES FOR ".concat(TARGET_DATE));
                    console.log('============================================\n');
                    startTime = Date.now();
                    // Step 1: Update daily summary
                    console.log("\n=== Updating Daily Summary for ".concat(TARGET_DATE, " ==="));
                    return [4 /*yield*/, _db_1.db
                            .select({
                            totalCurtailedEnergy: (0, drizzle_orm_1.sql)(templateObject_1 || (templateObject_1 = __makeTemplateObject(["SUM(ABS(", "::numeric))"], ["SUM(ABS(", "::numeric))"])), schema_1.curtailmentRecords.volume),
                            totalPayment: (0, drizzle_orm_1.sql)(templateObject_2 || (templateObject_2 = __makeTemplateObject(["SUM(", "::numeric)"], ["SUM(", "::numeric)"])), schema_1.curtailmentRecords.payment)
                        })
                            .from(schema_1.curtailmentRecords)
                            .where((0, drizzle_orm_1.eq)(schema_1.curtailmentRecords.settlementDate, TARGET_DATE))];
                case 1:
                    totals = _o.sent();
                    if (!totals[0] || !totals[0].totalCurtailedEnergy) {
                        console.log('No curtailment records found for this date, setting summary to zero values');
                        totals[0] = {
                            totalCurtailedEnergy: '0',
                            totalPayment: '0'
                        };
                    }
                    // Update daily summary
                    return [4 /*yield*/, _db_1.db.insert(schema_1.dailySummaries).values({
                            summaryDate: TARGET_DATE,
                            totalCurtailedEnergy: ((_a = totals[0].totalCurtailedEnergy) === null || _a === void 0 ? void 0 : _a.toString()) || '0',
                            totalPayment: ((_b = totals[0].totalPayment) === null || _b === void 0 ? void 0 : _b.toString()) || '0'
                        }).onConflictDoUpdate({
                            target: [schema_1.dailySummaries.summaryDate],
                            set: {
                                totalCurtailedEnergy: ((_c = totals[0].totalCurtailedEnergy) === null || _c === void 0 ? void 0 : _c.toString()) || '0',
                                totalPayment: ((_d = totals[0].totalPayment) === null || _d === void 0 ? void 0 : _d.toString()) || '0'
                            }
                        })];
                case 2:
                    // Update daily summary
                    _o.sent();
                    console.log('Daily summary updated:', {
                        energy: "".concat(Number(totals[0].totalCurtailedEnergy || 0).toFixed(2), " MWh"),
                        payment: "\u00A3".concat(Number(totals[0].totalPayment || 0).toFixed(2))
                    });
                    // Step 2: Update monthly summary
                    console.log("\n=== Updating Monthly Summary for ".concat(YEAR_MONTH, " ==="));
                    return [4 /*yield*/, _db_1.db
                            .select({
                            totalCurtailedEnergy: (0, drizzle_orm_1.sql)(templateObject_3 || (templateObject_3 = __makeTemplateObject(["SUM(", "::numeric)"], ["SUM(", "::numeric)"])), schema_1.dailySummaries.totalCurtailedEnergy),
                            totalPayment: (0, drizzle_orm_1.sql)(templateObject_4 || (templateObject_4 = __makeTemplateObject(["SUM(", "::numeric)"], ["SUM(", "::numeric)"])), schema_1.dailySummaries.totalPayment)
                        })
                            .from(schema_1.dailySummaries)
                            .where((0, drizzle_orm_1.sql)(templateObject_5 || (templateObject_5 = __makeTemplateObject(["date_trunc('month', ", "::date) = date_trunc('month', ", "::date)"], ["date_trunc('month', ", "::date) = date_trunc('month', ", "::date)"])), schema_1.dailySummaries.summaryDate, TARGET_DATE))];
                case 3:
                    monthlyTotals = _o.sent();
                    if (!monthlyTotals[0] || !monthlyTotals[0].totalCurtailedEnergy) {
                        console.log('No daily summaries found for this month, setting monthly summary to zero values');
                        monthlyTotals[0] = {
                            totalCurtailedEnergy: '0',
                            totalPayment: '0'
                        };
                    }
                    // Update monthly summary
                    return [4 /*yield*/, _db_1.db.insert(schema_1.monthlySummaries).values({
                            yearMonth: YEAR_MONTH,
                            totalCurtailedEnergy: ((_e = monthlyTotals[0].totalCurtailedEnergy) === null || _e === void 0 ? void 0 : _e.toString()) || '0',
                            totalPayment: ((_f = monthlyTotals[0].totalPayment) === null || _f === void 0 ? void 0 : _f.toString()) || '0',
                            updatedAt: new Date()
                        }).onConflictDoUpdate({
                            target: [schema_1.monthlySummaries.yearMonth],
                            set: {
                                totalCurtailedEnergy: ((_g = monthlyTotals[0].totalCurtailedEnergy) === null || _g === void 0 ? void 0 : _g.toString()) || '0',
                                totalPayment: ((_h = monthlyTotals[0].totalPayment) === null || _h === void 0 ? void 0 : _h.toString()) || '0',
                                updatedAt: new Date()
                            }
                        })];
                case 4:
                    // Update monthly summary
                    _o.sent();
                    console.log('Monthly summary updated:', {
                        energy: "".concat(Number(monthlyTotals[0].totalCurtailedEnergy || 0).toFixed(2), " MWh"),
                        payment: "\u00A3".concat(Number(monthlyTotals[0].totalPayment || 0).toFixed(2))
                    });
                    // Step 3: Update yearly summary
                    console.log("\n=== Updating Yearly Summary for ".concat(YEAR, " ==="));
                    return [4 /*yield*/, _db_1.db
                            .select({
                            totalCurtailedEnergy: (0, drizzle_orm_1.sql)(templateObject_6 || (templateObject_6 = __makeTemplateObject(["SUM(", "::numeric)"], ["SUM(", "::numeric)"])), schema_1.dailySummaries.totalCurtailedEnergy),
                            totalPayment: (0, drizzle_orm_1.sql)(templateObject_7 || (templateObject_7 = __makeTemplateObject(["SUM(", "::numeric)"], ["SUM(", "::numeric)"])), schema_1.dailySummaries.totalPayment)
                        })
                            .from(schema_1.dailySummaries)
                            .where((0, drizzle_orm_1.sql)(templateObject_8 || (templateObject_8 = __makeTemplateObject(["date_trunc('year', ", "::date) = date_trunc('year', ", "::date)"], ["date_trunc('year', ", "::date) = date_trunc('year', ", "::date)"])), schema_1.dailySummaries.summaryDate, TARGET_DATE))];
                case 5:
                    yearlyTotals = _o.sent();
                    if (!yearlyTotals[0] || !yearlyTotals[0].totalCurtailedEnergy) {
                        console.log('No daily summaries found for this year, setting yearly summary to zero values');
                        yearlyTotals[0] = {
                            totalCurtailedEnergy: '0',
                            totalPayment: '0'
                        };
                    }
                    // Update yearly summary
                    return [4 /*yield*/, _db_1.db.insert(schema_1.yearlySummaries).values({
                            year: YEAR,
                            totalCurtailedEnergy: ((_j = yearlyTotals[0].totalCurtailedEnergy) === null || _j === void 0 ? void 0 : _j.toString()) || '0',
                            totalPayment: ((_k = yearlyTotals[0].totalPayment) === null || _k === void 0 ? void 0 : _k.toString()) || '0',
                            updatedAt: new Date()
                        }).onConflictDoUpdate({
                            target: [schema_1.yearlySummaries.year],
                            set: {
                                totalCurtailedEnergy: ((_l = yearlyTotals[0].totalCurtailedEnergy) === null || _l === void 0 ? void 0 : _l.toString()) || '0',
                                totalPayment: ((_m = yearlyTotals[0].totalPayment) === null || _m === void 0 ? void 0 : _m.toString()) || '0',
                                updatedAt: new Date()
                            }
                        })];
                case 6:
                    // Update yearly summary
                    _o.sent();
                    console.log('Yearly summary updated:', {
                        energy: "".concat(Number(yearlyTotals[0].totalCurtailedEnergy || 0).toFixed(2), " MWh"),
                        payment: "\u00A3".concat(Number(yearlyTotals[0].totalPayment || 0).toFixed(2))
                    });
                    endTime = Date.now();
                    console.log("\nSummary table updates completed in ".concat(((endTime - startTime) / 1000).toFixed(2), " seconds."));
                    return [3 /*break*/, 8];
                case 7:
                    error_3 = _o.sent();
                    console.error('Error updating summary tables:', error_3);
                    throw error_3;
                case 8: return [2 /*return*/];
            }
        });
    });
}
// Function to update Bitcoin calculation tables
function updateBitcoinCalculations() {
    return __awaiter(this, void 0, void 0, function () {
        var startTime, _i, MINER_MODELS_1, minerModel, endTime, error_4;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    _a.trys.push([0, 6, , 7]);
                    console.log('\n============================================');
                    console.log("UPDATING BITCOIN CALCULATIONS FOR ".concat(TARGET_DATE));
                    console.log('============================================\n');
                    startTime = Date.now();
                    // Delete existing Bitcoin calculations for this date
                    console.log("Deleting existing Bitcoin calculations for ".concat(TARGET_DATE, "..."));
                    return [4 /*yield*/, _db_1.db.delete(schema_1.historicalBitcoinCalculations)
                            .where((0, drizzle_orm_1.eq)(schema_1.historicalBitcoinCalculations.settlementDate, TARGET_DATE))];
                case 1:
                    _a.sent();
                    _i = 0, MINER_MODELS_1 = MINER_MODELS;
                    _a.label = 2;
                case 2:
                    if (!(_i < MINER_MODELS_1.length)) return [3 /*break*/, 5];
                    minerModel = MINER_MODELS_1[_i];
                    console.log("\n=== Processing Bitcoin calculations for ".concat(minerModel, " ==="));
                    return [4 /*yield*/, (0, bitcoinService_1.processSingleDay)(TARGET_DATE, minerModel)];
                case 3:
                    _a.sent();
                    _a.label = 4;
                case 4:
                    _i++;
                    return [3 /*break*/, 2];
                case 5:
                    endTime = Date.now();
                    console.log("\nBitcoin calculations completed in ".concat(((endTime - startTime) / 1000).toFixed(2), " seconds."));
                    return [3 /*break*/, 7];
                case 6:
                    error_4 = _a.sent();
                    console.error('Error updating Bitcoin calculations:', error_4);
                    throw error_4;
                case 7: return [2 /*return*/];
            }
        });
    });
}
// Main function to run the entire process
function runFullUpdate() {
    return __awaiter(this, void 0, void 0, function () {
        var startTime, endTime, error_5;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    _a.trys.push([0, 4, , 5]);
                    console.log('\n============================================');
                    console.log("STARTING FULL UPDATE FOR ".concat(TARGET_DATE));
                    console.log('============================================\n');
                    startTime = Date.now();
                    // Step 1: Reingest curtailment records
                    return [4 /*yield*/, reingestCurtailmentRecords()];
                case 1:
                    // Step 1: Reingest curtailment records
                    _a.sent();
                    // Step 2: Update summary tables
                    return [4 /*yield*/, updateSummaryTables()];
                case 2:
                    // Step 2: Update summary tables
                    _a.sent();
                    // Step 3: Update Bitcoin calculations
                    return [4 /*yield*/, updateBitcoinCalculations()];
                case 3:
                    // Step 3: Update Bitcoin calculations
                    _a.sent();
                    endTime = Date.now();
                    console.log('\n============================================');
                    console.log('FULL UPDATE COMPLETED SUCCESSFULLY');
                    console.log("Total duration: ".concat(((endTime - startTime) / 1000).toFixed(2), " seconds"));
                    console.log('============================================\n');
                    return [3 /*break*/, 5];
                case 4:
                    error_5 = _a.sent();
                    console.error('\n============================================');
                    console.error('FULL UPDATE FAILED');
                    console.error('Error:', error_5);
                    console.error('============================================\n');
                    process.exit(1);
                    return [3 /*break*/, 5];
                case 5: return [2 /*return*/];
            }
        });
    });
}
// Run the script if called directly
if (require.main === module) {
    runFullUpdate();
}
// Export functions for use in other scripts
exports.default = {
    reingestCurtailmentRecords: reingestCurtailmentRecords,
    updateSummaryTables: updateSummaryTables,
    updateBitcoinCalculations: updateBitcoinCalculations,
    runFullUpdate: runFullUpdate
};
var templateObject_1, templateObject_2, templateObject_3, templateObject_4, templateObject_5, templateObject_6, templateObject_7, templateObject_8;
