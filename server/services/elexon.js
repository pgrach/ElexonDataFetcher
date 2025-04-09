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
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.fetchBidsOffers = fetchBidsOffers;
exports.delay = delay;
var axios_1 = __importDefault(require("axios"));
var promises_1 = __importDefault(require("fs/promises"));
var path_1 = __importDefault(require("path"));
var url_1 = require("url");
var __filename = (0, url_1.fileURLToPath)(import.meta.url);
var __dirname = path_1.default.dirname(__filename);
var ELEXON_BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1";
var BMU_MAPPING_PATH = path_1.default.join(__dirname, "../data/bmuMapping.json");
var MAX_REQUESTS_PER_MINUTE = 4500; // Keep buffer below 5000 limit
var REQUEST_WINDOW_MS = 60000; // 1 minute in milliseconds
var PARALLEL_REQUESTS = 10; // Allow 10 parallel requests
var windFarmIds = null;
var requestTimestamps = [];
function loadWindFarmIds() {
    return __awaiter(this, void 0, void 0, function () {
        var mappingContent, bmuMapping, error_1;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    if (windFarmIds !== null) {
                        return [2 /*return*/, windFarmIds];
                    }
                    _a.label = 1;
                case 1:
                    _a.trys.push([1, 3, , 4]);
                    console.log('Loading BMU mapping from:', BMU_MAPPING_PATH);
                    return [4 /*yield*/, promises_1.default.readFile(BMU_MAPPING_PATH, 'utf8')];
                case 2:
                    mappingContent = _a.sent();
                    bmuMapping = JSON.parse(mappingContent);
                    windFarmIds = new Set(bmuMapping.map(function (bmu) { return bmu.elexonBmUnit; }));
                    console.log("Loaded ".concat(windFarmIds.size, " wind farm BMU IDs"));
                    return [2 /*return*/, windFarmIds];
                case 3:
                    error_1 = _a.sent();
                    console.error('Error loading BMU mapping:', error_1);
                    throw error_1;
                case 4: return [2 /*return*/];
            }
        });
    });
}
function trackRequest() {
    var now = Date.now();
    requestTimestamps = __spreadArray(__spreadArray([], requestTimestamps, true), [now], false).filter(function (timestamp) {
        return now - timestamp < REQUEST_WINDOW_MS;
    });
}
function waitForRateLimit() {
    return __awaiter(this, void 0, void 0, function () {
        var now, oldestRequest, waitTime;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    now = Date.now();
                    requestTimestamps = requestTimestamps.filter(function (timestamp) {
                        return now - timestamp < REQUEST_WINDOW_MS;
                    });
                    if (!(requestTimestamps.length >= MAX_REQUESTS_PER_MINUTE)) return [3 /*break*/, 2];
                    oldestRequest = requestTimestamps[0];
                    waitTime = REQUEST_WINDOW_MS - (now - oldestRequest);
                    console.log("Rate limit reached, waiting ".concat(Math.ceil(waitTime / 1000), "s..."));
                    return [4 /*yield*/, delay(waitTime + 100)];
                case 1:
                    _a.sent(); // Add 100ms buffer
                    return [2 /*return*/, waitForRateLimit()]; // Recheck after waiting
                case 2: return [2 /*return*/];
            }
        });
    });
}
function makeRequest(url, date, period) {
    return __awaiter(this, void 0, void 0, function () {
        var response, error_2;
        var _a;
        return __generator(this, function (_b) {
            switch (_b.label) {
                case 0: return [4 /*yield*/, waitForRateLimit()];
                case 1:
                    _b.sent();
                    _b.label = 2;
                case 2:
                    _b.trys.push([2, 4, , 7]);
                    return [4 /*yield*/, axios_1.default.get(url, {
                            headers: { 'Accept': 'application/json' },
                            timeout: 30000 // 30 second timeout
                        })];
                case 3:
                    response = _b.sent();
                    trackRequest();
                    return [2 /*return*/, response];
                case 4:
                    error_2 = _b.sent();
                    if (!(axios_1.default.isAxiosError(error_2) && ((_a = error_2.response) === null || _a === void 0 ? void 0 : _a.status) === 429)) return [3 /*break*/, 6];
                    console.log("[".concat(date, " P").concat(period, "] Rate limited, retrying after delay..."));
                    return [4 /*yield*/, delay(60000)];
                case 5:
                    _b.sent(); // Wait 1 minute on rate limit
                    return [2 /*return*/, makeRequest(url, date, period)];
                case 6: throw error_2;
                case 7: return [2 /*return*/];
            }
        });
    });
}
function fetchBidsOffers(date, period) {
    return __awaiter(this, void 0, void 0, function () {
        var validWindFarmIds_1, _a, bidsResponse, offersResponse, validBids, validOffers, allRecords, periodTotal, periodPayment, error_3;
        var _b, _c, _d, _e, _f;
        return __generator(this, function (_g) {
            switch (_g.label) {
                case 0:
                    _g.trys.push([0, 3, , 4]);
                    return [4 /*yield*/, loadWindFarmIds()];
                case 1:
                    validWindFarmIds_1 = _g.sent();
                    return [4 /*yield*/, Promise.all([
                            makeRequest("".concat(ELEXON_BASE_URL, "/balancing/settlement/stack/all/bid/").concat(date, "/").concat(period), date, period),
                            makeRequest("".concat(ELEXON_BASE_URL, "/balancing/settlement/stack/all/offer/").concat(date, "/").concat(period), date, period)
                        ]).catch(function (error) {
                            console.error("[".concat(date, " P").concat(period, "] Error fetching data:"), error.message);
                            return [{ data: { data: [] } }, { data: { data: [] } }];
                        })];
                case 2:
                    _a = _g.sent(), bidsResponse = _a[0], offersResponse = _a[1];
                    if (!((_b = bidsResponse.data) === null || _b === void 0 ? void 0 : _b.data) || !((_c = offersResponse.data) === null || _c === void 0 ? void 0 : _c.data)) {
                        console.error("[".concat(date, " P").concat(period, "] Invalid API response format"));
                        return [2 /*return*/, []];
                    }
                    validBids = bidsResponse.data.data.filter(function (record) {
                        return record.volume < 0 && record.soFlag && validWindFarmIds_1.has(record.id);
                    });
                    validOffers = offersResponse.data.data.filter(function (record) {
                        return record.volume < 0 && record.soFlag && validWindFarmIds_1.has(record.id);
                    });
                    allRecords = __spreadArray(__spreadArray([], validBids, true), validOffers, true);
                    if (allRecords.length > 0) {
                        periodTotal = allRecords.reduce(function (sum, r) { return sum + Math.abs(r.volume); }, 0);
                        periodPayment = allRecords.reduce(function (sum, r) { return sum + (Math.abs(r.volume) * r.originalPrice * -1); }, 0);
                        console.log("[".concat(date, " P").concat(period, "] Records: ").concat(allRecords.length, " (").concat(periodTotal.toFixed(2), " MWh, \u00A3").concat(periodPayment.toFixed(2), ")"));
                    }
                    return [2 /*return*/, allRecords];
                case 3:
                    error_3 = _g.sent();
                    if (axios_1.default.isAxiosError(error_3)) {
                        console.error("[".concat(date, " P").concat(period, "] Elexon API error:"), ((_d = error_3.response) === null || _d === void 0 ? void 0 : _d.data) || error_3.message);
                        throw new Error("Elexon API error: ".concat(((_f = (_e = error_3.response) === null || _e === void 0 ? void 0 : _e.data) === null || _f === void 0 ? void 0 : _f.error) || error_3.message));
                    }
                    console.error("[".concat(date, " P").concat(period, "] Unexpected error:"), error_3);
                    throw error_3;
                case 4: return [2 /*return*/];
            }
        });
    });
}
function delay(ms) {
    return __awaiter(this, void 0, void 0, function () {
        return __generator(this, function (_a) {
            return [2 /*return*/, new Promise(function (resolve) { return setTimeout(resolve, ms); })];
        });
    });
}
