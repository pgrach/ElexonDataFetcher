"use strict";
/**
 * Bitcoin calculation utilities for Bitcoin Mining Analytics platform
 *
 * This module centralizes Bitcoin mining calculations to ensure consistent
 * results across different parts of the application.
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.calculateBitcoin = calculateBitcoin;
exports.calculateMiningEfficiency = calculateMiningEfficiency;
exports.energyToHashrate = energyToHashrate;
exports.calculateMiningRevenue = calculateMiningRevenue;
exports.calculateExpectedRevenue = calculateExpectedRevenue;
var errors_1 = require("./errors");
var logger_1 = require("./logger");
var bitcoin_1 = require("../types/bitcoin");
// Constants for Bitcoin calculation
var SECONDS_PER_DAY = 86400;
var JOULES_IN_KWH = 3600000;
var TERAHASHES_TO_HASHES = 1e12;
var SATOSHIS_PER_BITCOIN = 1e8;
var SUPPLY_PER_BLOCK = 6.25; // Bitcoin reward per block
/**
 * Calculate the amount of Bitcoin that could be mined with the given parameters
 *
 * @param mwh - Energy in MWh
 * @param minerModel - The miner model name (e.g., 'S19J_PRO')
 * @param difficulty - Network difficulty (default to latest value if not provided)
 * @returns The amount of Bitcoin that could be mined
 */
function calculateBitcoin(mwh, minerModel, difficulty) {
    if (difficulty === void 0) { difficulty = bitcoin_1.DEFAULT_DIFFICULTY; }
    try {
        // Validate inputs
        if (mwh < 0) {
            throw new errors_1.CalculationError("Invalid energy value: ".concat(mwh, ". Must be a positive number."));
        }
        if (!bitcoin_1.minerModels[minerModel]) {
            throw new errors_1.CalculationError("Invalid miner model: ".concat(minerModel, ". Valid models are: ").concat(Object.keys(bitcoin_1.minerModels).join(', ')));
        }
        if (difficulty <= 0) {
            throw new errors_1.CalculationError("Invalid difficulty: ".concat(difficulty, ". Must be a positive number."));
        }
        // Get miner stats
        var minerStats = bitcoin_1.minerModels[minerModel];
        // Convert MWh to kWh
        var kWh = mwh * 1000;
        // Calculate maximum hashes achievable with this energy
        var totalHashes = calculateTotalHashes(kWh, minerStats);
        // Calculate expected bitcoins
        var bitcoinMined = calculateExpectedBitcoin(totalHashes, difficulty);
        return bitcoinMined;
    }
    catch (error) {
        if (error instanceof errors_1.CalculationError) {
            throw error;
        }
        // Log and wrap unexpected errors
        logger_1.logger.error('Bitcoin calculation error', {
            module: 'bitcoin',
            context: { mwh: mwh, minerModel: minerModel, difficulty: difficulty },
            error: error
        });
        throw new errors_1.CalculationError("Bitcoin calculation failed: ".concat(error.message), {
            context: { mwh: mwh, minerModel: minerModel, difficulty: difficulty },
            originalError: error
        });
    }
}
/**
 * Calculate the total number of hashes that could be computed with the given energy
 */
function calculateTotalHashes(kWh, minerStats) {
    // Energy in joules
    var joules = kWh * JOULES_IN_KWH;
    // Time in seconds the miner could run with this energy
    var secondsOfMining = joules / minerStats.power;
    // Total hashes = hashrate (H/s) * time (s)
    return minerStats.hashrate * TERAHASHES_TO_HASHES * secondsOfMining;
}
/**
 * Calculate the expected Bitcoin rewards based on hashing power
 */
function calculateExpectedBitcoin(totalHashes, difficulty) {
    // Expected number of hashes per block
    var hashesPerBlock = difficulty * Math.pow(2, 32);
    // Expected number of blocks
    var expectedBlocks = totalHashes / hashesPerBlock;
    // Expected Bitcoin (blocks * reward per block)
    return expectedBlocks * SUPPLY_PER_BLOCK;
}
/**
 * Calculate the mining efficiency in Bitcoin per MWh for a given miner model
 */
function calculateMiningEfficiency(minerModel, difficulty) {
    if (difficulty === void 0) { difficulty = bitcoin_1.DEFAULT_DIFFICULTY; }
    // Just calculate for 1 MWh to get BTC/MWh rate
    return calculateBitcoin(1, minerModel, difficulty);
}
/**
 * Convert MWh to estimated hashrate in TH/s
 */
function energyToHashrate(mwh, minerModel) {
    if (!bitcoin_1.minerModels[minerModel]) {
        throw new errors_1.CalculationError("Invalid miner model: ".concat(minerModel));
    }
    // Convert MWh to W
    var watts = mwh * 1000 * 1000;
    // Calculate how many miners could run
    var minerCount = watts / bitcoin_1.minerModels[minerModel].power;
    // Calculate hashrate
    return minerCount * bitcoin_1.minerModels[minerModel].hashrate;
}
/**
 * Calculate mining revenue in fiat currency
 */
function calculateMiningRevenue(bitcoin, bitcoinPrice) {
    return bitcoin * bitcoinPrice;
}
/**
 * Calculate expected mining revenue for a period
 */
function calculateExpectedRevenue(mwh, minerModel, bitcoinPrice, difficulty) {
    if (difficulty === void 0) { difficulty = bitcoin_1.DEFAULT_DIFFICULTY; }
    var bitcoin = calculateBitcoin(mwh, minerModel, difficulty);
    var fiatValue = calculateMiningRevenue(bitcoin, bitcoinPrice);
    return { bitcoin: bitcoin, fiatValue: fiatValue };
}
