"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.DEFAULT_DIFFICULTY = exports.minerModels = exports.BitcoinCalculationSchema = void 0;
var zod_1 = require("zod");
exports.BitcoinCalculationSchema = zod_1.z.object({
    bitcoinMined: zod_1.z.number(),
    difficulty: zod_1.z.number()
});
exports.minerModels = {
    S19J_PRO: {
        hashrate: 100,
        power: 3050
    },
    S9: {
        hashrate: 13.5,
        power: 1323
    },
    M20S: {
        hashrate: 68,
        power: 3360
    }
};
// Default values for fallback
exports.DEFAULT_DIFFICULTY = 108105433845147; // Current network difficulty as fallback
