"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.selectWindGenerationDataSchema = exports.insertWindGenerationDataSchema = exports.windGenerationData = exports.selectYearlyMiningPotentialSchema = exports.insertYearlyMiningPotentialSchema = exports.selectDailyMiningPotentialSchema = exports.insertDailyMiningPotentialSchema = exports.selectSettlementPeriodMiningSchema = exports.insertSettlementPeriodMiningSchema = exports.selectBitcoinYearlySummarySchema = exports.insertBitcoinYearlySummarySchema = exports.selectBitcoinMonthlySummarySchema = exports.insertBitcoinMonthlySummarySchema = exports.selectBitcoinDailySummarySchema = exports.insertBitcoinDailySummarySchema = exports.selectHistoricalBitcoinCalculationSchema = exports.insertHistoricalBitcoinCalculationSchema = exports.selectIngestionProgressSchema = exports.insertIngestionProgressSchema = exports.selectYearlySummarySchema = exports.insertYearlySummarySchema = exports.selectMonthlySummarySchema = exports.insertMonthlySummarySchema = exports.selectDailySummarySchema = exports.insertDailySummarySchema = exports.selectCurtailmentRecordSchema = exports.insertCurtailmentRecordSchema = exports.yearlyMiningPotential = exports.dailyMiningPotential = exports.settlementPeriodMining = exports.ingestionProgress = exports.yearlySummaries = exports.monthlySummaries = exports.dailySummaries = exports.bitcoinYearlySummaries = exports.bitcoinMonthlySummaries = exports.bitcoinDailySummaries = exports.historicalBitcoinCalculations = exports.curtailmentRecords = void 0;
var pg_core_1 = require("drizzle-orm/pg-core");
var drizzle_zod_1 = require("drizzle-zod");
exports.curtailmentRecords = (0, pg_core_1.pgTable)("curtailment_records", {
    id: (0, pg_core_1.serial)("id").primaryKey(),
    settlementDate: (0, pg_core_1.date)("settlement_date").notNull(),
    settlementPeriod: (0, pg_core_1.integer)("settlement_period").notNull(),
    farmId: (0, pg_core_1.text)("farm_id").notNull(),
    leadPartyName: (0, pg_core_1.text)("lead_party_name"),
    volume: (0, pg_core_1.numeric)("volume").notNull(),
    payment: (0, pg_core_1.numeric)("payment").notNull(),
    originalPrice: (0, pg_core_1.numeric)("original_price").notNull(),
    finalPrice: (0, pg_core_1.numeric)("final_price").notNull(),
    soFlag: (0, pg_core_1.boolean)("so_flag"),
    cadlFlag: (0, pg_core_1.boolean)("cadl_flag"),
    createdAt: (0, pg_core_1.timestamp)("created_at").defaultNow(),
});
exports.historicalBitcoinCalculations = (0, pg_core_1.pgTable)("historical_bitcoin_calculations", {
    id: (0, pg_core_1.serial)("id").primaryKey(),
    settlementDate: (0, pg_core_1.date)("settlement_date").notNull(),
    settlementPeriod: (0, pg_core_1.integer)("settlement_period").notNull(),
    farmId: (0, pg_core_1.text)("farm_id").notNull(),
    minerModel: (0, pg_core_1.text)("miner_model").notNull(),
    bitcoinMined: (0, pg_core_1.numeric)("bitcoin_mined").notNull(),
    difficulty: (0, pg_core_1.numeric)("difficulty").notNull(),
    calculatedAt: (0, pg_core_1.timestamp)("calculated_at").defaultNow(),
});
// Define the bitcoin_daily_summaries table 
exports.bitcoinDailySummaries = (0, pg_core_1.pgTable)("bitcoin_daily_summaries", {
    id: (0, pg_core_1.serial)("id").primaryKey(),
    summaryDate: (0, pg_core_1.date)("summary_date").notNull(),
    minerModel: (0, pg_core_1.text)("miner_model").notNull(),
    bitcoinMined: (0, pg_core_1.numeric)("bitcoin_mined").notNull(),
    // averageDifficulty column removed to follow DRY principle
    // as this data is already available in historical_bitcoin_calculations
    createdAt: (0, pg_core_1.timestamp)("created_at").defaultNow(),
    updatedAt: (0, pg_core_1.timestamp)("updated_at").defaultNow(),
});
exports.bitcoinMonthlySummaries = (0, pg_core_1.pgTable)("bitcoin_monthly_summaries", {
    id: (0, pg_core_1.serial)("id").primaryKey(),
    yearMonth: (0, pg_core_1.text)("year_month").notNull(),
    minerModel: (0, pg_core_1.text)("miner_model").notNull(),
    bitcoinMined: (0, pg_core_1.numeric)("bitcoin_mined").notNull(),
    createdAt: (0, pg_core_1.timestamp)("created_at").defaultNow(),
    updatedAt: (0, pg_core_1.timestamp)("updated_at").defaultNow(),
});
exports.bitcoinYearlySummaries = (0, pg_core_1.pgTable)("bitcoin_yearly_summaries", {
    id: (0, pg_core_1.serial)("id").primaryKey(),
    year: (0, pg_core_1.text)("year").notNull(),
    minerModel: (0, pg_core_1.text)("miner_model").notNull(),
    bitcoinMined: (0, pg_core_1.numeric)("bitcoin_mined").notNull(),
    createdAt: (0, pg_core_1.timestamp)("created_at").defaultNow(),
    updatedAt: (0, pg_core_1.timestamp)("updated_at").defaultNow(),
});
exports.dailySummaries = (0, pg_core_1.pgTable)("daily_summaries", {
    summaryDate: (0, pg_core_1.date)("summary_date").primaryKey(),
    totalCurtailedEnergy: (0, pg_core_1.numeric)("total_curtailed_energy"),
    totalPayment: (0, pg_core_1.numeric)("total_payment"),
    totalWindGeneration: (0, pg_core_1.numeric)("total_wind_generation").default('0'),
    windOnshoreGeneration: (0, pg_core_1.numeric)("wind_onshore_generation").default('0'),
    windOffshoreGeneration: (0, pg_core_1.numeric)("wind_offshore_generation").default('0'),
    createdAt: (0, pg_core_1.timestamp)("created_at").defaultNow(),
    lastUpdated: (0, pg_core_1.timestamp)("last_updated").defaultNow()
});
exports.monthlySummaries = (0, pg_core_1.pgTable)("monthly_summaries", {
    yearMonth: (0, pg_core_1.text)("year_month").primaryKey(),
    totalCurtailedEnergy: (0, pg_core_1.numeric)("total_curtailed_energy"),
    totalPayment: (0, pg_core_1.numeric)("total_payment"),
    totalWindGeneration: (0, pg_core_1.numeric)("total_wind_generation").default('0'),
    windOnshoreGeneration: (0, pg_core_1.numeric)("wind_onshore_generation").default('0'),
    windOffshoreGeneration: (0, pg_core_1.numeric)("wind_offshore_generation").default('0'),
    createdAt: (0, pg_core_1.timestamp)("created_at").defaultNow(),
    updatedAt: (0, pg_core_1.timestamp)("updated_at").defaultNow(),
    lastUpdated: (0, pg_core_1.timestamp)("last_updated").defaultNow()
});
exports.yearlySummaries = (0, pg_core_1.pgTable)("yearly_summaries", {
    year: (0, pg_core_1.text)("year").primaryKey(),
    totalCurtailedEnergy: (0, pg_core_1.numeric)("total_curtailed_energy"),
    totalPayment: (0, pg_core_1.numeric)("total_payment"),
    totalWindGeneration: (0, pg_core_1.numeric)("total_wind_generation").default('0'),
    windOnshoreGeneration: (0, pg_core_1.numeric)("wind_onshore_generation").default('0'),
    windOffshoreGeneration: (0, pg_core_1.numeric)("wind_offshore_generation").default('0'),
    createdAt: (0, pg_core_1.timestamp)("created_at").defaultNow(),
    updatedAt: (0, pg_core_1.timestamp)("updated_at").defaultNow(),
    lastUpdated: (0, pg_core_1.timestamp)("last_updated").defaultNow()
});
exports.ingestionProgress = (0, pg_core_1.pgTable)("ingestion_progress", {
    id: (0, pg_core_1.serial)("id").primaryKey(),
    lastProcessedDate: (0, pg_core_1.date)("last_processed_date").notNull(),
    status: (0, pg_core_1.text)("status").notNull(),
    errorMessage: (0, pg_core_1.text)("error_message"),
    createdAt: (0, pg_core_1.timestamp)("created_at").defaultNow(),
    updatedAt: (0, pg_core_1.timestamp)("updated_at").defaultNow()
});
// These materialized views have been replaced with direct query optimizations
// Keeping declarations for backward compatibility with existing code
// but these tables are no longer used in the application
// @deprecated - Use optimizedMiningService instead
exports.settlementPeriodMining = (0, pg_core_1.pgTable)("settlement_period_mining", {
    id: (0, pg_core_1.serial)("id").primaryKey(),
    settlementDate: (0, pg_core_1.date)("settlement_date").notNull(),
    settlementPeriod: (0, pg_core_1.integer)("settlement_period").notNull(),
    farmId: (0, pg_core_1.text)("farm_id").notNull(),
    minerModel: (0, pg_core_1.text)("miner_model").notNull(),
    curtailedEnergy: (0, pg_core_1.numeric)("curtailed_energy").notNull(),
    bitcoinMined: (0, pg_core_1.numeric)("bitcoin_mined").notNull(),
    valueAtPrice: (0, pg_core_1.numeric)("value_at_price"),
    price: (0, pg_core_1.numeric)("price"),
    difficulty: (0, pg_core_1.numeric)("difficulty").notNull(),
    createdAt: (0, pg_core_1.timestamp)("created_at").defaultNow(),
});
// @deprecated - Use optimizedMiningService instead
exports.dailyMiningPotential = (0, pg_core_1.pgTable)("daily_mining_potential", {
    id: (0, pg_core_1.serial)("id").primaryKey(),
    summaryDate: (0, pg_core_1.date)("summary_date").notNull(),
    farmId: (0, pg_core_1.text)("farm_id").notNull(),
    minerModel: (0, pg_core_1.text)("miner_model").notNull(),
    totalCurtailedEnergy: (0, pg_core_1.numeric)("total_curtailed_energy").notNull(),
    totalBitcoinMined: (0, pg_core_1.numeric)("total_bitcoin_mined").notNull(),
    averageValue: (0, pg_core_1.numeric)("average_value"),
    createdAt: (0, pg_core_1.timestamp)("created_at").defaultNow(),
    updatedAt: (0, pg_core_1.timestamp)("updated_at").defaultNow(),
});
// @deprecated - Use optimizedMiningService instead
exports.yearlyMiningPotential = (0, pg_core_1.pgTable)("yearly_mining_potential", {
    id: (0, pg_core_1.serial)("id").primaryKey(),
    year: (0, pg_core_1.text)("year").notNull(),
    farmId: (0, pg_core_1.text)("farm_id").notNull(),
    minerModel: (0, pg_core_1.text)("miner_model").notNull(),
    totalCurtailedEnergy: (0, pg_core_1.numeric)("total_curtailed_energy").notNull(),
    totalBitcoinMined: (0, pg_core_1.numeric)("total_bitcoin_mined").notNull(),
    averageValue: (0, pg_core_1.numeric)("average_value"),
    createdAt: (0, pg_core_1.timestamp)("created_at").defaultNow(),
    updatedAt: (0, pg_core_1.timestamp)("updated_at").defaultNow(),
});
exports.insertCurtailmentRecordSchema = (0, drizzle_zod_1.createInsertSchema)(exports.curtailmentRecords);
exports.selectCurtailmentRecordSchema = (0, drizzle_zod_1.createSelectSchema)(exports.curtailmentRecords);
exports.insertDailySummarySchema = (0, drizzle_zod_1.createInsertSchema)(exports.dailySummaries);
exports.selectDailySummarySchema = (0, drizzle_zod_1.createSelectSchema)(exports.dailySummaries);
exports.insertMonthlySummarySchema = (0, drizzle_zod_1.createInsertSchema)(exports.monthlySummaries);
exports.selectMonthlySummarySchema = (0, drizzle_zod_1.createSelectSchema)(exports.monthlySummaries);
exports.insertYearlySummarySchema = (0, drizzle_zod_1.createInsertSchema)(exports.yearlySummaries);
exports.selectYearlySummarySchema = (0, drizzle_zod_1.createSelectSchema)(exports.yearlySummaries);
exports.insertIngestionProgressSchema = (0, drizzle_zod_1.createInsertSchema)(exports.ingestionProgress);
exports.selectIngestionProgressSchema = (0, drizzle_zod_1.createSelectSchema)(exports.ingestionProgress);
exports.insertHistoricalBitcoinCalculationSchema = (0, drizzle_zod_1.createInsertSchema)(exports.historicalBitcoinCalculations);
exports.selectHistoricalBitcoinCalculationSchema = (0, drizzle_zod_1.createSelectSchema)(exports.historicalBitcoinCalculations);
exports.insertBitcoinDailySummarySchema = (0, drizzle_zod_1.createInsertSchema)(exports.bitcoinDailySummaries);
exports.selectBitcoinDailySummarySchema = (0, drizzle_zod_1.createSelectSchema)(exports.bitcoinDailySummaries);
exports.insertBitcoinMonthlySummarySchema = (0, drizzle_zod_1.createInsertSchema)(exports.bitcoinMonthlySummaries);
exports.selectBitcoinMonthlySummarySchema = (0, drizzle_zod_1.createSelectSchema)(exports.bitcoinMonthlySummaries);
exports.insertBitcoinYearlySummarySchema = (0, drizzle_zod_1.createInsertSchema)(exports.bitcoinYearlySummaries);
exports.selectBitcoinYearlySummarySchema = (0, drizzle_zod_1.createSelectSchema)(exports.bitcoinYearlySummaries);
// @deprecated - Schemas for materialized view tables
// Keeping these for backward compatibility
exports.insertSettlementPeriodMiningSchema = (0, drizzle_zod_1.createInsertSchema)(exports.settlementPeriodMining);
exports.selectSettlementPeriodMiningSchema = (0, drizzle_zod_1.createSelectSchema)(exports.settlementPeriodMining);
exports.insertDailyMiningPotentialSchema = (0, drizzle_zod_1.createInsertSchema)(exports.dailyMiningPotential);
exports.selectDailyMiningPotentialSchema = (0, drizzle_zod_1.createSelectSchema)(exports.dailyMiningPotential);
exports.insertYearlyMiningPotentialSchema = (0, drizzle_zod_1.createInsertSchema)(exports.yearlyMiningPotential);
exports.selectYearlyMiningPotentialSchema = (0, drizzle_zod_1.createSelectSchema)(exports.yearlyMiningPotential);
// Wind generation aggregation table
exports.windGenerationData = (0, pg_core_1.pgTable)("wind_generation_data", {
    id: (0, pg_core_1.serial)("id").primaryKey(),
    settlementDate: (0, pg_core_1.date)("settlement_date").notNull(),
    settlementPeriod: (0, pg_core_1.integer)("settlement_period").notNull(),
    windOnshore: (0, pg_core_1.numeric)("wind_onshore").notNull(),
    windOffshore: (0, pg_core_1.numeric)("wind_offshore").notNull(),
    totalWind: (0, pg_core_1.numeric)("total_wind").notNull(),
    lastUpdated: (0, pg_core_1.timestamp)("last_updated").defaultNow().notNull(),
    dataSource: (0, pg_core_1.text)("data_source").default("ELEXON").notNull(),
});
exports.insertWindGenerationDataSchema = (0, drizzle_zod_1.createInsertSchema)(exports.windGenerationData);
exports.selectWindGenerationDataSchema = (0, drizzle_zod_1.createSelectSchema)(exports.windGenerationData);
