import { pgTable, text, serial, date, integer, numeric, boolean, timestamp } from "drizzle-orm/pg-core";
import { createInsertSchema, createSelectSchema } from "drizzle-zod";

export const curtailmentRecords = pgTable("curtailment_records", {
  id: serial("id").primaryKey(),
  settlementDate: date("settlement_date").notNull(),
  settlementPeriod: integer("settlement_period").notNull(),
  bmuId: text("bmu_id").notNull(),
  leadPartyName: text("lead_party_name"),  
  volume: numeric("volume").notNull(),
  payment: numeric("payment").notNull(),
  originalPrice: numeric("original_price").notNull(),
  finalPrice: numeric("final_price").notNull(),
  soFlag: boolean("so_flag"),
  cadlFlag: boolean("cadl_flag"),
  createdAt: timestamp("created_at").defaultNow(),
});

export const historicalBitcoinCalculations = pgTable("historical_bitcoin_calculations", {
  id: serial("id").primaryKey(),
  settlementDate: date("settlement_date").notNull(),
  settlementPeriod: integer("settlement_period").notNull(),
  bmuId: text("bmu_id").notNull(),
  minerModel: text("miner_model").notNull(),
  bitcoinMined: numeric("bitcoin_mined").notNull(),
  difficulty: numeric("difficulty").notNull(),
  calculatedAt: timestamp("calculated_at").defaultNow(),
});

export const bitcoinMonthlySummaries = pgTable("bitcoin_monthly_summaries", {
  id: serial("id").primaryKey(),
  yearMonth: text("year_month").notNull(),
  minerModel: text("miner_model").notNull(),
  bitcoinMined: numeric("bitcoin_mined").notNull(),
  valueAtMining: numeric("value_at_mining").notNull(),
  createdAt: timestamp("created_at").defaultNow(),
  updatedAt: timestamp("updated_at").defaultNow(),
});

export const bitcoinYearlySummaries = pgTable("bitcoin_yearly_summaries", {
  id: serial("id").primaryKey(),
  year: text("year").notNull(),
  minerModel: text("miner_model").notNull(),
  bitcoinMined: numeric("bitcoin_mined").notNull(),
  valueAtMining: numeric("value_at_mining").notNull(),
  createdAt: timestamp("created_at").defaultNow(),
  updatedAt: timestamp("updated_at").defaultNow(),
});

export const dailySummaries = pgTable("daily_summaries", {
  summaryDate: date("summary_date").primaryKey(),
  totalCurtailedEnergy: numeric("total_curtailed_energy"),
  totalPayment: numeric("total_payment"),
  createdAt: timestamp("created_at").defaultNow()
});

export const monthlySummaries = pgTable("monthly_summaries", {
  yearMonth: text("year_month").primaryKey(),
  totalCurtailedEnergy: numeric("total_curtailed_energy"),
  totalPayment: numeric("total_payment"),
  createdAt: timestamp("created_at").defaultNow(),
  updatedAt: timestamp("updated_at").defaultNow()
});

export const yearlySummaries = pgTable("yearly_summaries", {
  year: text("year").primaryKey(),
  totalCurtailedEnergy: numeric("total_curtailed_energy"),
  totalPayment: numeric("total_payment"),
  createdAt: timestamp("created_at").defaultNow(),
  updatedAt: timestamp("updated_at").defaultNow()
});

export const ingestionProgress = pgTable("ingestion_progress", {
  id: serial("id").primaryKey(),
  lastProcessedDate: date("last_processed_date").notNull(),
  status: text("status").notNull(),
  errorMessage: text("error_message"),
  createdAt: timestamp("created_at").defaultNow(),
  updatedAt: timestamp("updated_at").defaultNow()
});

// These materialized views have been replaced with direct query optimizations
// Keeping declarations for backward compatibility with existing code
// but these tables are no longer used in the application

// @deprecated - Use optimizedMiningService instead
export const settlementPeriodMining = pgTable("settlement_period_mining", {
  id: serial("id").primaryKey(),
  settlementDate: date("settlement_date").notNull(),
  settlementPeriod: integer("settlement_period").notNull(),
  bmuId: text("bmu_id").notNull(),
  minerModel: text("miner_model").notNull(),
  curtailedEnergy: numeric("curtailed_energy").notNull(),
  bitcoinMined: numeric("bitcoin_mined").notNull(),
  valueAtPrice: numeric("value_at_price"),
  price: numeric("price"),
  difficulty: numeric("difficulty").notNull(),
  createdAt: timestamp("created_at").defaultNow(),
});

// @deprecated - Use optimizedMiningService instead
export const dailyMiningPotential = pgTable("daily_mining_potential", {
  id: serial("id").primaryKey(),
  summaryDate: date("summary_date").notNull(),
  bmuId: text("bmu_id").notNull(),
  minerModel: text("miner_model").notNull(),
  totalCurtailedEnergy: numeric("total_curtailed_energy").notNull(),
  totalBitcoinMined: numeric("total_bitcoin_mined").notNull(),
  averageValue: numeric("average_value"),
  createdAt: timestamp("created_at").defaultNow(),
  updatedAt: timestamp("updated_at").defaultNow(),
});

// @deprecated - Use optimizedMiningService instead
export const yearlyMiningPotential = pgTable("yearly_mining_potential", {
  id: serial("id").primaryKey(),
  year: text("year").notNull(),
  bmuId: text("bmu_id").notNull(),
  minerModel: text("miner_model").notNull(),
  totalCurtailedEnergy: numeric("total_curtailed_energy").notNull(),
  totalBitcoinMined: numeric("total_bitcoin_mined").notNull(),
  averageValue: numeric("average_value"),
  createdAt: timestamp("created_at").defaultNow(),
  updatedAt: timestamp("updated_at").defaultNow(),
});

export const insertCurtailmentRecordSchema = createInsertSchema(curtailmentRecords);
export const selectCurtailmentRecordSchema = createSelectSchema(curtailmentRecords);
export const insertDailySummarySchema = createInsertSchema(dailySummaries);
export const selectDailySummarySchema = createSelectSchema(dailySummaries);
export const insertMonthlySummarySchema = createInsertSchema(monthlySummaries);
export const selectMonthlySummarySchema = createSelectSchema(monthlySummaries);
export const insertYearlySummarySchema = createInsertSchema(yearlySummaries);
export const selectYearlySummarySchema = createSelectSchema(yearlySummaries);
export const insertIngestionProgressSchema = createInsertSchema(ingestionProgress);
export const selectIngestionProgressSchema = createSelectSchema(ingestionProgress);

export const insertHistoricalBitcoinCalculationSchema = createInsertSchema(historicalBitcoinCalculations);
export const selectHistoricalBitcoinCalculationSchema = createSelectSchema(historicalBitcoinCalculations);
export const insertBitcoinMonthlySummarySchema = createInsertSchema(bitcoinMonthlySummaries);
export const selectBitcoinMonthlySummarySchema = createSelectSchema(bitcoinMonthlySummaries);
export const insertBitcoinYearlySummarySchema = createInsertSchema(bitcoinYearlySummaries);
export const selectBitcoinYearlySummarySchema = createSelectSchema(bitcoinYearlySummaries);

// @deprecated - Schemas for materialized view tables
// Keeping these for backward compatibility
export const insertSettlementPeriodMiningSchema = createInsertSchema(settlementPeriodMining);
export const selectSettlementPeriodMiningSchema = createSelectSchema(settlementPeriodMining);
export const insertDailyMiningPotentialSchema = createInsertSchema(dailyMiningPotential);
export const selectDailyMiningPotentialSchema = createSelectSchema(dailyMiningPotential);
export const insertYearlyMiningPotentialSchema = createInsertSchema(yearlyMiningPotential);
export const selectYearlyMiningPotentialSchema = createSelectSchema(yearlyMiningPotential);

// Define types for all tables
export type CurtailmentRecord = typeof curtailmentRecords.$inferSelect;
export type InsertCurtailmentRecord = typeof curtailmentRecords.$inferInsert;
export type DailySummary = typeof dailySummaries.$inferSelect;
export type InsertDailySummary = typeof dailySummaries.$inferInsert;
export type MonthlySummary = typeof monthlySummaries.$inferSelect;
export type InsertMonthlySummary = typeof monthlySummaries.$inferInsert;
export type YearlySummary = typeof yearlySummaries.$inferSelect;
export type InsertYearlySummary = typeof yearlySummaries.$inferInsert;
export type IngestionProgress = typeof ingestionProgress.$inferSelect;
export type InsertIngestionProgress = typeof ingestionProgress.$inferInsert;

export type HistoricalBitcoinCalculation = typeof historicalBitcoinCalculations.$inferSelect;
export type InsertHistoricalBitcoinCalculation = typeof historicalBitcoinCalculations.$inferInsert;
export type BitcoinMonthlySummary = typeof bitcoinMonthlySummaries.$inferSelect;
export type InsertBitcoinMonthlySummary = typeof bitcoinMonthlySummaries.$inferInsert;
export type BitcoinYearlySummary = typeof bitcoinYearlySummaries.$inferSelect;
export type InsertBitcoinYearlySummary = typeof bitcoinYearlySummaries.$inferInsert;

// @deprecated - Types for materialized view tables
// Keeping these for backward compatibility
export type SettlementPeriodMining = typeof settlementPeriodMining.$inferSelect;
export type InsertSettlementPeriodMining = typeof settlementPeriodMining.$inferInsert;
export type DailyMiningPotential = typeof dailyMiningPotential.$inferSelect;
export type InsertDailyMiningPotential = typeof dailyMiningPotential.$inferInsert;
export type YearlyMiningPotential = typeof yearlyMiningPotential.$inferSelect;
export type InsertYearlyMiningPotential = typeof yearlyMiningPotential.$inferInsert;