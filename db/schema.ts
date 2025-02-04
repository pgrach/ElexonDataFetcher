import { pgTable, text, serial, date, integer, numeric, boolean, timestamp, index, unique } from "drizzle-orm/pg-core";
import { createInsertSchema, createSelectSchema } from "drizzle-zod";

export const curtailmentRecords = pgTable("curtailment_records", {
  id: serial("id").primaryKey(),
  settlementDate: date("settlement_date").notNull(),
  settlementPeriod: integer("settlement_period").notNull(),
  farmId: text("farm_id").notNull(),
  leadPartyName: text("lead_party_name"),  
  volume: numeric("volume").notNull(),
  payment: numeric("payment").notNull(),
  originalPrice: numeric("original_price").notNull(),
  finalPrice: numeric("final_price").notNull(),
  soFlag: boolean("so_flag"),
  cadlFlag: boolean("cadl_flag"),
  createdAt: timestamp("created_at").defaultNow(),
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

export const bitcoinDifficultyHistory = pgTable("bitcoin_difficulty_history", {
  id: serial("id").primaryKey(),
  timestamp: timestamp("timestamp").notNull(),
  difficulty: numeric("difficulty").notNull(),
  price: numeric("price").notNull(),
  createdAt: timestamp("created_at").defaultNow()
}, (table) => ({
  timestampIdx: index("bitcoin_difficulty_history_timestamp_idx").on(table.timestamp)
}));

export const settlementPeriodMining = pgTable("settlement_period_mining", {
  id: serial("id").primaryKey(),
  settlementDate: date("settlement_date").notNull(),
  settlementPeriod: integer("settlement_period").notNull(),
  farmId: text("farm_id").notNull(),
  curtailedEnergy: numeric("curtailed_energy").notNull(),
  bitcoinMined: numeric("bitcoin_mined").notNull(),
  valueAtPrice: numeric("value_at_price").notNull(),
  difficulty: numeric("difficulty").notNull(),
  price: numeric("price").notNull(),
  minerModel: text("miner_model").notNull(),
  createdAt: timestamp("created_at").defaultNow(),
}, (table) => ({
  uniquePeriodIdx: unique("settlement_period_mining_unique_idx").on(
    table.settlementDate,
    table.settlementPeriod,
    table.farmId,
    table.minerModel
  )
}));

export const dailyMiningPotential = pgTable("daily_mining_potential", {
  id: serial("id").primaryKey(),
  summaryDate: date("summary_date").notNull(),
  farmId: text("farm_id").notNull(),
  minerModel: text("miner_model").notNull(),
  totalCurtailedEnergy: numeric("total_curtailed_energy").notNull(),
  totalBitcoinMined: numeric("total_bitcoin_mined").notNull(),
  averageValue: numeric("average_value").notNull(),
  createdAt: timestamp("created_at").defaultNow(),
  updatedAt: timestamp("updated_at").defaultNow()
}, (table) => ({
  uniqueDailyIdx: unique("daily_mining_potential_unique_idx").on(
    table.summaryDate,
    table.farmId,
    table.minerModel
  )
}));

export const monthlyMiningPotential = pgTable("monthly_mining_potential", {
  id: serial("id").primaryKey(),
  yearMonth: text("year_month").notNull(),
  farmId: text("farm_id").notNull(),
  minerModel: text("miner_model").notNull(),
  totalCurtailedEnergy: numeric("total_curtailed_energy").notNull(),
  totalBitcoinMined: numeric("total_bitcoin_mined").notNull(),
  averageValue: numeric("average_value").notNull(),
  createdAt: timestamp("created_at").defaultNow(),
  updatedAt: timestamp("updated_at").defaultNow()
}, (table) => ({
  uniqueMonthlyIdx: unique("monthly_mining_potential_unique_idx").on(
    table.yearMonth,
    table.farmId,
    table.minerModel
  )
}));

export const yearlyMiningPotential = pgTable("yearly_mining_potential", {
  id: serial("id").primaryKey(),
  year: text("year").notNull(),
  farmId: text("farm_id").notNull(),
  minerModel: text("miner_model").notNull(),
  totalCurtailedEnergy: numeric("total_curtailed_energy").notNull(),
  totalBitcoinMined: numeric("total_bitcoin_mined").notNull(),
  averageValue: numeric("average_value").notNull(),
  createdAt: timestamp("created_at").defaultNow(),
  updatedAt: timestamp("updated_at").defaultNow()
}, (table) => ({
  uniqueYearlyIdx: unique("yearly_mining_potential_unique_idx").on(
    table.year,
    table.farmId,
    table.minerModel
  )
}));

export const ingestionProgress = pgTable("ingestion_progress", {
  id: serial("id").primaryKey(),
  lastProcessedDate: date("last_processed_date").notNull(),
  status: text("status").notNull(),
  errorMessage: text("error_message"),
  createdAt: timestamp("created_at").defaultNow(),
  updatedAt: timestamp("updated_at").defaultNow()
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
export const insertBitcoinDifficultyHistorySchema = createInsertSchema(bitcoinDifficultyHistory);
export const selectBitcoinDifficultyHistorySchema = createSelectSchema(bitcoinDifficultyHistory);
export const insertSettlementPeriodMiningSchema = createInsertSchema(settlementPeriodMining);
export const selectSettlementPeriodMiningSchema = createSelectSchema(settlementPeriodMining);
export const insertDailyMiningPotentialSchema = createInsertSchema(dailyMiningPotential);
export const selectDailyMiningPotentialSchema = createSelectSchema(dailyMiningPotential);
export const insertMonthlyMiningPotentialSchema = createInsertSchema(monthlyMiningPotential);
export const selectMonthlyMiningPotentialSchema = createSelectSchema(monthlyMiningPotential);
export const insertYearlyMiningPotentialSchema = createInsertSchema(yearlyMiningPotential);
export const selectYearlyMiningPotentialSchema = createSelectSchema(yearlyMiningPotential);

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
export type BitcoinDifficultyHistory = typeof bitcoinDifficultyHistory.$inferSelect;
export type InsertBitcoinDifficultyHistory = typeof bitcoinDifficultyHistory.$inferInsert;
export type SettlementPeriodMining = typeof settlementPeriodMining.$inferSelect;
export type InsertSettlementPeriodMining = typeof settlementPeriodMining.$inferInsert;
export type DailyMiningPotential = typeof dailyMiningPotential.$inferSelect;
export type InsertDailyMiningPotential = typeof dailyMiningPotential.$inferInsert;
export type MonthlyMiningPotential = typeof monthlyMiningPotential.$inferSelect;
export type InsertMonthlyMiningPotential = typeof monthlyMiningPotential.$inferInsert;
export type YearlyMiningPotential = typeof yearlyMiningPotential.$inferSelect;
export type InsertYearlyMiningPotential = typeof yearlyMiningPotential.$inferInsert;