import { pgTable, text, serial, date, integer, numeric, boolean, timestamp, index, uniqueIndex } from "drizzle-orm/pg-core";
import { createInsertSchema, createSelectSchema } from "drizzle-zod";

export const curtailmentRecords = pgTable("curtailment_records", {
  id: serial("id").primaryKey(),
  settlementDate: date("settlement_date").notNull(),
  settlementPeriod: integer("settlement_period").notNull(),
  farmId: text("farm_id").notNull(),
  volume: numeric("volume").notNull(),
  payment: numeric("payment").notNull(),
  originalPrice: numeric("original_price").notNull(),
  finalPrice: numeric("final_price").notNull(),
  soFlag: boolean("so_flag"),
  cadlFlag: boolean("cadl_flag"),
  createdAt: timestamp("created_at").defaultNow()
}, (table) => ({
  farmIdIdx: index("farm_id_idx").on(table.farmId),
  dateIdx: index("settlement_date_idx").on(table.settlementDate)
}));

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

export const ingestionProgress = pgTable("ingestion_progress", {
  id: serial("id").primaryKey(),
  lastProcessedDate: date("last_processed_date").notNull(),
  status: text("status").notNull(),
  errorMessage: text("error_message"),
  createdAt: timestamp("created_at").defaultNow(),
  updatedAt: timestamp("updated_at").defaultNow()
});

export const farmDailySummaries = pgTable("farm_daily_summaries", {
  id: serial("id").primaryKey(),
  farmId: text("farm_id").notNull(),
  summaryDate: date("summary_date").notNull(),
  totalCurtailedEnergy: numeric("total_curtailed_energy").notNull(),
  totalPayment: numeric("total_payment").notNull(),
  averageOriginalPrice: numeric("average_original_price").notNull(),
  averageFinalPrice: numeric("average_final_price").notNull(),
  curtailmentEvents: integer("curtailment_events").notNull(),
  soFlaggedEvents: integer("so_flagged_events").notNull(),
  cadlFlaggedEvents: integer("cadl_flagged_events").notNull(),
  createdAt: timestamp("created_at").defaultNow(),
  updatedAt: timestamp("updated_at").defaultNow()
}, (table) => ({
  farmDateUniqueIdx: uniqueIndex("farm_date_unique_idx").on(table.farmId, table.summaryDate),
  farmIdIdx: index("farm_daily_farm_id_idx").on(table.farmId),
  dateIdx: index("farm_daily_date_idx").on(table.summaryDate)
}));

export const insertCurtailmentRecordSchema = createInsertSchema(curtailmentRecords);
export const selectCurtailmentRecordSchema = createSelectSchema(curtailmentRecords);
export const insertDailySummarySchema = createInsertSchema(dailySummaries);
export const selectDailySummarySchema = createSelectSchema(dailySummaries);
export const insertMonthlySummarySchema = createInsertSchema(monthlySummaries);
export const selectMonthlySummarySchema = createSelectSchema(monthlySummaries);
export const insertIngestionProgressSchema = createInsertSchema(ingestionProgress);
export const selectIngestionProgressSchema = createSelectSchema(ingestionProgress);
export const insertFarmDailySummarySchema = createInsertSchema(farmDailySummaries);
export const selectFarmDailySummarySchema = createSelectSchema(farmDailySummaries);

export type CurtailmentRecord = typeof curtailmentRecords.$inferSelect;
export type InsertCurtailmentRecord = typeof curtailmentRecords.$inferInsert;
export type DailySummary = typeof dailySummaries.$inferSelect;
export type InsertDailySummary = typeof dailySummaries.$inferInsert;
export type MonthlySummary = typeof monthlySummaries.$inferSelect;
export type InsertMonthlySummary = typeof monthlySummaries.$inferInsert;
export type IngestionProgress = typeof ingestionProgress.$inferSelect;
export type InsertIngestionProgress = typeof ingestionProgress.$inferInsert;
export type FarmDailySummary = typeof farmDailySummaries.$inferSelect;
export type InsertFarmDailySummary = typeof farmDailySummaries.$inferInsert;