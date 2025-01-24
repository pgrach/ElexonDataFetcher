import { pgTable, text, serial, date, integer, numeric, boolean, timestamp, unique } from "drizzle-orm/pg-core";
import { createInsertSchema, createSelectSchema } from "drizzle-zod";
import { z } from "zod";

export const curtailmentRecords = pgTable("curtailment_records", {
  id: serial("id").primaryKey(),
  settlementDate: date("settlement_date").notNull(),
  settlementPeriod: integer("settlement_period").notNull(),
  farmId: text("farm_id").notNull(),
  leadPartyName: text("lead_party_name"),  // Making it nullable initially
  volume: numeric("volume").notNull(),
  payment: numeric("payment").notNull(),
  originalPrice: numeric("original_price").notNull(),
  finalPrice: numeric("final_price").notNull(),
  soFlag: boolean("so_flag"),
  cadlFlag: boolean("cadl_flag"),
  createdAt: timestamp("created_at").defaultNow()
}, (table) => {
  return {
    uniqueSettlementRecord: unique().on(
      table.settlementDate, 
      table.settlementPeriod,
      table.farmId
    )
  };
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

export const ingestionProgress = pgTable("ingestion_progress", {
  id: serial("id").primaryKey(),
  lastProcessedDate: date("last_processed_date").notNull(),
  status: text("status").notNull(),
  errorMessage: text("error_message"),
  createdAt: timestamp("created_at").defaultNow(),
  updatedAt: timestamp("updated_at").defaultNow()
});

// Custom Zod schema for curtailment records
const curtailmentRecordValidationSchema = z.object({
  settlementPeriod: z.number().min(1).max(48),
  volume: z.string().regex(/^\d+(\.\d+)?$/),  // Positive numeric string
  payment: z.string().regex(/^-?\d+(\.\d+)?$/),  // Numeric string
  originalPrice: z.string().regex(/^-?\d+(\.\d+)?$/),  // Numeric string
  finalPrice: z.string().regex(/^-?\d+(\.\d+)?$/),  // Numeric string
});

export const insertCurtailmentRecordSchema = createInsertSchema(curtailmentRecords);
export const selectCurtailmentRecordSchema = createSelectSchema(curtailmentRecords);
export const insertDailySummarySchema = createInsertSchema(dailySummaries);
export const selectDailySummarySchema = createSelectSchema(dailySummaries);
export const insertMonthlySummarySchema = createInsertSchema(monthlySummaries);
export const selectMonthlySummarySchema = createSelectSchema(monthlySummaries);
export const insertIngestionProgressSchema = createInsertSchema(ingestionProgress);
export const selectIngestionProgressSchema = createSelectSchema(ingestionProgress);

export type CurtailmentRecord = typeof curtailmentRecords.$inferSelect;
export type InsertCurtailmentRecord = typeof curtailmentRecords.$inferInsert;
export type DailySummary = typeof dailySummaries.$inferSelect;
export type InsertDailySummary = typeof dailySummaries.$inferInsert;
export type MonthlySummary = typeof monthlySummaries.$inferSelect;
export type InsertMonthlySummary = typeof monthlySummaries.$inferInsert;
export type IngestionProgress = typeof ingestionProgress.$inferSelect;
export type InsertIngestionProgress = typeof ingestionProgress.$inferInsert;