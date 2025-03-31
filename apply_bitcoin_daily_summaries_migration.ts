/**
 * Apply Bitcoin Daily Summaries Migration
 * 
 * This script applies the migration to create the bitcoin_daily_summaries table
 * in the database if it doesn't exist.
 * 
 * Usage:
 *   npx tsx apply_bitcoin_daily_summaries_migration.ts
 */

import { db } from './db';
import fs from 'fs';
import path from 'path';
import { sql } from 'drizzle-orm';

async function log(message: string, level: "info" | "error" | "warning" | "success" = "info"): Promise<void> {
  const timestamp = new Date().toISOString();
  const prefix = level === "info" 
    ? "\x1b[37m[INFO]" 
    : level === "error" 
      ? "\x1b[31m[ERROR]" 
      : level === "warning" 
        ? "\x1b[33m[WARNING]" 
        : "\x1b[32m[SUCCESS]";
  
  console.log(`[${timestamp}] ${prefix} ${message}\x1b[0m`);
}

async function applyMigration(): Promise<void> {
  try {
    log("=== Applying Bitcoin Daily Summaries Migration ===");
    
    // Read the migration file
    const migrationFile = path.join(process.cwd(), 'db', 'migrations', '002_add_bitcoin_daily_summaries.sql');
    const migrationSql = fs.readFileSync(migrationFile, 'utf8');
    
    // Execute the migration
    await db.execute(sql.raw(migrationSql));
    
    log("=== Migration successfully applied ===", "success");
  } catch (error) {
    log(`Error applying migration: ${error}`, "error");
    throw error;
  }
}

// Run the main function
applyMigration().catch(error => {
  console.error("Fatal error:", error);
  process.exit(1);
});