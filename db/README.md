# Bitcoin Mining Analytics Platform - Database Directory

This directory contains database-related code for the Bitcoin Mining Analytics platform.

## Files

- `index.ts` - Database connection setup using Drizzle ORM
- `schema.ts` - Database schema definitions using Drizzle ORM

## Database Schema

The database schema is defined in `schema.ts` using Drizzle ORM. It consists of several interconnected tables:

### Primary Tables

- `curtailment_records` - Stores raw curtailment data from the Elexon API
- `historical_bitcoin_calculations` - Stores Bitcoin mining potential calculations

### Summary Tables

- `daily_summaries` - Aggregates curtailment data by day
- `monthly_summaries` - Aggregates curtailment data by month
- `yearly_summaries` - Aggregates curtailment data by year
- `bitcoin_monthly_summaries` - Aggregates Bitcoin calculations by month
- `bitcoin_yearly_summaries` - Aggregates Bitcoin calculations by year

### Support Tables

- `ingestion_progress` - Tracks progress of data ingestion

For a detailed description of the database schema, see `docs/database_schema.md`.

## Database Connection

The database connection is set up in `index.ts` using Drizzle ORM:

```typescript
import { drizzle } from 'drizzle-orm/node-postgres';
import { Pool } from 'pg';

// Create a PostgreSQL connection pool
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

// Create a Drizzle ORM instance
export const db = drizzle(pool);
```

## Schema Management

The database schema is managed using Drizzle ORM's migration system:

1. Define tables in `schema.ts`
2. Generate migrations using Drizzle Kit
3. Run migrations to update the database schema

### Generating Migrations

To generate migrations based on schema changes:

```bash
npm run db:generate
```

This generates SQL migration files in the `migrations/` directory.

### Running Migrations

To run migrations and update the database schema:

```bash
npm run db:push
```

## Type Safety

Drizzle ORM provides type safety for database operations through TypeScript types:

```typescript
// Example types from schema.ts
export type CurtailmentRecord = typeof curtailmentRecords.$inferSelect;
export type InsertCurtailmentRecord = typeof curtailmentRecords.$inferInsert;
export type HistoricalBitcoinCalculation = typeof historicalBitcoinCalculations.$inferSelect;
export type InsertHistoricalBitcoinCalculation = typeof historicalBitcoinCalculations.$inferInsert;
```

These types ensure that database operations use the correct field names and data types.

## Database Operations

Database operations are performed using Drizzle ORM's query builder:

```typescript
// Example query to select curtailment records
const records = await db.select()
  .from(curtailmentRecords)
  .where(eq(curtailmentRecords.settlementDate, date))
  .orderBy(asc(curtailmentRecords.settlementPeriod));

// Example query to insert a record
await db.insert(curtailmentRecords)
  .values({
    settlementDate: date,
    settlementPeriod: period,
    bmuId: bmuId,
    volume: volume,
    originalPrice: originalPrice,
    finalPrice: finalPrice,
    payment: payment,
    soFlag: soFlag,
    cadlFlag: cadlFlag,
    leadParty: leadParty
  })
  .onConflictDoUpdate({
    target: [
      curtailmentRecords.settlementDate,
      curtailmentRecords.settlementPeriod,
      curtailmentRecords.bmuId
    ],
    set: {
      volume: volume,
      originalPrice: originalPrice,
      finalPrice: finalPrice,
      payment: payment,
      soFlag: soFlag,
      cadlFlag: cadlFlag,
      leadParty: leadParty,
      updatedAt: new Date()
    }
  });
```

## Best Practices

1. **Use Transactions**: Use transactions for operations that modify multiple tables
2. **Parameterized Queries**: Always use parameterized queries to prevent SQL injection
3. **Connection Pooling**: Use connection pooling to manage database connections efficiently
4. **Error Handling**: Implement proper error handling for database operations
5. **Migrations**: Use migrations to manage schema changes
6. **Indexes**: Add indexes for frequently queried columns
7. **Type Safety**: Leverage TypeScript types for type-safe database operations