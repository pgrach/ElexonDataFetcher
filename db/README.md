# Bitcoin Mining Analytics Platform - Database

This directory contains the database configuration and schema definitions for the Bitcoin Mining Analytics platform.

## Files

- `index.ts` - Database connection setup using Drizzle ORM
- `schema.ts` - Table definitions and relationships for PostgreSQL

## Database Schema

The database schema includes the following tables:

### Primary Tables

- `curtailment_records` - Stores raw curtailment data from the Elexon API
  - Includes settlement date, period, BMU ID, volume, and payment information

- `historical_bitcoin_calculations` - Stores Bitcoin mining potential calculations
  - Links to curtailment records and includes Bitcoin mined, difficulty, and date information

### Summary Tables

- `bitcoin_monthly_summaries` - Monthly aggregations of Bitcoin calculations by miner model
- `bitcoin_yearly_summaries` - Yearly aggregations of Bitcoin calculations by miner model
- `daily_summaries` - Daily aggregations of curtailment records
- `monthly_summaries` - Monthly aggregations of curtailment records
- `yearly_summaries` - Yearly aggregations of curtailment records

### Support Tables

- `ingestion_progress` - Tracks data ingestion progress
- `settlement_period_mining` - Settlement period mining calculations
- `daily_mining_potential` - Daily mining potential calculations
- `yearly_mining_potential` - Yearly mining potential calculations

## Drizzle ORM

The application uses Drizzle ORM for database operations:

- Type-safe database operations
- Schema definition with TypeScript
- Query building with a fluent API
- Migration support

## Database Operations

Database operations are centralized in the `server/utils/database.ts` file, which provides standardized functions for:

- Executing queries with error handling
- Checking if tables exist
- Getting record counts
- Batch inserting data
- Transaction management
- Database health monitoring

## Environment Configuration

Database configuration is defined in `drizzle.config.ts` in the root directory, with settings pulled from environment variables.

The required environment variable is:
- `DATABASE_URL` - PostgreSQL connection string