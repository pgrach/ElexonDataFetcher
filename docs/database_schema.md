# Bitcoin Mining Analytics Platform - Database Schema

This document describes the database schema used in the Bitcoin Mining Analytics platform.

## Schema Overview

The database schema consists of several interconnected tables that form the foundation of the data model. The schema is implemented using PostgreSQL with Drizzle ORM for type safety and query building.

## Core Tables

### Curtailment Records

The `curtailment_records` table stores the raw curtailment data from the Elexon API.

```sql
CREATE TABLE curtailment_records (
  id SERIAL PRIMARY KEY,
  settlement_date DATE NOT NULL,
  settlement_period INTEGER NOT NULL,
  bmu_id VARCHAR(255) NOT NULL,
  volume DECIMAL(15, 5) NOT NULL,
  original_price DECIMAL(15, 5) NOT NULL,
  final_price DECIMAL(15, 5) NOT NULL,
  payment DECIMAL(15, 5) NOT NULL,
  so_flag BOOLEAN NOT NULL,
  cadl_flag BOOLEAN,
  lead_party VARCHAR(255),
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  UNIQUE(settlement_date, settlement_period, bmu_id)
);

CREATE INDEX idx_curtailment_settlement_date ON curtailment_records(settlement_date);
CREATE INDEX idx_curtailment_lead_party ON curtailment_records(lead_party);
CREATE INDEX idx_curtailment_bmu_id ON curtailment_records(bmu_id);
```

### Historical Bitcoin Calculations

The `historical_bitcoin_calculations` table stores the calculated Bitcoin mining potential for each curtailment record.

```sql
CREATE TABLE historical_bitcoin_calculations (
  id SERIAL PRIMARY KEY,
  settlement_date DATE NOT NULL,
  settlement_period INTEGER NOT NULL,
  farm_id VARCHAR(255) NOT NULL,
  miner_model VARCHAR(50) NOT NULL,
  curtailed_energy DECIMAL(15, 5) NOT NULL,
  bitcoin_mined DECIMAL(18, 8) NOT NULL,
  difficulty BIGINT NOT NULL,
  calculated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  UNIQUE(settlement_date, settlement_period, farm_id, miner_model)
);

CREATE INDEX idx_bitcoin_calc_date ON historical_bitcoin_calculations(settlement_date);
CREATE INDEX idx_bitcoin_calc_model ON historical_bitcoin_calculations(miner_model);
CREATE INDEX idx_bitcoin_calc_farm ON historical_bitcoin_calculations(farm_id);
```

## Summary Tables

### Bitcoin Monthly Summaries

The `bitcoin_monthly_summaries` table aggregates Bitcoin calculations by month and miner model.

```sql
CREATE TABLE bitcoin_monthly_summaries (
  id SERIAL PRIMARY KEY,
  year_month VARCHAR(7) NOT NULL,
  miner_model VARCHAR(50) NOT NULL,
  bitcoin_mined DECIMAL(18, 8) NOT NULL,
  curtailed_energy DECIMAL(15, 5) NOT NULL,
  average_difficulty DECIMAL(18, 2) NOT NULL,
  last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  UNIQUE(year_month, miner_model)
);

CREATE INDEX idx_btc_monthly_year_month ON bitcoin_monthly_summaries(year_month);
```

### Bitcoin Yearly Summaries

The `bitcoin_yearly_summaries` table aggregates Bitcoin calculations by year and miner model.

```sql
CREATE TABLE bitcoin_yearly_summaries (
  id SERIAL PRIMARY KEY,
  year VARCHAR(4) NOT NULL,
  miner_model VARCHAR(50) NOT NULL,
  bitcoin_mined DECIMAL(18, 8) NOT NULL,
  curtailed_energy DECIMAL(15, 5) NOT NULL,
  average_difficulty DECIMAL(18, 2) NOT NULL,
  last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  UNIQUE(year, miner_model)
);

CREATE INDEX idx_btc_yearly_year ON bitcoin_yearly_summaries(year);
```

### Curtailment Summaries

The platform also maintains several summary tables for curtailment data:

```sql
CREATE TABLE daily_summaries (
  id SERIAL PRIMARY KEY,
  date DATE UNIQUE NOT NULL,
  total_curtailed_energy DECIMAL(15, 5) NOT NULL,
  total_payment DECIMAL(15, 5) NOT NULL,
  last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE monthly_summaries (
  id SERIAL PRIMARY KEY,
  year_month VARCHAR(7) UNIQUE NOT NULL,
  total_curtailed_energy DECIMAL(15, 5) NOT NULL,
  total_payment DECIMAL(15, 5) NOT NULL,
  last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE yearly_summaries (
  id SERIAL PRIMARY KEY,
  year VARCHAR(4) UNIQUE NOT NULL,
  total_curtailed_energy DECIMAL(15, 5) NOT NULL,
  total_payment DECIMAL(15, 5) NOT NULL,
  last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
```

### Support Tables

```sql
CREATE TABLE ingestion_progress (
  id SERIAL PRIMARY KEY,
  date DATE UNIQUE NOT NULL,
  status VARCHAR(50) NOT NULL,
  records_processed INTEGER NOT NULL DEFAULT 0,
  error_message TEXT,
  started_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  completed_at TIMESTAMP WITH TIME ZONE
);
```

## Table Relationships

The relationships between tables create a hierarchical data structure:

1. **Primary Data Collection**:
   - Curtailment records are ingested from the Elexon API
   - Bitcoin calculations are derived from curtailment records

2. **Aggregation Hierarchy**:
   - Daily level: Individual settlement periods (48 per day)
   - Monthly level: Aggregated daily data
   - Yearly level: Aggregated monthly data

3. **Curtailment to Bitcoin Flow**:
   - Curtailment records → Historical Bitcoin calculations
   - Historical Bitcoin calculations → Monthly Bitcoin summaries
   - Monthly Bitcoin summaries → Yearly Bitcoin summaries

## Indexing Strategy

The database uses a strategic indexing approach:

1. **Primary Keys**: Every table has an integer primary key for efficient joins
2. **Unique Constraints**: Used to prevent duplicate records
3. **Composite Indexes**: Created on frequently queried combinations
4. **Foreign Key Indexes**: Added to improve join performance

## Data Types

The schema uses appropriate data types for each field:

- `DECIMAL(15, 5)` for energy and monetary values
- `DECIMAL(18, 8)` for Bitcoin amounts (8 decimal places)
- `BIGINT` for difficulty values
- `VARCHAR` for identifiers and string values
- `DATE` for settlement dates
- `TIMESTAMP WITH TIME ZONE` for time tracking

## Implementation with Drizzle ORM

The schema is implemented using Drizzle ORM in the `db/schema.ts` file. This provides:

1. Type-safe database operations
2. Automatic SQL generation
3. Schema validation
4. Migration support

## Schema Evolution

The schema has evolved over time to accommodate new requirements:

1. Initial version: Basic curtailment records and Bitcoin calculations
2. Addition of summary tables for aggregated data
3. Implementation of ingestion progress tracking
4. Optimization of indexes for query performance

## Performance Considerations

The schema is designed for efficient querying:

1. Denormalized structure for faster reporting
2. Strategic indexes on frequently queried columns
3. Composite indexes for common query patterns
4. Regular statistics updates for query planner optimization