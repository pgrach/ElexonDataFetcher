# Database Optimization Guide

This document provides an overview of the database optimizations made to improve performance and maintainability of the Bitcoin mining analytics platform.

## Overview

The optimization focuses on creating materialized view tables that pre-calculate frequently accessed mining potential data to reduce query time and server load. These tables mirror the structure of traditional views but store the results physically, providing faster access at the cost of needing periodic refreshes.

## New Tables

Three new tables have been added to the schema:

1. **settlement_period_mining**: Stores per-settlement-period mining potential data
   - Contains bitcoin mining calculations at the most granular level (settlement period)
   - Used for detailed time-series analysis and visualizations

2. **daily_mining_potential**: Aggregates mining data on a daily basis
   - Pre-calculates daily totals per farm and miner model
   - Improves performance for the most common daily reports and charts

3. **yearly_mining_potential**: Stores annual mining potential summaries
   - Provides high-level aggregated data for yearly reporting
   - Enables efficient year-over-year comparisons

## Implementation Details

### Database Schema Updates
The tables are defined in `db/schema.ts` with appropriate types and schemas for integration with the Drizzle ORM.

### Migration
A migration script is provided in `migrations/add_materialized_views.sql` that:
- Checks if tables already exist before creating them
- Adds appropriate indexes for optimization
- Can be run safely multiple times without causing data loss

### Population Service
A dedicated service in `server/services/miningPotentialService.ts` handles:
- Initial population of the materialized view tables
- Automated refreshing when new data is processed
- Fallback to original tables when materialized data is not available

### API Integration
New API endpoints have been added in `server/routes/miningPotentialRoutes.ts` that:
- Expose the optimized data through RESTful endpoints
- Maintain backwards compatibility with existing endpoints
- Provide improved performance for client-side data requests

## Usage

### Running Migrations
To create the necessary tables:
```bash
npx tsx run_migration.ts
```

### Populating Historical Data
To populate materialized views with existing data:
```bash
# Populate recent data (default: last 30 days)
npx tsx populate_materialized_views.ts

# Populate specific date range
npx tsx populate_materialized_views.ts range 2023-01-01 2023-12-31
```

### Using the API
The new optimized endpoints are available at:
- `/api/mining-potential/daily?date=YYYY-MM-DD&minerModel=MODEL_NAME`
- `/api/mining-potential/yearly/YYYY?minerModel=MODEL_NAME`

These endpoints provide the same data structure as the original endpoints, ensuring seamless integration with existing frontend code.

## Benefits

1. **Improved Performance**: Pre-calculated data reduces query time significantly, especially for complex aggregations
2. **Reduced Database Load**: Fewer ad-hoc complex queries leads to better overall database performance
3. **Consistent Structure**: Standardized schema improves code maintainability and reduces technical debt
4. **Transparent Fallback**: If materialized data is missing, the system falls back to original calculation methods
5. **DRY Implementation**: Eliminates duplicated calculation logic across multiple endpoints

## Maintenance Considerations

The materialized views need periodic refreshing to stay current with the latest data. This happens automatically through two mechanisms:

1. **On-Demand Refresh**: When data is accessed but not found in the materialized tables
2. **Proactive Refresh**: When new curtailment data is processed via the ingestion system

For manual refreshing, use the population script as described above.