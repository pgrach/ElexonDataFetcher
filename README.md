# Bitcoin Mining Analytics Platform

An advanced Bitcoin mining analytics platform that provides comprehensive insights into cryptocurrency and wind farm performance through sophisticated data reconciliation and real-time tracking technologies.

## Table of Contents

- [Overview](#overview)
- [Technical Architecture](#technical-architecture)
- [Key Features](#key-features)
- [Reconciliation System](#reconciliation-system)
  - [Data Flow Architecture](#data-flow-architecture)
  - [Available Tools](#available-tools)
  - [Common Commands](#common-commands)
  - [Current Status](#current-status)
- [Development Guide](#development-guide)
  - [Core Files](#core-files)
  - [Running the Project](#running-the-project)
  - [Directory Structure](#directory-structure)
- [API Documentation](#api-documentation)
- [Optimization Strategies](#optimization-strategies)
  - [Database Optimizations](#database-optimizations)
  - [Error Handling](#error-handling)
  - [Performance Monitoring](#performance-monitoring)
- [Maintenance](#maintenance)
  - [Automated Checks](#automated-checks)
  - [Recovery Procedures](#recovery-procedures)
  - [Troubleshooting](#troubleshooting)

## Overview

The Bitcoin Mining Analytics platform enables the analysis of potential Bitcoin mining operations using curtailed wind energy. It ingests curtailment data from renewable energy sources, calculates potential Bitcoin mining output, and provides comprehensive analytics through an interactive dashboard.

The system's core function is to maintain data integrity between curtailment records and corresponding Bitcoin mining calculations, ensuring accurate insights for optimization decisions.

## Technical Architecture

- **Backend**: TypeScript/Node.js with Express
- **Database**: PostgreSQL (primary) and AWS DynamoDB (for blockchain data)
- **Frontend**: React with Tailwind CSS and shadcn/ui components
- **State Management**: React Query for data fetching and caching
- **Build System**: Vite for fast development and optimized production builds
- **API Integrations**: Elexon API for energy data, external services for cryptocurrency metrics

The platform follows a modern full-stack JavaScript architecture with advanced data processing capabilities:

```
┌────────────────┐     ┌────────────────┐     ┌────────────────┐
│   Elexon API   │────▶│  Express API   │────▶│  React Client  │
└────────────────┘     └───────┬────────┘     └────────────────┘
                              │                        │
                              ▼                        ▼
┌────────────────┐     ┌────────────────┐     ┌────────────────┐
│    DynamoDB    │◀───▶│   PostgreSQL   │     │  Data Viz &    │
│ (Difficulty)   │     │ (Curtailment)  │     │   Dashboard    │
└────────────────┘     └────────────────┘     └────────────────┘
```

## Key Features

- **Comprehensive Data Reconciliation**: Ensures integrity between curtailment records and Bitcoin calculations
- **Multi-dimensional Analytics**: Farm-specific, daily, monthly, and yearly views
- **Real-time Visualization**: Dynamic charts for curtailment, Bitcoin mining potential, and financial metrics
- **Multiple Miner Models**: Support for various Bitcoin mining hardware with different efficiency profiles
- **Automated Monitoring**: Daily reconciliation checks for data completeness
- **Optimized Performance**: Direct query optimization for large datasets without materialized views

## Reconciliation System

The reconciliation system is the core component that ensures data integrity between curtailment records and Bitcoin mining calculations.

### Data Flow Architecture

The platform's data follows this flow:

1. **Curtailment Records**: Raw data ingested from external APIs about energy curtailment
2. **Historical Bitcoin Calculations**: Calculations of potential Bitcoin mining based on curtailment
3. **Monthly/Yearly Summaries**: Aggregated statistics for reporting

Critical database tables:
- `curtailment_records`: Contains curtailment data from Elexon API
- `historical_bitcoin_calculations`: Contains mining calculations for each curtailment record
- `daily_summaries`, `monthly_summaries`, `yearly_summaries`: Aggregated statistics

### Available Tools

Several specialized tools are available to maintain data integrity:

#### 1. Unified Reconciliation System

The primary tool for data integrity management:

```bash
npx tsx unified_reconciliation.ts [command] [options]
```

Commands:
- `status` - Show current reconciliation status
- `analyze` - Analyze missing calculations and detect issues
- `reconcile [batchSize]` - Process all missing calculations with specified batch size
- `date YYYY-MM-DD` - Process a specific date
- `range YYYY-MM-DD YYYY-MM-DD [batchSize]` - Process a date range
- `critical DATE` - Process a problematic date with extra safeguards
- `spot-fix DATE PERIOD FARM` - Fix a specific date-period-farm combination

#### 2. Daily Reconciliation Check

Automatically checks the reconciliation status for recent dates and processes any missing calculations:

```bash
npx tsx daily_reconciliation_check.ts [days=2] [forceProcess=false]
```

Options:
- `days` - Number of recent days to check (default: 2)
- `forceProcess` - Force processing even if no issues found (default: false)

#### 3. Specialized Data Processing Scripts

For specific data management needs:

- **ingestMonthlyData.ts** - Processes monthly data ingestion from Elexon
- **processDifficultyMismatch.ts** - Fixes difficulty inconsistencies
- **updateHistoricalCalculations.ts** - Updates Bitcoin calculations with batching
- **updateBmuMapping.ts** - Updates BMU mapping data from Elexon API

### Current Status

Currently, the platform shows a 68.46% reconciliation rate (1,035,073 calculations out of 1,511,934 expected) across three miner models:
- S19J_PRO: 345,031 calculations
- S9: 345,021 calculations
- M20S: 345,021 calculations

December 2023 reconciliation is now 100% complete. The current focus is on November 2023.

### Recent Recoveries

Successfully recovered missing data for March 1-2, 2025:

- March 1, 2025: Recovered 819 curtailment records (21,178.62 MWh)
- March 2, 2025: Recovered 2,444 curtailment records (61,575.86 MWh)
- Bitcoin calculations fully reconciled for all dates

## Development Guide

### Core Files

- `unified_reconciliation.ts` - Main system for all reconciliation operations
- `daily_reconciliation_check.ts` - Automated daily reconciliation
- `server/services/historicalReconciliation.ts` - Core service for reconciliation logic
- `server/services/bitcoinService.ts` - Bitcoin calculation logic
- `server/services/optimizedMiningService.ts` - Optimized mining potential calculations
- `server/routes/optimizedMiningRoutes.ts` - API endpoints for mining potential data

### Running the Project

The app runs with the "Start application" workflow, which executes `npm run dev`. This starts:
- Express backend server (port 3000)
- Vite frontend development server (proxied through Express)

### Directory Structure

```
/
├── server/                # Backend code
│   ├── controllers/       # API controllers
│   ├── middleware/        # Express middleware
│   ├── routes/            # API routes
│   ├── scripts/           # Maintenance scripts
│   │   ├── data/          # Data processing scripts
│   │   └── maintenance/   # System maintenance scripts
│   ├── services/          # Business logic services
│   ├── types/             # TypeScript type definitions
│   └── utils/             # Utility functions
├── client/                # Frontend code
│   ├── src/
│   │   ├── components/    # React components
│   │   ├── hooks/         # Custom React hooks
│   │   ├── lib/           # Utility libraries
│   │   └── pages/         # Page components
├── db/                    # Database models and schema
├── logs/                  # Application logs
└── migrations/            # Database migrations
```

## API Documentation

The platform exposes several API endpoints to access curtailment and mining data:

- `/api/curtailment/mining-potential` - Get Bitcoin mining potential
- `/api/curtailment/monthly-mining-potential` - Get monthly mining data
- `/api/summary/daily` - Get daily curtailment summaries
- `/api/summary/monthly` - Get monthly curtailment summaries
- `/api/summary/yearly` - Get yearly curtailment summaries

## Optimization Strategies

The platform has undergone several optimization rounds to improve performance, reliability, and maintainability.

### Database Optimizations

Recent optimizations include:

1. **Direct Query Optimization**: Replaced materialized views with optimized direct queries
2. **Connection Pooling**: Advanced connection management with proper cleanup
3. **Query Parameterization**: Standardized query parameterization for improved security and performance
4. **Transaction Support**: Consistent transaction management for data integrity

### Error Handling

The system uses a standardized error handling system with:

1. **Error Classification**: Errors are categorized by type and severity
2. **Contextual Details**: Errors include rich context for debugging
3. **Consistent Logging**: All errors are logged with standard formatting
4. **API Error Responses**: Standardized API error responses with appropriate HTTP status codes

### Performance Monitoring

The platform includes comprehensive performance monitoring:

1. **Query Timing**: All database queries are timed and logged
2. **Request Logging**: API requests are logged with timing information
3. **Resource Monitoring**: Database connection pool usage is monitored
4. **Automatic Recovery**: The system attempts to recover from transient errors

## Maintenance

### Automated Checks

The system includes automated monitoring to catch missing data before it causes issues:

1. **Automated Daily Check**: The `daily_reconciliation_check.ts` script runs automatically to find and fix issues
2. **Reconciliation Reports**: The `unified_reconciliation.ts analyze` command generates detailed reports on data completeness

### Recovery Procedures

If missing data is detected:

1. **Identify the Gap**: Use the unified reconciliation system to identify missing data:
   ```bash
   npx tsx unified_reconciliation.ts status
   ```

2. **Reprocess the Data**: Use the appropriate command for the situation:
   ```bash
   # For a specific date
   npx tsx unified_reconciliation.ts date YYYY-MM-DD
   
   # For a date range
   npx tsx unified_reconciliation.ts range YYYY-MM-DD YYYY-MM-DD
   
   # For problematic dates
   npx tsx unified_reconciliation.ts critical YYYY-MM-DD
   ```

3. **Verify Recovery**: Run verification again to confirm all data is properly reconciled:
   ```bash
   npx tsx unified_reconciliation.ts status
   ```

### Troubleshooting

Common issues and their solutions:

1. **Missing Curtailment Data**: Often caused by API access issues or network interruptions
   - Solution: Use `unified_reconciliation.ts date YYYY-MM-DD` to re-ingest data from source APIs

2. **Missing Bitcoin Calculations**: May occur when curtailment data exists but calculation failed
   - Solution: Fix with `unified_reconciliation.ts reconcile` which only regenerates the calculations

3. **Data Consistency Issues**: When totals don't match between tables
   - Solution: Use `unified_reconciliation.ts critical YYYY-MM-DD` to apply consistency fixes

4. **BMU Mapping Issues**: When new wind farms are added but not reflected in the system
   - Solution: Update BMU mapping with `npx tsx server/scripts/maintenance/updateBmuMapping.ts`