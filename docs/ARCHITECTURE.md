# Bitcoin Mining Analytics Platform - Architecture

This document provides an overview of the Bitcoin Mining Analytics platform architecture.

## High-Level Architecture

The Bitcoin Mining Analytics platform follows a modern full-stack architecture:

```
┌────────────────┐        ┌─────────────────┐        ┌──────────────────┐
│                │        │                 │        │                  │
│  React         │◄─────►│  Express Server  │◄─────►│  PostgreSQL      │
│  Frontend      │        │  API            │        │  Database        │
│                │        │                 │        │                  │
└────────────────┘        └─────────────────┘        └──────────────────┘
                                  ▲
                                  │
                                  ▼
                          ┌─────────────────┐
                          │                 │
                          │  AWS DynamoDB   │
                          │  (Difficulty)   │
                          │                 │
                          └─────────────────┘
```

## Component Overview

### Frontend (client/)

- React application built with Vite
- Tailwind CSS for styling with shadcn/ui components
- React Query for data fetching and state management
- Wouter for client-side routing
- Recharts for data visualization

### Backend (server/)

- Express.js server
- TypeScript for type safety
- RESTful API endpoints for data access
- Service modules for business logic
- Middleware for request processing, logging, and error handling

### Database (db/)

- PostgreSQL with Drizzle ORM
- Core tables:
  - curtailment_records
  - historical_bitcoin_calculations
- Summary tables:
  - daily_summaries
  - monthly_summaries
  - yearly_summaries
  - bitcoin_monthly_summaries
  - bitcoin_yearly_summaries

### External Integrations

- Elexon API for curtailment data
- AWS DynamoDB for Bitcoin difficulty data
- Minerstat API for Bitcoin price data

## Data Flow

### Curtailment Data Processing

1. Fetch Elexon API data for a specific date
2. Process and store curtailment records in PostgreSQL
3. Update daily, monthly, and yearly summaries

### Bitcoin Calculation Flow

1. Retrieve curtailment records for a period
2. Fetch Bitcoin difficulty from DynamoDB
3. Calculate Bitcoin mining potential
4. Store calculations in historical_bitcoin_calculations
5. Update bitcoin_monthly_summaries and bitcoin_yearly_summaries

### API Data Flow

1. Client sends request to Express server
2. Server routes the request to the appropriate controller
3. Controller uses services to fetch and process data
4. Data is returned to the client in JSON format

## Key Processes

### Data Reconciliation

The platform includes several reconciliation processes:

- Daily reconciliation check
- Unified reconciliation system
- Complete reingestion process

These processes ensure data consistency and integrity across all tables.

### Data Updates

Data updates follow a cascading pattern:

1. Update curtailment records
2. Trigger Bitcoin calculation updates
3. Recalculate monthly and yearly summaries

## Performance Considerations

- In-memory caching for frequently accessed data
- Batch processing to avoid timeouts
- Connection pooling for database efficiency
- Query optimization for large datasets

## Security

- Environment variables for sensitive configuration
- Input validation using Zod schemas
- Error handling to prevent information leakage

## Deployment

The application is designed to be deployed on Replit, with automatic workflows for starting the application.