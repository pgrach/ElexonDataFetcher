# Bitcoin Mining Analytics Platform - Backend

This directory contains the Node.js/Express backend for the Bitcoin Mining Analytics platform.

## Directory Structure

- `controllers/` - Request handlers for API endpoints
- `data/` - Data management and storage utilities
- `middleware/` - Express middleware for request processing
  - `middleware/errorHandler.ts` - Global error handling middleware
  - `middleware/performanceMonitor.ts` - Performance monitoring middleware
  - `middleware/requestLogger.ts` - Request logging middleware
- `routes/` - API route definitions
- `scripts/` - Server-side utility scripts
- `services/` - Business logic and data processing services
  - `services/bitcoinService.ts` - Bitcoin calculation services
  - `services/curtailment.ts` - Curtailment data processing
  - `services/dataUpdater.ts` - Data update orchestration
  - `services/dynamodbService.ts` - AWS DynamoDB integration
  - `services/elexon.ts` - Elexon API integration
  - `services/historicalReconciliation.ts` - Data reconciliation services
  - `services/optimizedMiningService.ts` - Optimized mining potential calculations
- `types/` - TypeScript type definitions
- `utils/` - Utility functions and helpers
  - `utils/bitcoin.ts` - Bitcoin calculation utilities
  - `utils/cache.ts` - In-memory caching system
  - `utils/checkpoints.ts` - Checkpoint management for long-running processes
  - `utils/database.ts` - Database utility functions
  - `utils/dates.ts` - Date parsing and formatting utilities
  - `utils/errors.ts` - Standardized error handling
  - `utils/logger.ts` - Logging system
- `index.ts` - Express server setup and initialization
- `routes.ts` - API route registration
- `vite.ts` - Vite integration for serving the frontend

## Technology Stack

- Node.js with Express for the HTTP server
- TypeScript for type safety
- PostgreSQL with Drizzle ORM for relational data storage
- AWS DynamoDB for historical Bitcoin difficulty data
- Express middleware for request processing
- Zod for data validation

## API Endpoints

The backend provides several API endpoints for different aspects of the Bitcoin Mining Analytics platform:

- Curtailment data endpoints
- Bitcoin mining potential calculation endpoints
- Historical data endpoints
- System status and monitoring endpoints

## Data Processing Flow

1. External data is fetched from the Elexon API
2. Curtailment records are processed and stored in the PostgreSQL database
3. Bitcoin mining potential is calculated using the curtailment data and difficulty from DynamoDB
4. Daily, monthly, and yearly summaries are automatically updated
5. The reconciliation system ensures data integrity across all tables

## Environment Variables

The backend requires several environment variables:

- `DATABASE_URL` - PostgreSQL connection string
- `AWS_REGION` - AWS region for DynamoDB
- `AWS_ACCESS_KEY_ID` - AWS access key for DynamoDB
- `AWS_SECRET_ACCESS_KEY` - AWS secret key for DynamoDB
- `PORT` - Port for the Express server (optional, defaults to 3000)