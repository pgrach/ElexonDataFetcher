# Bitcoin Mining Potential Platform

## Overview

This is a full-stack web application built with Express.js and React that analyzes Bitcoin mining potential from curtailed wind energy data. The platform fetches curtailment data from the Elexon BMRS API, processes it through various mining calculations, and provides comprehensive analytics and visualizations.

## System Architecture

The application follows a modern full-stack architecture:

### Frontend
- **React** with TypeScript for the user interface
- **Tailwind CSS** with shadcn/ui components for styling
- **Vite** for build tooling and development server
- **TanStack Query** for state management and API calls
- **Wouter** for client-side routing

### Backend
- **Express.js** with TypeScript for the REST API
- **Node.js** runtime with ES modules
- **Drizzle ORM** for database operations
- **PostgreSQL** as the primary database
- **DynamoDB** for historical difficulty data storage

### Database Architecture
- **Primary Database**: PostgreSQL with Drizzle ORM
- **Schema**: Defined in `db/schema.ts` with tables for:
  - `curtailment_records` - Wind farm curtailment data
  - `historical_bitcoin_calculations` - Bitcoin mining calculations
  - `bitcoin_daily_summaries` - Daily aggregated data
  - `bitcoin_monthly_summaries` - Monthly aggregated data
  - `wind_generation_data` - Wind generation metrics

## Key Components

### Data Ingestion Services
- **Curtailment Service** (`server/services/curtailmentService.ts`) - Fetches and processes curtailment data from Elexon API
- **Wind Generation Service** (`server/services/windGenerationService.ts`) - Manages wind generation data collection
- **Data Update Service** (`server/services/dataUpdateService.ts`) - Coordinates automated data updates

### Bitcoin Calculation Engine
- **Bitcoin Service** (`server/services/bitcoinService.ts`) - Handles Bitcoin mining calculations
- **Mining Service** (`server/services/optimizedMiningService.ts`) - Optimized mining potential calculations
- **Calculation Utilities** (`server/utils/bitcoin.ts`) - Core Bitcoin calculation logic

### API Layer
- **REST API** with Express.js providing endpoints for:
  - Daily/Monthly/Yearly summaries
  - Curtailment data analysis
  - Bitcoin mining potential calculations
  - Wind generation metrics
  - Farm-specific data tables

### Background Services
- **Scheduled Data Updates** - Automated data fetching and processing
- **Historical Reconciliation** - Data consistency checks and corrections
- **Wind Data Updates** - Regular wind generation data synchronization

## Data Flow

1. **Data Collection**: Automated services fetch curtailment and wind generation data from Elexon API
2. **Processing**: Raw data is processed and stored in PostgreSQL with proper normalization
3. **Calculations**: Bitcoin mining potential is calculated using various miner models (S19J_PRO, S9, M20S)
4. **Aggregation**: Daily, monthly, and yearly summaries are generated and cached
5. **API Access**: Frontend consumes processed data through REST API endpoints
6. **Visualization**: React components display analytics and charts to users

## External Dependencies

### APIs
- **Elexon BMRS API** - Primary data source for curtailment and wind generation data
- **Minerstat API** - Bitcoin price and difficulty data
- **AWS DynamoDB** - Historical Bitcoin difficulty storage

### Infrastructure
- **PostgreSQL** - Primary database (configured via DATABASE_URL)
- **AWS Services** - DynamoDB for historical data storage
- **Node.js** - Runtime environment

### Third-Party Libraries
- **Drizzle ORM** - Database abstraction layer
- **Radix UI** - Headless UI components
- **Axios** - HTTP client for API requests
- **date-fns** - Date manipulation utilities

## Deployment Strategy

### Development
- **Development Server**: `npm run dev` starts both frontend and backend in development mode
- **Hot Reloading**: Vite provides hot module replacement for frontend development
- **TypeScript**: Full TypeScript support with type checking

### Production
- **Build Process**: `npm run build` creates optimized production builds
- **Static Assets**: Frontend builds to `dist/public` directory
- **Server Bundle**: Backend builds to `dist/index.js` with ESM format
- **Environment Variables**: Configured for production deployment

### Database Management
- **Migrations**: Drizzle migrations stored in `./migrations`
- **Schema Push**: `npm run db:push` for schema synchronization
- **Connection**: Uses DATABASE_URL environment variable

## User Preferences

Preferred communication style: Simple, everyday language.

## Data Ingestion Best Practices

### Critical Success Methodology
Based on July 6, 2025 experience where database contained only 9 records but API verification revealed 199 records:

1. **API-First Verification**: Always verify against Elexon BMRS API before trusting database
2. **Use Proven Systems**: Leverage `processDailyCurtailment()` service, never create custom scripts  
3. **Payment Sign Logic**: Payments must be POSITIVE (subsidies paid TO wind farms)
4. **End-to-End Testing**: Verify both database integrity AND API endpoint functionality
5. **Pattern Recognition**: High curtailment typically in periods 39-48, expect 100-5000 MWh daily

### Key Files for Data Operations
- `server/services/curtailmentService.ts` - Proven ingestion service
- `server/services/elexon.ts` - API interface  
- `server/controllers/summary.ts` - Payment logic reference
- `data-verification-methodology.md` - Complete verification process

## Changelog

Changelog:
- July 07, 2025. July 6 data successfully verified against Elexon API and re-ingested (9→199 records, 160→1,739 MWh). Updated data verification methodology.
- July 04, 2025. Initial setup