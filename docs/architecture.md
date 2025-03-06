# Bitcoin Mining Analytics Platform - Architecture

This document describes the architecture of the Bitcoin Mining Analytics platform.

## System Overview

The Bitcoin Mining Analytics platform is designed to process curtailment data from wind farms and calculate the potential Bitcoin that could have been mined if that curtailed energy had been used for Bitcoin mining.

```
┌─────────────────┐        ┌─────────────────┐         ┌─────────────────┐
│                 │        │                 │         │                 │
│   Elexon API    │───────►│ Express Backend │◄────────│  React Frontend │
│                 │        │                 │         │                 │
└─────────────────┘        └────────┬────────┘         └─────────────────┘
                                    │
                                    │
                           ┌────────▼────────┐
                           │                 │
                           │   PostgreSQL    │
                           │                 │
                           └────────┬────────┘
                                    │
                                    │
                           ┌────────▼────────┐
                           │                 │
                           │    DynamoDB     │
                           │                 │
                           └─────────────────┘
```

## Key Components

### Frontend (React + Vite)

The frontend is built with React and Vite, providing a responsive user interface for data visualization and analysis.

- **Pages**: Home, Detail Views
- **Components**: Charts, Tables, Filters
- **State Management**: React Query for API data
- **Routing**: wouter for client-side routing

### Backend (Node.js + Express)

The backend provides RESTful API endpoints for data access and processing.

- **Controllers**: Handle request/response logic
- **Services**: Implement business logic
- **Routes**: Define API endpoints
- **Middleware**: Handle authentication, logging, error handling
- **Utils**: Shared utility functions

### Database (PostgreSQL + DynamoDB)

The application uses a dual-database approach:

- **PostgreSQL**: Primary database for storing curtailment records and Bitcoin calculations
- **DynamoDB**: Used for storing historical Bitcoin difficulty data

### Data Processing Pipeline

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│                 │     │                 │     │                 │
│  Data Ingestion │────►│ Data Processing │────►│  Data Storage   │
│                 │     │                 │     │                 │
└─────────────────┘     └─────────────────┘     └─────────────────┘
        │                       │                       │
        ▼                       ▼                       ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│                 │     │                 │     │                 │
│ Elexon API Data │     │ Bitcoin Calcs   │     │ Primary Tables  │
│                 │     │                 │     │                 │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                                                        │
                                                        ▼
                                               ┌─────────────────┐
                                               │                 │
                                               │ Summary Tables  │
                                               │                 │
                                               └─────────────────┘
```

## Key Processes

### Data Ingestion

1. Fetch curtailment data from the Elexon API
2. Process and validate the data
3. Store records in the PostgreSQL database

### Bitcoin Calculation

1. Retrieve curtailment records for specific dates and periods
2. Fetch Bitcoin network difficulty from DynamoDB
3. Calculate potential Bitcoin mining for different miner models
4. Store calculations in historical_bitcoin_calculations table

### Data Reconciliation

1. Check for missing calculations across all dates
2. Process any missing calculations
3. Update summary tables for daily, monthly, and yearly aggregations

## Communication Patterns

### API Requests

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│             │     │             │     │             │     │             │
│  React UI   │────►│ API Gateway │────►│  Controller │────►│   Service   │
│             │     │             │     │             │     │             │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
                                                                   │
                                                                   ▼
                                                            ┌─────────────┐
                                                            │             │
                                                            │  Database   │
                                                            │             │
                                                            └─────────────┘
```

### Error Handling

The system uses a comprehensive error handling approach:

1. Custom error classes (DatabaseError, ApiError, etc.)
2. Global error handler middleware
3. Consistent error response format
4. Detailed error logging

## Deployment Architecture

The application is deployed on Replit with the following structure:

```
┌─────────────────────────────────────┐
│          Replit Environment         │
│                                     │
│  ┌─────────────┐   ┌─────────────┐  │
│  │             │   │             │  │
│  │ Express API │◄──►│ Vite Dev   │  │
│  │             │   │ Server      │  │
│  └─────────────┘   └─────────────┘  │
│          │                          │
│          ▼                          │
│  ┌─────────────┐                    │
│  │             │                    │
│  │ PostgreSQL  │                    │
│  │             │                    │
│  └─────────────┘                    │
│                                     │
└─────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────┐
│           AWS Services              │
│                                     │
│  ┌─────────────┐                    │
│  │             │                    │
│  │  DynamoDB   │                    │
│  │             │                    │
│  └─────────────┘                    │
│                                     │
└─────────────────────────────────────┘
```

## Security Architecture

The application implements several security measures:

1. Environment variables for sensitive configuration
2. Input validation using Zod
3. Parameter sanitization for database queries
4. HTTPS for all external API calls

## Performance Optimization

Several performance optimizations are implemented:

1. In-memory caching for frequent calculations
2. Query optimization with proper indexing
3. Batch processing for large datasets
4. Checkpoint system for long-running processes

## Future Architecture Considerations

Potential future enhancements include:

1. Microservice architecture for better scalability
2. Event-driven processing for real-time updates
3. GraphQL API for more efficient data fetching
4. Docker containerization for consistent deployment