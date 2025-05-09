# Component Responsibility Separation

This document outlines the separation of responsibilities between different components in the application.

## API Controllers

Controllers are responsible for handling HTTP requests, validating input, and returning appropriate responses. They delegate business logic to services.

- `curtailmentController.ts` - Handles requests for curtailment data
- `summaryController.ts` - Handles requests for summary data (daily, monthly, yearly)

## Services

Services encapsulate business logic and data processing. They are responsible for implementing the core functionality of the application.

- `minerstatService.ts` - Handles fetching and caching Bitcoin price and difficulty data from external APIs
- `summaryService.ts` - Handles fetching and processing summary data
- `bitcoinService.ts` - Handles Bitcoin calculations and updating Bitcoin-related summaries
- `curtailmentService.ts` - Handles curtailment data processing and aggregation
- `windDataUpdateService.ts` - Handles wind generation data updates
- `dataUpdateService.ts` - Orchestrates data update processes

## Models

Models represent the data structures and schema of the application. They may also include data access methods.

- Schema definitions in `db/schema.ts`
- Data models with validation in dedicated files (e.g., `models/windFarm.ts`)

## Utilities

Utilities provide helper functions and shared functionality.

- `utils/cache.ts` - Provides caching mechanisms
- `utils/bitcoin.ts` - Bitcoin calculation utilities
- `utils/logger.ts` - Logging utilities

## Scripts

Scripts are used for one-off tasks, maintenance, and data migrations.

- `scripts/maintenance/logs/logRotation.ts` - Handles log file rotation
- Migration scripts
- Data reconciliation scripts

## Key Benefits of This Separation

1. **Improved Testability** - Each component has a clearly defined responsibility, making it easier to test in isolation
2. **Better Maintainability** - Changes to one part of the system are less likely to affect other parts
3. **Clearer Dependencies** - Dependencies between components are explicit and easier to understand
4. **Enhanced Collaboration** - Different team members can work on different components without as much risk of conflicts
5. **Easier Debugging** - Issues can be isolated to specific components

## Example Flow: Fetching Monthly Bitcoin Mining Potential

1. API request comes in to endpoint `/api/bitcoin/monthly/:month`
2. `curtailmentController.getMonthlyMiningPotential` handles the request
3. Controller validates input and calls `minerstatService.fetchBitcoinStats()`
4. Controller calls `bitcoinService.calculateMonthlyBitcoinSummary()`
5. The Bitcoin service calculates or retrieves the data
6. Controller formats and returns the response

This separation ensures that the controller only handles the HTTP request/response cycle, while the services handle the actual business logic.